#include "wz_extension.hpp"
#include "mssql_utils.hpp"
#include "wz_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

// ============================================================================
// Data structures
// ============================================================================

struct KontobezeichnungResult {
    string table_name;
    int64_t rows_inserted;
    string duration;
    bool success;
    string error_message;
};

struct KontobezeichnungBindData : public TableFunctionData {
    string secret_name;
    string source_table;
    string gui_verfahren_id;
    string str_angelegt;

    KontobezeichnungResult result;
    bool executed;

    KontobezeichnungBindData() : executed(false) {}
};

struct KontobezeichnungGlobalState : public GlobalTableFunctionState {
    bool output_done;
    KontobezeichnungGlobalState() : output_done(false) {}
};

// ============================================================================
// Bind
// ============================================================================

static unique_ptr<FunctionData> KontobezeichnungBind(ClientContext &context,
                                                      TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types,
                                                      vector<string> &names) {
    auto bind_data = make_uniq<KontobezeichnungBindData>();

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        string key = kv.first;
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);

        if (key == "secret") {
            bind_data->secret_name = kv.second.ToString();
        } else if (key == "source_table") {
            bind_data->source_table = kv.second.ToString();
        } else if (key == "gui_verfahren_id") {
            bind_data->gui_verfahren_id = kv.second.ToString();
        } else if (key == "str_angelegt") {
            bind_data->str_angelegt = kv.second.ToString();
        }
    }

    // Defaults
    if (bind_data->secret_name.empty()) {
        bind_data->secret_name = "mssql_conn";
    }
    if (bind_data->str_angelegt.empty()) {
        bind_data->str_angelegt = "wz";
    }

    // Validate required parameters
    if (bind_data->source_table.empty()) {
        throw BinderException("import_kontobezeichnung: source_table is required");
    }
    if (bind_data->gui_verfahren_id.empty()) {
        throw BinderException("import_kontobezeichnung: gui_verfahren_id is required");
    }
    if (!IsValidSqlIdentifier(bind_data->source_table)) {
        throw BinderException("import_kontobezeichnung: invalid source_table name");
    }

    // Define return columns
    names = {"table_name", "rows_inserted", "duration", "success", "error_message"};
    return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR,
                    LogicalType::BOOLEAN, LogicalType::VARCHAR};

    return std::move(bind_data);
}

// ============================================================================
// Init
// ============================================================================

static unique_ptr<GlobalTableFunctionState> KontobezeichnungInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
    return make_uniq<KontobezeichnungGlobalState>();
}

// ============================================================================
// Execute
// ============================================================================

static void KontobezeichnungExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<KontobezeichnungBindData>();
    auto &state = data_p.global_state->Cast<KontobezeichnungGlobalState>();

    if (state.output_done) {
        output.SetCardinality(0);
        return;
    }

    if (!bind_data.executed) {
        bind_data.executed = true;

        auto start_time = std::chrono::steady_clock::now();
        auto &db = DatabaseInstance::GetDatabase(context);

        bind_data.result.table_name = "tblVerfahrenKontenbezeichnung";
        bind_data.result.rows_inserted = 0;
        bind_data.result.success = false;

        try {
            // ----------------------------------------------------------------
            // 1. Load source data
            // ----------------------------------------------------------------
            Connection conn(db);
            string load_sql = "SELECT * FROM " + bind_data.source_table;
            auto src_result = conn.Query(load_sql);
            if (src_result->HasError()) {
                throw std::runtime_error("Failed to load source table: " + src_result->GetError());
            }

            // Collect column names
            vector<string> col_names;
            for (idx_t i = 0; i < src_result->types.size(); i++) {
                col_names.push_back(src_result->names[i]);
            }

            // Find konto and kontobezeichnung columns
            idx_t konto_idx = FindColumnIndex(col_names, "konto");
            if (konto_idx == DConstants::INVALID_INDEX) {
                konto_idx = FindColumnIndex(col_names, "decKontoNr");
            }
            if (konto_idx == DConstants::INVALID_INDEX) {
                konto_idx = FindColumnIndex(col_names, "kontonr");
            }
            if (konto_idx == DConstants::INVALID_INDEX) {
                konto_idx = FindColumnIndex(col_names, "konto_nr");
            }
            if (konto_idx == DConstants::INVALID_INDEX) {
                throw std::runtime_error("Source table must contain a 'konto' column (also accepts: decKontoNr, kontonr, konto_nr)");
            }

            idx_t bez_idx = FindColumnIndex(col_names, "kontobezeichnung");
            if (bez_idx == DConstants::INVALID_INDEX) {
                bez_idx = FindColumnIndex(col_names, "strBezeichnung");
            }
            if (bez_idx == DConstants::INVALID_INDEX) {
                bez_idx = FindColumnIndex(col_names, "bezeichnung");
            }
            if (bez_idx == DConstants::INVALID_INDEX) {
                throw std::runtime_error("Source table must contain a 'kontobezeichnung' column (also accepts: strBezeichnung, bezeichnung)");
            }

            // Materialize rows
            struct KbRow {
                string konto;
                string bezeichnung;
            };
            vector<KbRow> rows;

            for (auto &chunk : src_result->Collection().Chunks()) {
                for (idx_t row = 0; row < chunk.size(); row++) {
                    KbRow r;
                    auto konto_val = chunk.data[konto_idx].GetValue(row);
                    auto bez_val = chunk.data[bez_idx].GetValue(row);
                    if (konto_val.IsNull() || bez_val.IsNull()) {
                        continue; // skip rows with NULL konto or bezeichnung
                    }
                    r.konto = konto_val.ToString();
                    r.bezeichnung = bez_val.ToString();
                    rows.push_back(std::move(r));
                }
            }

            if (rows.empty()) {
                bind_data.result.success = true;
                bind_data.result.rows_inserted = 0;
                auto end_time = std::chrono::steady_clock::now();
                double elapsed = std::chrono::duration<double>(end_time - start_time).count();
                bind_data.result.duration = FormatDuration(elapsed);
                // Fall through to output
            } else {
                // ----------------------------------------------------------------
                // 2. Validate gui_verfahren_id is a plausible UUID
                // ----------------------------------------------------------------
                string &vf_id = bind_data.gui_verfahren_id;
                if (vf_id.size() < 32) {
                    throw std::runtime_error("gui_verfahren_id does not look like a valid UUID: " + vf_id);
                }

                // ----------------------------------------------------------------
                // 3. Attach MSSQL and build INSERT statements
                // ----------------------------------------------------------------
                string db_name = "kb_mssql_" + std::to_string(reinterpret_cast<uintptr_t>(&bind_data));
                string attach_sql = "ATTACH '' AS \"" + db_name + "\" (TYPE mssql, SECRET " + bind_data.secret_name + ")";

                Connection mssql_conn(db);
                string error_msg;

                if (!ExecuteMssqlStatement(mssql_conn, attach_sql, error_msg)) {
                    throw std::runtime_error("Failed to attach MSSQL: " + error_msg);
                }

                // Begin transaction
                if (!ExecuteMssqlStatement(mssql_conn, "BEGIN TRANSACTION", error_msg)) {
                    throw std::runtime_error("Failed to begin transaction: " + error_msg);
                }

                string current_ts = GetCurrentTimestamp();
                int64_t total_inserted = 0;

                // Batch insert rows
                for (size_t batch_start = 0; batch_start < rows.size(); batch_start += MSSQL_BULK_INSERT_BATCH_SIZE) {
                    size_t batch_end = std::min(batch_start + MSSQL_BULK_INSERT_BATCH_SIZE, rows.size());

                    string insert_sql = "INSERT INTO \"" + db_name + "\".dbo.tblVerfahrenKontenbezeichnung "
                        "(lngTimestamp, strAngelegt, dtmAngelegt, strGeaendert, dtmGeaendert, "
                        "guiVerfahrenID, lngEAKonto, dtmGueltigAb, decKontoNr, strBezeichnung) VALUES ";

                    for (size_t i = batch_start; i < batch_end; i++) {
                        if (i > batch_start) {
                            insert_sql += ", ";
                        }
                        insert_sql += "(0, '" + EscapeSqlString(bind_data.str_angelegt) + "', "
                            "'" + current_ts + "', "
                            "NULL, NULL, "
                            "'" + EscapeSqlString(vf_id) + "', "
                            "0, "
                            "'1980-01-01 00:00:00', "
                            + rows[i].konto + ", "
                            "'" + EscapeSqlString(rows[i].bezeichnung) + "')";
                    }

                    if (!ExecuteMssqlStatement(mssql_conn, insert_sql, error_msg)) {
                        // Rollback on error
                        string rollback_err;
                        ExecuteMssqlStatement(mssql_conn, "ROLLBACK", rollback_err);
                        throw std::runtime_error("INSERT failed at batch starting row " +
                            std::to_string(batch_start) + ": " + error_msg);
                    }

                    total_inserted += static_cast<int64_t>(batch_end - batch_start);
                }

                // Commit
                if (!ExecuteMssqlStatement(mssql_conn, "COMMIT", error_msg)) {
                    string rollback_err;
                    ExecuteMssqlStatement(mssql_conn, "ROLLBACK", rollback_err);
                    throw std::runtime_error("COMMIT failed: " + error_msg);
                }

                bind_data.result.rows_inserted = total_inserted;
                bind_data.result.success = true;

                auto end_time = std::chrono::steady_clock::now();
                double elapsed = std::chrono::duration<double>(end_time - start_time).count();
                bind_data.result.duration = FormatDuration(elapsed);
            }

        } catch (std::exception &e) {
            bind_data.result.success = false;
            bind_data.result.error_message = e.what();
            auto end_time = std::chrono::steady_clock::now();
            double elapsed = std::chrono::duration<double>(end_time - start_time).count();
            bind_data.result.duration = FormatDuration(elapsed);
        }
    }

    // Output single result row
    auto &r = bind_data.result;
    output.data[0].SetValue(0, Value(r.table_name));
    output.data[1].SetValue(0, Value::BIGINT(r.rows_inserted));
    output.data[2].SetValue(0, Value(r.duration));
    output.data[3].SetValue(0, Value::BOOLEAN(r.success));
    output.data[4].SetValue(0, r.error_message.empty() ? Value(nullptr) : Value(r.error_message));
    output.SetCardinality(1);
    state.output_done = true;
}

// ============================================================================
// Register
// ============================================================================

void RegisterImportKontobezeichnungFunction(DatabaseInstance &db) {
    TableFunction func("import_kontobezeichnung", {}, KontobezeichnungExecute,
                       KontobezeichnungBind, KontobezeichnungInitGlobal);

    func.named_parameters["secret"] = LogicalType::VARCHAR;
    func.named_parameters["source_table"] = LogicalType::VARCHAR;
    func.named_parameters["gui_verfahren_id"] = LogicalType::VARCHAR;
    func.named_parameters["str_angelegt"] = LogicalType::VARCHAR;

    Connection con(db);
    con.BeginTransaction();
    auto &catalog = Catalog::GetSystemCatalog(db);
    CreateTableFunctionInfo info(func);
    catalog.CreateFunction(*con.context, info);
    con.Commit();
}

} // namespace duckdb
