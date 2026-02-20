#include "wz_extension.hpp"
#include "wz_utils.hpp"
#include "mssql_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>

namespace duckdb {

// ============================================================================
// DuckDB type -> MSSQL type mapping
// ============================================================================

static string MapDuckDBTypeToMssql(const string &duckdb_type) {
    string upper = duckdb_type;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

    if (upper == "INTEGER" || upper == "INT") return "INT";
    if (upper == "BIGINT" || upper == "INT64" || upper == "LONG") return "BIGINT";
    if (upper == "SMALLINT" || upper == "INT16" || upper == "SHORT") return "SMALLINT";
    if (upper == "TINYINT" || upper == "INT8") return "TINYINT";
    if (upper == "UINTEGER") return "BIGINT";
    if (upper == "UBIGINT") return "DECIMAL(20,0)";
    if (upper == "USMALLINT") return "INT";
    if (upper == "UTINYINT") return "SMALLINT";
    if (upper == "HUGEINT" || upper == "INT128") return "DECIMAL(38,0)";
    if (upper == "DOUBLE" || upper == "FLOAT8") return "FLOAT";
    if (upper == "FLOAT" || upper == "FLOAT4" || upper == "REAL") return "REAL";
    if (upper == "BOOLEAN" || upper == "BOOL") return "BIT";
    if (upper == "DATE") return "DATE";
    if (upper == "TIME") return "TIME";
    if (upper == "TIMESTAMP" || upper == "DATETIME" || upper == "TIMESTAMP_S" ||
        upper == "TIMESTAMP_MS" || upper == "TIMESTAMP_NS") return "DATETIME2";
    if (upper == "TIMESTAMP WITH TIME ZONE" || upper == "TIMESTAMPTZ") return "DATETIMEOFFSET";
    if (upper == "BLOB" || upper == "BYTEA") return "VARBINARY(MAX)";
    if (upper == "UUID") return "UNIQUEIDENTIFIER";

    // DECIMAL(p,s) - pass through
    if (upper.substr(0, 7) == "DECIMAL") return duckdb_type;

    // VARCHAR(n) - map to NVARCHAR(n) or NVARCHAR(MAX)
    if (upper.substr(0, 7) == "VARCHAR") {
        if (upper == "VARCHAR") return "NVARCHAR(MAX)";
        auto open = upper.find('(');
        auto close = upper.find(')');
        if (open != string::npos && close != string::npos) {
            return "NVARCHAR" + upper.substr(open, close - open + 1);
        }
        return "NVARCHAR(MAX)";
    }

    // Default fallback
    return "NVARCHAR(MAX)";
}

// ============================================================================
// Data structures
// ============================================================================

struct TableTransferResult {
    string table_name;
    int64_t rows_transferred;
    string method;
    string duration;
    bool success;
    string error_message;
};

struct ColumnInfo {
    string name;
    string duckdb_type;
    string mssql_type;
};

struct TableInfo {
    string name;
    vector<ColumnInfo> columns;
};

struct MoveToMssqlBindData : public TableFunctionData {
    string secret_name;
    string target_schema;
    bool all_tables;
    vector<string> explicit_tables;
    vector<string> exclude_tables;

    vector<TableTransferResult> results;
    bool executed;
};

enum class MovePhase {
    DISCOVER_TABLES,
    PROCESS_TABLES,
    OUTPUT_RESULTS,
    DONE
};

struct MoveToMssqlGlobalState : public GlobalTableFunctionState {
    MovePhase phase;
    idx_t current_table_idx;
    idx_t current_result_idx;
    vector<TableInfo> tables;
    std::chrono::high_resolution_clock::time_point total_start;

    MoveToMssqlGlobalState()
        : phase(MovePhase::DISCOVER_TABLES), current_table_idx(0), current_result_idx(0) {}
};

// ============================================================================
// Bind
// ============================================================================

static unique_ptr<FunctionData> MoveToMssqlBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto bind_data = make_uniq<MoveToMssqlBindData>();
    bind_data->executed = false;
    bind_data->secret_name = "mssql_conn";
    bind_data->target_schema = "dbo";
    bind_data->all_tables = true;

    for (auto &kv : input.named_parameters) {
        auto lower_name = kv.first;
        std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);

        if (lower_name == "secret") {
            bind_data->secret_name = kv.second.GetValue<string>();
        } else if (lower_name == "schema") {
            bind_data->target_schema = kv.second.GetValue<string>();
        } else if (lower_name == "all_tables") {
            bind_data->all_tables = kv.second.GetValue<bool>();
        } else if (lower_name == "tables") {
            auto &children = ListValue::GetChildren(kv.second);
            for (auto &child : children) {
                bind_data->explicit_tables.push_back(child.GetValue<string>());
            }
        } else if (lower_name == "exclude") {
            auto &children = ListValue::GetChildren(kv.second);
            for (auto &child : children) {
                bind_data->exclude_tables.push_back(child.GetValue<string>());
            }
        }
    }

    // If explicit tables provided, override all_tables
    if (!bind_data->explicit_tables.empty()) {
        bind_data->all_tables = false;
    }

    if (!IsValidSqlIdentifier(bind_data->secret_name)) {
        throw BinderException("move_to_mssql: invalid secret name: " + bind_data->secret_name);
    }
    if (!IsValidSqlIdentifier(bind_data->target_schema)) {
        throw BinderException("move_to_mssql: invalid schema name: " + bind_data->target_schema);
    }

    names = {"table_name", "rows_transferred", "method", "duration", "success", "error_message"};
    return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR,
                    LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR};

    return std::move(bind_data);
}

// ============================================================================
// Init
// ============================================================================

static unique_ptr<GlobalTableFunctionState> MoveToMssqlInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
    return make_uniq<MoveToMssqlGlobalState>();
}

// ============================================================================
// Per-table BCP transfer
// ============================================================================

static bool BcpTransferTable(Connection &local_conn, ClientContext &context,
                              const string &secret_name,
                              const string &source_table,
                              const string &schema,
                              const string &mssql_table_name,
                              const vector<ColumnInfo> &columns,
                              string &error_message,
                              int64_t &rows_transferred) {
    rows_transferred = 0;

    MssqlConnInfo conn_info;
    if (!GetMssqlConnInfo(context, secret_name, conn_info)) {
        error_message = "Could not extract MSSQL connection info from secret";
        return false;
    }

    // Build column list for export
    vector<string> col_names;
    col_names.reserve(columns.size());
    string col_list;
    for (size_t i = 0; i < columns.size(); i++) {
        col_names.push_back(columns[i].name);
        if (i > 0) col_list += ", ";
        col_list += "\"" + columns[i].name + "\"";
    }

    // Export to TSV
    auto temp_dir = std::filesystem::temp_directory_path();
    string safe_name = source_table;
    for (char &c : safe_name) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') c = '_';
    }
    string csv_path = (temp_dir / ("mssql_transfer_" + safe_name + ".tsv")).string();
    string fmt_path = (temp_dir / ("mssql_transfer_" + safe_name + ".fmt")).string();

    string export_sql = "COPY (SELECT " + col_list + " FROM \"" + source_table +
                        "\") TO '" + csv_path + "' (DELIMITER '\t', HEADER false, NULL '', QUOTE '')";
    auto export_result = local_conn.Query(export_sql);
    if (export_result->HasError()) {
        error_message = "Failed to export table to TSV: " + export_result->GetError();
        return false;
    }

    // Generate format file
    if (!GenerateBcpFormatFile(fmt_path, col_names, error_message)) {
        std::filesystem::remove(csv_path);
        return false;
    }

    // Invoke BCP
    string full_table = conn_info.database + "." + schema + "." + mssql_table_name;
    bool success = InvokeBcp(conn_info, full_table, csv_path, fmt_path, error_message, rows_transferred);

    // Cleanup
    std::filesystem::remove(csv_path);
    std::filesystem::remove(fmt_path);

    return success;
}

// ============================================================================
// Per-table INSERT VALUES fallback
// ============================================================================

static bool InsertFallbackTransfer(Connection &local_conn, Connection &mssql_conn,
                                    const string &source_table,
                                    const string &db_name,
                                    const string &schema,
                                    const string &mssql_table_name,
                                    const vector<ColumnInfo> &columns,
                                    string &error_message,
                                    int64_t &rows_transferred) {
    rows_transferred = 0;

    // Column lists
    string mssql_col_list;
    string select_cols;
    for (size_t i = 0; i < columns.size(); i++) {
        if (i > 0) { mssql_col_list += ", "; select_cols += ", "; }
        mssql_col_list += "[" + columns[i].name + "]";
        select_cols += "\"" + columns[i].name + "\"";
    }

    // Read source
    string select_sql = "SELECT " + select_cols + " FROM \"" + source_table + "\"";
    auto result = local_conn.Query(select_sql);
    if (result->HasError()) {
        error_message = "Failed to read source table: " + result->GetError();
        return false;
    }

    idx_t col_count = result->types.size();
    string insert_prefix = "INSERT INTO " + db_name + ".[" + schema + "].[" +
                           mssql_table_name + "] (" + mssql_col_list + ") VALUES ";

    vector<string> pending_stmts;
    idx_t rows_in_batch = 0;
    string values_sql;
    values_sql.reserve(256 * 1024);

    auto flush_stmt = [&]() {
        if (rows_in_batch == 0) return;
        pending_stmts.push_back(insert_prefix + values_sql);
        rows_in_batch = 0;
        values_sql.clear();
    };

    auto flush_roundtrip = [&]() -> bool {
        if (pending_stmts.empty()) return true;
        string batch;
        size_t total_len = 0;
        for (auto &s : pending_stmts) total_len += s.size() + 2;
        batch.reserve(total_len);
        for (size_t i = 0; i < pending_stmts.size(); i++) {
            if (i > 0) batch += "; ";
            batch += pending_stmts[i];
        }
        pending_stmts.clear();
        if (!ExecuteMssqlStatement(mssql_conn, batch, error_message)) {
            error_message = "INSERT batch failed: " + error_message;
            return false;
        }
        return true;
    };

    for (auto &chunk : result->Collection().Chunks()) {
        idx_t row_count = chunk.size();
        for (idx_t row = 0; row < row_count; row++) {
            if (rows_in_batch > 0) values_sql += ',';
            values_sql += '(';
            for (idx_t col = 0; col < col_count; col++) {
                if (col > 0) values_sql += ',';
                Value val = chunk.data[col].GetValue(row);
                if (val.IsNull()) {
                    values_sql += "NULL";
                } else {
                    auto &type = result->types[col];
                    if (type == LogicalType::INTEGER || type == LogicalType::BIGINT ||
                        type == LogicalType::SMALLINT || type == LogicalType::TINYINT ||
                        type == LogicalType::BOOLEAN) {
                        values_sql += val.ToString();
                    } else {
                        values_sql += '\'';
                        values_sql += EscapeSqlString(val.ToString());
                        values_sql += '\'';
                    }
                }
            }
            values_sql += ')';
            rows_in_batch++;
            rows_transferred++;

            if (rows_in_batch >= MSSQL_BULK_INSERT_BATCH_SIZE) {
                flush_stmt();
                if (pending_stmts.size() >= MSSQL_BULK_STMTS_PER_ROUNDTRIP) {
                    if (!flush_roundtrip()) return false;
                }
            }
        }
    }

    flush_stmt();
    return flush_roundtrip();
}

// ============================================================================
// Execute (state machine)
// ============================================================================

static void MoveToMssqlExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<MoveToMssqlBindData>();
    auto &state = data_p.global_state->Cast<MoveToMssqlGlobalState>();

    auto output_results = [&]() {
        idx_t count = 0;
        while (state.current_result_idx < bind_data.results.size() && count < STANDARD_VECTOR_SIZE) {
            auto &r = bind_data.results[state.current_result_idx];
            output.data[0].SetValue(count, Value(r.table_name));
            output.data[1].SetValue(count, Value::BIGINT(r.rows_transferred));
            output.data[2].SetValue(count, Value(r.method));
            output.data[3].SetValue(count, Value(r.duration));
            output.data[4].SetValue(count, Value::BOOLEAN(r.success));
            output.data[5].SetValue(count, Value(r.error_message));
            count++;
            state.current_result_idx++;
        }
        output.SetCardinality(count);
        if (state.current_result_idx >= bind_data.results.size()) {
            state.phase = MovePhase::DONE;
        }
    };

    switch (state.phase) {

    // -----------------------------------------------------------------
    // DISCOVER_TABLES
    // -----------------------------------------------------------------
    case MovePhase::DISCOVER_TABLES: {
        state.total_start = std::chrono::high_resolution_clock::now();
        auto &db = DatabaseInstance::GetDatabase(context);
        Connection conn(db);

        vector<string> tables_to_transfer;

        if (bind_data.all_tables) {
            // Build exclude set (case-insensitive)
            set<string> exclude_set;
            for (auto &ex : bind_data.exclude_tables) {
                string lower_ex = ex;
                std::transform(lower_ex.begin(), lower_ex.end(), lower_ex.begin(), ::tolower);
                exclude_set.insert(lower_ex);
            }

            auto result = conn.Query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
                "ORDER BY table_name");
            if (result->HasError()) {
                TableTransferResult err;
                err.table_name = "ERROR";
                err.rows_transferred = 0;
                err.method = "";
                err.duration = "00:00:00";
                err.success = false;
                err.error_message = "Failed to discover tables: " + result->GetError();
                bind_data.results.push_back(std::move(err));
                state.phase = MovePhase::OUTPUT_RESULTS;
                output_results();
                return;
            }

            for (auto &chunk : result->Collection().Chunks()) {
                for (idx_t row = 0; row < chunk.size(); row++) {
                    string tname = chunk.data[0].GetValue(row).ToString();
                    string tname_lower = tname;
                    std::transform(tname_lower.begin(), tname_lower.end(), tname_lower.begin(), ::tolower);
                    if (exclude_set.find(tname_lower) == exclude_set.end()) {
                        tables_to_transfer.push_back(tname);
                    }
                }
            }
        } else {
            tables_to_transfer = bind_data.explicit_tables;
        }

        if (tables_to_transfer.empty()) {
            TableTransferResult err;
            err.table_name = "ERROR";
            err.rows_transferred = 0;
            err.method = "";
            err.duration = "00:00:00";
            err.success = false;
            err.error_message = "No tables found to transfer";
            bind_data.results.push_back(std::move(err));
            state.phase = MovePhase::OUTPUT_RESULTS;
            output_results();
            return;
        }

        // Discover columns for each table
        for (auto &tname : tables_to_transfer) {
            TableInfo info;
            info.name = tname;

            auto desc_result = conn.Query("DESCRIBE \"" + tname + "\"");
            if (desc_result->HasError()) {
                TableTransferResult err;
                err.table_name = tname;
                err.rows_transferred = 0;
                err.method = "";
                err.duration = "00:00:00";
                err.success = false;
                err.error_message = "Failed to describe table: " + desc_result->GetError();
                bind_data.results.push_back(std::move(err));
                continue;
            }

            for (auto &chunk : desc_result->Collection().Chunks()) {
                for (idx_t row = 0; row < chunk.size(); row++) {
                    ColumnInfo col;
                    col.name = chunk.data[0].GetValue(row).ToString();
                    col.duckdb_type = chunk.data[1].GetValue(row).ToString();
                    col.mssql_type = MapDuckDBTypeToMssql(col.duckdb_type);
                    info.columns.push_back(std::move(col));
                }
            }

            if (!info.columns.empty()) {
                state.tables.push_back(std::move(info));
            }
        }

        state.current_table_idx = 0;
        state.phase = MovePhase::PROCESS_TABLES;
        output.SetCardinality(0);
        return;
    }

    // -----------------------------------------------------------------
    // PROCESS_TABLES - one table per Execute call
    // -----------------------------------------------------------------
    case MovePhase::PROCESS_TABLES: {
        if (state.current_table_idx >= state.tables.size()) {
            // All done - add summary
            auto total_end = std::chrono::high_resolution_clock::now();
            double total_dur = std::chrono::duration<double>(total_end - state.total_start).count();

            int64_t total_rows = 0;
            int success_count = 0;
            int total_count = static_cast<int>(bind_data.results.size());
            for (auto &r : bind_data.results) {
                if (r.success) { total_rows += r.rows_transferred; success_count++; }
            }

            TableTransferResult summary;
            summary.table_name = "SUMMARY";
            summary.rows_transferred = total_rows;
            summary.method = "";
            summary.duration = FormatDuration(total_dur);
            summary.success = (success_count > 0);
            summary.error_message = std::to_string(success_count) + "/" +
                                     std::to_string(total_count) + " tables transferred";
            bind_data.results.push_back(std::move(summary));

            state.phase = MovePhase::OUTPUT_RESULTS;
            output_results();
            return;
        }

        auto &table = state.tables[state.current_table_idx];
        auto table_start = std::chrono::high_resolution_clock::now();
        auto &db = DatabaseInstance::GetDatabase(context);

        TableTransferResult result;
        result.table_name = table.name;
        result.rows_transferred = 0;
        result.success = false;

        string error_msg;

        // 1. DROP + CREATE on MSSQL
        {
            Connection mssql_conn(db);
            string drop_sql = "DROP TABLE IF EXISTS " + bind_data.secret_name +
                              ".[" + bind_data.target_schema + "].[" + table.name + "]";
            ExecuteMssqlStatement(mssql_conn, drop_sql, error_msg);  // ignore error (table may not exist)
        }

        {
            Connection mssql_conn(db);
            string create_cols;
            for (size_t i = 0; i < table.columns.size(); i++) {
                if (i > 0) create_cols += ", ";
                create_cols += "[" + table.columns[i].name + "] " + table.columns[i].mssql_type;
            }
            string create_sql = "CREATE TABLE " + bind_data.secret_name +
                                ".[" + bind_data.target_schema + "].[" + table.name + "] (" +
                                create_cols + ")";
            if (!ExecuteMssqlStatement(mssql_conn, create_sql, error_msg)) {
                result.method = "";
                result.error_message = "Failed to create MSSQL table: " + error_msg;
                auto table_end = std::chrono::high_resolution_clock::now();
                result.duration = FormatDuration(std::chrono::duration<double>(table_end - table_start).count());
                bind_data.results.push_back(std::move(result));
                state.current_table_idx++;
                output.SetCardinality(0);
                return;
            }
        }

        // 2. Try BCP
        {
            Connection local_conn(db);
            int64_t rows = 0;
            string bcp_error;
            bool bcp_ok = BcpTransferTable(local_conn, context, bind_data.secret_name,
                                            table.name, bind_data.target_schema, table.name,
                                            table.columns, bcp_error, rows);

            if (bcp_ok) {
                result.rows_transferred = rows;
                result.method = "BCP";
                result.success = true;
            } else {
                // 3. Fallback to INSERT VALUES
                Connection local_conn2(db);
                Connection mssql_conn2(db);
                string insert_error;
                int64_t insert_rows = 0;
                bool insert_ok = InsertFallbackTransfer(local_conn2, mssql_conn2,
                                                         table.name, bind_data.secret_name,
                                                         bind_data.target_schema,
                                                         table.name,
                                                         table.columns,
                                                         insert_error, insert_rows);
                if (insert_ok) {
                    result.rows_transferred = insert_rows;
                    result.method = "INSERT";
                    result.success = true;
                } else {
                    result.method = "";
                    result.error_message = "BCP: " + bcp_error + " | INSERT: " + insert_error;
                }
            }
        }

        auto table_end = std::chrono::high_resolution_clock::now();
        result.duration = FormatDuration(std::chrono::duration<double>(table_end - table_start).count());
        bind_data.results.push_back(std::move(result));
        state.current_table_idx++;

        // Yield between tables
        output.SetCardinality(0);
        return;
    }

    // -----------------------------------------------------------------
    // OUTPUT_RESULTS
    // -----------------------------------------------------------------
    case MovePhase::OUTPUT_RESULTS: {
        output_results();
        return;
    }

    // -----------------------------------------------------------------
    // DONE
    // -----------------------------------------------------------------
    default:
        output.SetCardinality(0);
        return;
    }
}

// ============================================================================
// Register the function
// ============================================================================

void RegisterMoveToMssqlFunction(DatabaseInstance &db) {
    TableFunction func("move_to_mssql", {}, MoveToMssqlExecute, MoveToMssqlBind, MoveToMssqlInitGlobal);

    func.named_parameters["secret"] = LogicalType::VARCHAR;
    func.named_parameters["all_tables"] = LogicalType::BOOLEAN;
    func.named_parameters["tables"] = LogicalType::LIST(LogicalType::VARCHAR);
    func.named_parameters["exclude"] = LogicalType::LIST(LogicalType::VARCHAR);
    func.named_parameters["schema"] = LogicalType::VARCHAR;

    Connection con(db);
    con.BeginTransaction();
    auto &catalog = Catalog::GetSystemCatalog(db);
    CreateTableFunctionInfo info(func);
    catalog.CreateFunction(*con.context, info);
    con.Commit();
}

} // namespace duckdb
