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
    string source_schema;      // DuckDB schema this table came from
    string target_schema;      // MSSQL schema to create this table in
    vector<ColumnInfo> columns;
    // Pre-built SQL strings (built in Step 3, used in Step 4)
    // IMPORTANT: All fields below MUST be populated in Step 3 before any MSSQL
    // operations. MSSQL extension DDL corrupts the heap, so vectors/strings
    // read after DDL ops return garbage. Only use these pre-built values in Step 4.
    string drop_sql;
    string create_sql;
    string col_list;           // quoted column names for SELECT/INSERT
    vector<string> col_names;  // unquoted column names for BCP format file
};

struct MoveToMssqlBindData : public TableFunctionData {
    string secret_name;
    string target_schema;
    string duckdb_schema;
    string duckdb_catalog;
    bool all_tables;
    bool all_duckdb_schemas;      // true when duckdb_schema='all'
    bool mssql_schema_explicit;   // true when user explicitly set mssql_schema/schema
    string source_path;           // file path to auto-attach as source DuckDB database
    vector<string> explicit_tables;
    vector<string> exclude_tables;

    // Populated during Execute
    vector<TableTransferResult> results;
    bool executed;
};

struct MoveToMssqlGlobalState : public GlobalTableFunctionState {
    idx_t current_result_idx;
    MoveToMssqlGlobalState() : current_result_idx(0) {}
};

// ============================================================================
// Bind — ONLY parse parameters, no Connection or Query calls
// ============================================================================

static unique_ptr<FunctionData> MoveToMssqlBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto bind_data = make_uniq<MoveToMssqlBindData>();
    bind_data->executed = false;
    bind_data->secret_name = "mssql_conn";
    bind_data->target_schema = "dbo";
    bind_data->duckdb_schema = "main";
    bind_data->duckdb_catalog = "memory";
    bind_data->all_tables = true;
    bind_data->all_duckdb_schemas = false;
    bind_data->mssql_schema_explicit = false;

    for (auto &kv : input.named_parameters) {
        auto lower_name = kv.first;
        std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);

        if (lower_name == "secret") {
            bind_data->secret_name = kv.second.GetValue<string>();
        } else if (lower_name == "schema" || lower_name == "mssql_schema") {
            bind_data->target_schema = kv.second.GetValue<string>();
            bind_data->mssql_schema_explicit = true;
        } else if (lower_name == "duckdb_catalog") {
            bind_data->duckdb_catalog = kv.second.GetValue<string>();
        } else if (lower_name == "duckdb_schema") {
            auto val = kv.second.GetValue<string>();
            string val_lower = val;
            std::transform(val_lower.begin(), val_lower.end(), val_lower.begin(), ::tolower);
            if (val_lower == "all") {
                bind_data->all_duckdb_schemas = true;
            }
            bind_data->duckdb_schema = val;
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
        } else if (lower_name == "source") {
            bind_data->source_path = kv.second.GetValue<string>();
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
    if (!bind_data->all_duckdb_schemas && !IsValidSqlIdentifier(bind_data->duckdb_schema)) {
        throw BinderException("move_to_mssql: invalid duckdb_schema name: " + bind_data->duckdb_schema);
    }
    if (!IsValidSqlIdentifier(bind_data->duckdb_catalog)) {
        throw BinderException("move_to_mssql: invalid duckdb_catalog name: " + bind_data->duckdb_catalog);
    }

    // Define output columns — no database access needed here
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
                              const vector<string> &col_names,
                              const string &col_list,
                              const string &duckdb_catalog,
                              const string &duckdb_schema,
                              string &error_message,
                              int64_t &rows_transferred) {
    rows_transferred = 0;

    MssqlConnInfo conn_info;
    if (!GetMssqlConnInfo(context, secret_name, conn_info)) {
        error_message = "Could not extract MSSQL connection info from secret";
        return false;
    }

    // Export to TSV
    auto temp_dir = std::filesystem::temp_directory_path();
    string safe_name = source_table;
    for (char &c : safe_name) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') c = '_';
    }
    string csv_path = (temp_dir / ("mssql_transfer_" + safe_name + ".tsv")).string();
    string fmt_path = (temp_dir / ("mssql_transfer_" + safe_name + ".fmt")).string();

    string duckdb_prefix = "\"" + duckdb_catalog + "\".\"" + duckdb_schema + "\".";
    string export_sql = "COPY (SELECT " + col_list + " FROM " + duckdb_prefix + "\"" + source_table +
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
                                    const string &col_list,
                                    const string &duckdb_catalog,
                                    const string &duckdb_schema,
                                    string &error_message,
                                    int64_t &rows_transferred) {
    rows_transferred = 0;

    // Read source
    string duckdb_prefix = "\"" + duckdb_catalog + "\".\"" + duckdb_schema + "\".";
    string select_sql = "SELECT " + col_list + " FROM " + duckdb_prefix + "\"" + source_table + "\"";
    auto result = local_conn.Query(select_sql);
    if (result->HasError()) {
        error_message = "Failed to read source table: " + result->GetError();
        return false;
    }

    idx_t col_count = result->types.size();
    string insert_prefix = "INSERT INTO " + db_name + "." + schema + ".\"" +
                           mssql_table_name + "\" (" + col_list + ") VALUES ";

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
                // Check null via validity mask (no UTF-8 validation)
                auto &validity = FlatVector::Validity(chunk.data[col]);
                if (!validity.RowIsValid(row)) {
                    values_sql += "NULL";
                } else {
                    auto &type = result->types[col];
                    if (type == LogicalType::INTEGER || type == LogicalType::BIGINT ||
                        type == LogicalType::SMALLINT || type == LogicalType::TINYINT ||
                        type == LogicalType::BOOLEAN) {
                        // Numeric/bool: GetValue is safe (no UTF-8 issues)
                        Value val = chunk.data[col].GetValue(row);
                        values_sql += val.ToString();
                    } else if (type.InternalType() == PhysicalType::VARCHAR) {
                        // String types: raw byte access to bypass UTF-8 validation.
                        // Data from attached databases may contain non-UTF-8 bytes
                        // (e.g. Windows-1252 encoded German text with umlauts).
                        auto str_data = FlatVector::GetData<string_t>(chunk.data[col]);
                        values_sql += '\'';
                        values_sql += EscapeSqlString(str_data[row].GetString());
                        values_sql += '\'';
                    } else {
                        // Other types (DATE, TIMESTAMP, DECIMAL, UUID, etc.)
                        Value val = chunk.data[col].GetValue(row);
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
// Execute — all work in first call, then output results
// ============================================================================

static void MoveToMssqlExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<MoveToMssqlBindData>();
    auto &state = data_p.global_state->Cast<MoveToMssqlGlobalState>();

    // First call: discover tables, do all transfers, collect results
    if (!bind_data.executed) {
        bind_data.executed = true;
        auto total_start = std::chrono::high_resolution_clock::now();

        auto &db = DatabaseInstance::GetDatabase(context);
        MssqlConnInfo validated_conn_info;

        // Step 0: Auto-attach source database file if 'source' parameter is set
        if (!bind_data.source_path.empty()) {
            std::filesystem::path p(bind_data.source_path);
            string catalog_name = p.stem().string();

            Connection attach_conn(db);
            string attach_sql = "ATTACH '" + EscapeSqlString(bind_data.source_path) +
                                "' AS \"" + catalog_name + "\"";
            auto attach_result = attach_conn.Query(attach_sql);
            if (attach_result->HasError()) {
                // Might already be attached — verify by checking information_schema
                Connection check_conn(db);
                auto check_result = check_conn.Query(
                    "SELECT 1 FROM information_schema.schemata WHERE catalog_name = '" +
                    EscapeSqlString(catalog_name) + "' LIMIT 1");
                if (check_result->HasError() ||
                    check_result->Collection().Count() == 0) {
                    TableTransferResult err;
                    err.table_name = "ERROR";
                    err.rows_transferred = 0;
                    err.method = "";
                    err.duration = "00:00:00";
                    err.success = false;
                    err.error_message = "Failed to attach database '" +
                                        bind_data.source_path + "': " +
                                        attach_result->GetError();
                    bind_data.results.push_back(std::move(err));
                    goto output_results;
                }
            }

            // Override duckdb_catalog with the derived catalog name
            bind_data.duckdb_catalog = catalog_name;
        }

        // Step 1: Validate MSSQL secret exists
        {
            bool secret_ok = false;
            try {
                secret_ok = GetMssqlConnInfo(context, bind_data.secret_name, validated_conn_info);
            } catch (...) {
                secret_ok = false;
            }
            if (!secret_ok) {
                TableTransferResult err;
                err.table_name = "ERROR";
                err.rows_transferred = 0;
                err.method = "";
                err.duration = "00:00:00";
                err.success = false;
                err.error_message = "Could not find MSSQL secret '" + bind_data.secret_name +
                                    "'. Create one with: CREATE SECRET my_secret (TYPE mssql, host '...', database '...', user '...', password '...')";
                bind_data.results.push_back(std::move(err));
                goto output_results;
            }
        }

        // Step 1b: Ensure target database exists (via sqlcmd against master)
        {
            string target_db = validated_conn_info.database;
            // Escape ] as ]] for bracket-delimited identifiers
            string escaped_db;
            for (char c : target_db) {
                if (c == ']') escaped_db += "]]";
                else escaped_db += c;
            }

            string check_create_sql =
                "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N''" +
                EscapeSqlString(target_db) + "'') CREATE DATABASE [" + escaped_db + "]";
            string sqlcmd_output, sqlcmd_error;
            if (!ExecuteSqlCmd(validated_conn_info, "master", check_create_sql,
                               sqlcmd_output, sqlcmd_error)) {
                // Not fatal — database might already exist, or sqlcmd might not be available.
                // Log as informational result row.
                TableTransferResult info_row;
                info_row.table_name = "DB_CREATE";
                info_row.rows_transferred = 0;
                info_row.method = "sqlcmd";
                info_row.duration = "00:00:00";
                info_row.success = false;
                info_row.error_message = "Could not auto-create database '" + target_db +
                                          "' (may already exist): " + sqlcmd_error;
                bind_data.results.push_back(std::move(info_row));
            } else {
                TableTransferResult info_row;
                info_row.table_name = "DB_CREATE";
                info_row.rows_transferred = 0;
                info_row.method = "sqlcmd";
                info_row.duration = "00:00:00";
                info_row.success = true;
                info_row.error_message = "Database '" + target_db + "' ensured";
                bind_data.results.push_back(std::move(info_row));
            }
        }

        // Step 2: Discover table names (Connection + Query is safe in Execute)
        // NOTE: Heap-allocate local data structures because MSSQL extension
        // operations via attached databases corrupt the heap. Destructors for
        // stack-allocated objects (vectors, Connection) segfault after MSSQL ops.
        // We intentionally leak these to avoid the crash — a few KB per call.
        {
            // Track (source_schema, table_name) pairs
            struct TableSource {
                string schema;
                string name;
            };
            auto *table_sources_ptr = new vector<TableSource>();
            auto &table_sources = *table_sources_ptr;

            auto *local_conn_ptr = new Connection(db);
            auto &local_conn = *local_conn_ptr;

            if (bind_data.all_tables) {
                set<string> exclude_set;
                for (auto &ex : bind_data.exclude_tables) {
                    string lower_ex = ex;
                    std::transform(lower_ex.begin(), lower_ex.end(), lower_ex.begin(), ::tolower);
                    exclude_set.insert(lower_ex);
                }

                string query =
                    "SELECT table_schema, table_name FROM information_schema.tables "
                    "WHERE table_catalog = '" + EscapeSqlString(bind_data.duckdb_catalog) + "' "
                    "AND table_type = 'BASE TABLE' ";
                if (bind_data.all_duckdb_schemas) {
                    // Skip system schemas when discovering all schemas
                    query += "AND table_schema NOT IN ('information_schema', 'pg_catalog') ";
                } else {
                    query += "AND table_schema = '" + EscapeSqlString(bind_data.duckdb_schema) + "' ";
                }
                query += "ORDER BY table_schema, table_name";

                auto result = local_conn.Query(query);
                if (!result->HasError()) {
                    for (auto &chunk : result->Collection().Chunks()) {
                        for (idx_t row = 0; row < chunk.size(); row++) {
                            string tschema = chunk.data[0].GetValue(row).ToString();
                            string tname = chunk.data[1].GetValue(row).ToString();
                            string tname_lower = tname;
                            std::transform(tname_lower.begin(), tname_lower.end(), tname_lower.begin(), ::tolower);
                            if (exclude_set.find(tname_lower) == exclude_set.end()) {
                                table_sources.push_back({tschema, tname});
                            }
                        }
                    }
                }
            } else {
                // Explicit table list — use duckdb_schema (or 'main' if 'all')
                string src_schema = bind_data.all_duckdb_schemas ? "main" : bind_data.duckdb_schema;
                for (auto &t : bind_data.explicit_tables) {
                    table_sources.push_back({src_schema, t});
                }
            }

            if (table_sources.empty()) {
                TableTransferResult err;
                err.table_name = "ERROR";
                err.rows_transferred = 0;
                err.method = "";
                err.duration = "00:00:00";
                err.success = false;
                err.error_message = "No tables found to transfer";
                bind_data.results.push_back(std::move(err));
                goto output_results;
            }

            // Step 3: Discover columns for each table using information_schema
            // Heap-allocate to avoid destructor crash (see note at Step 2)
            auto *tables_ptr = new vector<TableInfo>();
            auto &tables_to_transfer = *tables_ptr;
            for (auto &ts : table_sources) {
                TableInfo info;
                info.name = ts.name;
                info.source_schema = ts.schema;
                // Determine target MSSQL schema:
                // - If user explicitly set mssql_schema, use that for all tables
                // - Otherwise, mirror the DuckDB schema name
                info.target_schema = bind_data.mssql_schema_explicit
                                         ? bind_data.target_schema
                                         : ts.schema;

                auto col_result = local_conn.Query(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_catalog = '" + EscapeSqlString(bind_data.duckdb_catalog) + "' "
                    "AND table_schema = '" + EscapeSqlString(ts.schema) + "' "
                    "AND table_name = '" + EscapeSqlString(ts.name) + "' "
                    "ORDER BY ordinal_position");
                if (col_result->HasError()) {
                    TableTransferResult err;
                    err.table_name = ts.name;
                    err.rows_transferred = 0;
                    err.method = "";
                    err.duration = "00:00:00";
                    err.success = false;
                    err.error_message = "Failed to get columns: " + col_result->GetError();
                    bind_data.results.push_back(std::move(err));
                    continue;
                }

                // Build SQL strings directly from query results
                string create_cols;
                string col_list_tmp;
                vector<string> col_names;  // for BCP
                bool has_cols = false;

                for (auto &chunk : col_result->Collection().Chunks()) {
                    for (idx_t row = 0; row < chunk.size(); row++) {
                        string col_name = chunk.data[0].GetValue(row).ToString();
                        string col_type = chunk.data[1].GetValue(row).ToString();
                        if (has_cols) { create_cols += ", "; col_list_tmp += ", "; }
                        create_cols += "\"" + col_name + "\" " + col_type;
                        col_list_tmp += "\"" + col_name + "\"";
                        col_names.push_back(col_name);

                        ColumnInfo ci;
                        ci.name = col_name;
                        ci.duckdb_type = col_type;
                        ci.mssql_type = MapDuckDBTypeToMssql(col_type);
                        info.columns.push_back(std::move(ci));
                        has_cols = true;
                    }
                }

                if (has_cols) {
                    info.drop_sql = "DROP TABLE IF EXISTS " + bind_data.secret_name +
                                    "." + info.target_schema + ".\"" + info.name + "\"";
                    info.create_sql = "CREATE TABLE " + bind_data.secret_name +
                                      "." + info.target_schema + ".\"" + info.name + "\" (" +
                                      create_cols + ")";
                    info.col_list = col_list_tmp;
                    info.col_names = col_names;

                    tables_to_transfer.push_back(std::move(info));
                }
            }

            // Step 3b: Ensure all target schemas exist in MSSQL
            {
                set<string> needed_schemas;
                for (auto &t : tables_to_transfer) {
                    if (t.target_schema != "dbo") {
                        needed_schemas.insert(t.target_schema);
                    }
                }

                for (auto &schema_name : needed_schemas) {
                    // Check if schema exists
                    Connection schema_conn(db);
                    string schema_check_sql =
                        "SELECT * FROM mssql_scan('" + EscapeSqlString(bind_data.secret_name) +
                        "', $$SELECT 1 AS exists_flag FROM sys.schemas WHERE name = '" +
                        EscapeSqlString(schema_name) + "'$$)";
                    auto schema_result = schema_conn.Query(schema_check_sql);

                    bool schema_exists = false;
                    if (!schema_result->HasError()) {
                        for (auto &chunk : schema_result->Collection().Chunks()) {
                            if (chunk.size() > 0) schema_exists = true;
                        }
                    }

                    if (!schema_exists) {
                        // Try creating via mssql_scan first
                        Connection create_conn(db);
                        string create_sql =
                            "SELECT * FROM mssql_scan('" + EscapeSqlString(bind_data.secret_name) +
                            "', $$EXEC('CREATE SCHEMA [" + EscapeSqlString(schema_name) +
                            "]'); SELECT 1 AS ok$$)";
                        auto create_result = create_conn.Query(create_sql);
                        if (create_result->HasError()) {
                            // Fallback: try via sqlcmd
                            string escaped_schema;
                            for (char c : schema_name) {
                                if (c == ']') escaped_schema += "]]";
                                else escaped_schema += c;
                            }
                            string sqlcmd_sql =
                                "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N''" +
                                EscapeSqlString(schema_name) + "'') EXEC(''CREATE SCHEMA [" +
                                escaped_schema + "]'')";
                            string sqlcmd_output, sqlcmd_error;
                            if (!ExecuteSqlCmd(validated_conn_info, validated_conn_info.database,
                                               sqlcmd_sql, sqlcmd_output, sqlcmd_error)) {
                                TableTransferResult err;
                                err.table_name = "SCHEMA_CREATE";
                                err.rows_transferred = 0;
                                err.method = "";
                                err.duration = "00:00:00";
                                err.success = false;
                                err.error_message = "Failed to create MSSQL schema '" +
                                                    schema_name + "': " + create_result->GetError() +
                                                    " | sqlcmd: " + sqlcmd_error;
                                bind_data.results.push_back(std::move(err));
                                goto output_results;
                            }
                        }
                    }
                }
            }

            // Step 4: Transfer each table
            for (auto &table : tables_to_transfer) {
                auto table_start = std::chrono::high_resolution_clock::now();

                TableTransferResult result;
                result.table_name = table.name;
                result.rows_transferred = 0;
                result.success = false;

                try {
                    string error_msg;

                    // Use pre-built SQL strings (built in Step 3 while data was fresh)
                    // Use a SEPARATE Connection for MSSQL DDL operations
                    {
                        Connection mssql_conn(db);
                        ExecuteMssqlStatement(mssql_conn, table.drop_sql, error_msg);
                    }

                    bool create_ok = false;
                    {
                        Connection mssql_conn(db);
                        create_ok = ExecuteMssqlStatement(mssql_conn, table.create_sql, error_msg);
                    }

                    if (!create_ok) {
                        result.method = "";
                        result.error_message = "Failed to create MSSQL table: " + error_msg;
                        auto table_end = std::chrono::high_resolution_clock::now();
                        result.duration = FormatDuration(std::chrono::duration<double>(table_end - table_start).count());
                        bind_data.results.push_back(std::move(result));
                        continue;
                    }

                    // 4c. Try BCP transfer — uses local_conn for COPY TO export only
                    {
                        int64_t rows = 0;
                        string bcp_error;
                        bool bcp_ok = BcpTransferTable(local_conn, context, bind_data.secret_name,
                                                        table.name, table.target_schema, table.name,
                                                        table.col_names, table.col_list,
                                                        bind_data.duckdb_catalog, table.source_schema,
                                                        bcp_error, rows);

                        if (bcp_ok) {
                            result.rows_transferred = rows;
                            result.method = "BCP";
                            result.success = true;
                        } else {
                            // 4d. Fallback to INSERT VALUES — use separate connections
                            Connection read_conn(db);
                            Connection write_conn(db);
                            string insert_error;
                            int64_t insert_rows = 0;
                            bool insert_ok = InsertFallbackTransfer(read_conn, write_conn,
                                                                     table.name, bind_data.secret_name,
                                                                     table.target_schema,
                                                                     table.name,
                                                                     table.col_list,
                                                                     bind_data.duckdb_catalog, table.source_schema,
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
                } catch (std::exception &e) {
                    result.error_message = string("Exception: ") + e.what();
                } catch (...) {
                    result.error_message = "Unknown exception during transfer";
                }

                auto table_end = std::chrono::high_resolution_clock::now();
                result.duration = FormatDuration(std::chrono::duration<double>(table_end - table_start).count());
                bind_data.results.push_back(std::move(result));
            }
        }

        output_results:

        // Add summary row
        {
            auto total_end = std::chrono::high_resolution_clock::now();
            double total_dur = std::chrono::duration<double>(total_end - total_start).count();

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
        }
    }

    // Output results (may span multiple Execute calls for large result sets)
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
    func.named_parameters["mssql_schema"] = LogicalType::VARCHAR;
    func.named_parameters["duckdb_schema"] = LogicalType::VARCHAR;
    func.named_parameters["duckdb_catalog"] = LogicalType::VARCHAR;
    func.named_parameters["source"] = LogicalType::VARCHAR;

    Connection con(db);
    con.BeginTransaction();
    auto &catalog = Catalog::GetSystemCatalog(db);
    CreateTableFunctionInfo info(func);
    catalog.CreateFunction(*con.context, info);
    con.Commit();
}

} // namespace duckdb
