# `move_to_mssql` Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `move_to_mssql()` table function that bulk-transfers DuckDB tables to MSSQL Server via BCP with INSERT fallback.

**Architecture:** Extract shared BCP/MSSQL helpers from `into_wz_function.cpp` into a new `mssql_utils.hpp` header. Create `move_to_mssql_function.cpp` with a state-machine-based table function that discovers tables, creates them on MSSQL, and transfers data via BCP (falling back to batched INSERT VALUES). Register alongside the existing `into_wz` function.

**Tech Stack:** C++17, DuckDB extension API, BCP (Bulk Copy Program), MSSQL

**Design doc:** `docs/plans/2026-02-20-move-to-mssql-design.md`

---

### Task 1: Create `mssql_utils.hpp` with shared MSSQL/BCP utilities

Extract reusable code from `into_wz_function.cpp` into a new header-only file.

**Files:**
- Create: `src/include/mssql_utils.hpp`

**Step 1: Create the header file**

Create `src/include/mssql_utils.hpp` with these functions extracted and generalized from `into_wz_function.cpp` (lines 1047-1262):

```cpp
#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "wz_utils.hpp"
#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace duckdb {

// Batch sizes for fallback INSERT VALUES path
static constexpr size_t MSSQL_BULK_INSERT_BATCH_SIZE = 1000;
static constexpr size_t MSSQL_BULK_STMTS_PER_ROUNDTRIP = 5;

// ============================================================================
// MSSQL connection info
// ============================================================================

struct MssqlConnInfo {
    string server;
    string database;
    string user;
    string password;
    bool trusted;  // Windows authentication
};

// Parse ODBC-style connection string into MssqlConnInfo.
inline bool ParseConnectionString(const string &conn_str, MssqlConnInfo &info) {
    info.trusted = false;
    std::istringstream ss(conn_str);
    string token;
    while (std::getline(ss, token, ';')) {
        auto eq_pos = token.find('=');
        if (eq_pos == string::npos) continue;
        string key = token.substr(0, eq_pos);
        string val = token.substr(eq_pos + 1);
        while (!key.empty() && key.front() == ' ') key.erase(key.begin());
        while (!key.empty() && key.back() == ' ') key.pop_back();
        string lower_key;
        lower_key.resize(key.size());
        std::transform(key.begin(), key.end(), lower_key.begin(), ::tolower);

        if (lower_key == "server" || lower_key == "data source") {
            info.server = val;
        } else if (lower_key == "database" || lower_key == "initial catalog") {
            info.database = val;
        } else if (lower_key == "uid" || lower_key == "user id" || lower_key == "user") {
            info.user = val;
        } else if (lower_key == "pwd" || lower_key == "password") {
            info.password = val;
        } else if (lower_key == "trusted_connection" || lower_key == "integrated security") {
            string lower_val;
            lower_val.resize(val.size());
            std::transform(val.begin(), val.end(), lower_val.begin(), ::tolower);
            if (lower_val == "yes" || lower_val == "true" || lower_val == "sspi") {
                info.trusted = true;
            }
        }
    }
    return !info.server.empty() && !info.database.empty() &&
           (info.trusted || (!info.user.empty() && !info.password.empty()));
}

// Extract MSSQL connection info from a DuckDB secret via the C++ SecretManager API.
inline bool GetMssqlConnInfo(ClientContext &context, const string &secret_name, MssqlConnInfo &info) {
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &secret_manager = SecretManager::Get(db);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

    auto extract_info = [&](const KeyValueSecret *kv) -> bool {
        info = MssqlConnInfo();
        Value val;
        if (kv->TryGetValue("host", val) && !val.IsNull()) info.server = val.ToString();
        if (kv->TryGetValue("database", val) && !val.IsNull()) info.database = val.ToString();
        if (kv->TryGetValue("user", val) && !val.IsNull()) info.user = val.ToString();
        if (kv->TryGetValue("password", val) && !val.IsNull()) info.password = val.ToString();
        if (kv->TryGetValue("port", val) && !val.IsNull()) {
            string port_str = val.ToString();
            if (!port_str.empty() && port_str != "1433") {
                info.server += "," + port_str;
            }
        }
        return !info.server.empty() && !info.database.empty() &&
               (info.trusted || (!info.user.empty() && !info.password.empty()));
    };

    auto entry = secret_manager.GetSecretByName(transaction, secret_name);
    if (entry) {
        auto *kv = dynamic_cast<const KeyValueSecret *>(entry->secret.get());
        if (kv && extract_info(kv)) return true;
    }

    auto all = secret_manager.AllSecrets(transaction);
    for (auto &e : all) {
        string type = e.secret->GetType();
        std::transform(type.begin(), type.end(), type.begin(), ::tolower);
        if (type == "mssql") {
            auto *kv = dynamic_cast<const KeyValueSecret *>(e.secret.get());
            if (kv && extract_info(kv)) return true;
        }
    }

    return false;
}

// ============================================================================
// SQL execution helper
// ============================================================================

// Execute a SQL statement via a Connection and capture errors.
inline bool ExecuteMssqlStatement(Connection &conn, const string &sql, string &error_message) {
    auto result = conn.Query(sql);
    if (result->HasError()) {
        error_message = result->GetError();
        return false;
    }
    return true;
}

// ============================================================================
// BCP utilities
// ============================================================================

// Generate a BCP format file for character-mode import.
// Accepts a dynamic list of column names (not hardcoded).
// Uses \n (LF) as row terminator to match DuckDB COPY TO output.
inline bool GenerateBcpFormatFile(const string &fmt_path,
                                   const vector<string> &column_names,
                                   string &error_message) {
    std::ofstream f(fmt_path);
    if (!f.is_open()) {
        error_message = "Failed to create BCP format file: " + fmt_path;
        return false;
    }

    int num_cols = static_cast<int>(column_names.size());
    f << "14.0\n";
    f << num_cols << "\n";
    for (int i = 0; i < num_cols; i++) {
        const char *terminator = (i < num_cols - 1) ? "\\t" : "\\n";
        f << (i + 1) << "       SQLCHAR       0       8000      \""
          << terminator << "\"     " << (i + 1) << "     " << column_names[i] << "       \"\"\n";
    }

    f.close();
    return true;
}

// Invoke bcp.exe to bulk-load a data file into a table using a format file.
// full_table_name should be like "dbname.schema.tablename".
inline bool InvokeBcp(const MssqlConnInfo &info,
                       const string &full_table_name,
                       const string &csv_path,
                       const string &fmt_path,
                       string &error_message,
                       int64_t &rows_loaded) {
    rows_loaded = 0;

    string cmd = "bcp " + full_table_name + " in \"" + csv_path + "\"" +
                 " -S " + info.server +
                 " -f \"" + fmt_path + "\" -k -b 5000";

    if (info.trusted) {
        cmd += " -T";
    } else {
        cmd += " -U " + info.user + " -P " + info.password;
    }

    cmd += " 2>&1";

    string output;
#ifdef _WIN32
    FILE *pipe = _popen(cmd.c_str(), "r");
#else
    FILE *pipe = popen(cmd.c_str(), "r");
#endif
    if (!pipe) {
        error_message = "Failed to execute bcp command";
        return false;
    }

    char buffer[256];
    while (fgets(buffer, sizeof(buffer), pipe)) {
        output += buffer;
    }

#ifdef _WIN32
    int exit_code = _pclose(pipe);
#else
    int exit_code = pclose(pipe);
#endif

    if (exit_code != 0) {
        if (output.size() > 500) output = output.substr(0, 500) + "...";
        error_message = "bcp failed (exit " + std::to_string(exit_code) + "): " + output;
        return false;
    }

    auto pos = output.find("rows copied");
    if (pos != string::npos) {
        auto num_end = pos;
        while (num_end > 0 && output[num_end - 1] == ' ') num_end--;
        auto num_start = num_end;
        while (num_start > 0 && std::isdigit(output[num_start - 1])) num_start--;
        if (num_start < num_end) {
            rows_loaded = std::stoll(output.substr(num_start, num_end - num_start));
        }
    }

    return true;
}

} // namespace duckdb
```

**Key changes from original:**
- `GenerateBcpFormatFile` now accepts `vector<string> column_names` instead of parsing from PRIMANOTA_COLUMN_LIST
- `InvokeBcp` now accepts `full_table_name` (e.g., `"mydb.dbo.mytable"`) instead of building it with hardcoded `dbo`
- `ExecuteMssqlStatementWithConn` renamed to `ExecuteMssqlStatement` (cleaner name)
- All functions are `inline` (matching `wz_utils.hpp` pattern)

**Step 2: Commit**

```bash
git add src/include/mssql_utils.hpp
git commit -m "feat: extract shared MSSQL/BCP utilities into mssql_utils.hpp"
```

---

### Task 2: Refactor `into_wz_function.cpp` to use `mssql_utils.hpp`

Replace the duplicated code in `into_wz_function.cpp` with calls to the shared header.

**Files:**
- Modify: `src/into_wz_function.cpp`

**Step 1: Add the include**

At `src/into_wz_function.cpp:2`, after `#include "wz_utils.hpp"`, add:

```cpp
#include "mssql_utils.hpp"
```

**Step 2: Remove extracted code blocks**

Delete these sections from `into_wz_function.cpp`:
- Lines 1047-1092: `MssqlConnInfo` struct and `ParseConnectionString()`
- Lines 1094-1140: `GetMssqlConnInfo()` function
- Lines 584-593: `ExecuteMssqlStatementWithConn()` function

**Step 3: Update call sites**

Replace all calls to old function names with new ones:

1. Replace `ExecuteMssqlStatementWithConn(` → `ExecuteMssqlStatement(` (throughout file)

2. In `GenerateBcpFormatFile` call site (inside `BcpTransferPrimanota`, ~line 1290): the local `GenerateBcpFormatFile` still parses PRIMANOTA_COLUMN_LIST. Replace the local function with a wrapper that parses the column list and calls the shared version:

Replace the local `GenerateBcpFormatFile(const string &fmt_path, string &error_message)` (lines 1165-1194) with:

```cpp
static bool GeneratePrimanotaFormatFile(const string &fmt_path, string &error_message) {
    // Parse column names from PRIMANOTA_COLUMN_LIST
    vector<string> col_names;
    std::istringstream cols(PRIMANOTA_COLUMN_LIST);
    string col;
    while (std::getline(cols, col, ',')) {
        while (!col.empty() && col.front() == ' ') col.erase(col.begin());
        while (!col.empty() && col.back() == ' ') col.pop_back();
        if (!col.empty()) col_names.push_back(col);
    }
    return GenerateBcpFormatFile(fmt_path, col_names, error_message);
}
```

3. Update the call in `BcpTransferPrimanota` (~line 1290):
   - `GenerateBcpFormatFile(fmt_path, error_message)` → `GeneratePrimanotaFormatFile(fmt_path, error_message)`

4. Update `InvokeBcp` call in `BcpTransferPrimanota` (~line 1296):
   - `InvokeBcp(conn_info, "tblPrimanota", csv_path, fmt_path, error_message, rows_transferred)` → `InvokeBcp(conn_info, conn_info.database + ".dbo.tblPrimanota", csv_path, fmt_path, error_message, rows_transferred)`

**Step 4: Remove redundant includes**

These includes are now provided by `mssql_utils.hpp`, but keep them in `into_wz_function.cpp` if they're used for other purposes too (they are — `<sstream>`, `<fstream>`, `<filesystem>` are used elsewhere in the file). So **no include removals needed**.

**Step 5: Build and verify**

```bash
make clean && make release
```

Expected: Clean compilation, no errors. The extension binary at `build/release/extension/wz/wz.duckdb_extension` should be produced.

**Step 6: Commit**

```bash
git add src/into_wz_function.cpp
git commit -m "refactor: use shared mssql_utils.hpp in into_wz_function"
```

---

### Task 3: Add `RegisterMoveToMssqlFunction` declaration

**Files:**
- Modify: `src/include/wz_extension.hpp`
- Modify: `src/wz_extension.cpp`

**Step 1: Add declaration to header**

In `src/include/wz_extension.hpp`, after line 61 (`void RegisterIntoWzFunction(DatabaseInstance &db);`), add:

```cpp
void RegisterMoveToMssqlFunction(DatabaseInstance &db);
```

**Step 2: Add registration calls to entry points**

In `src/wz_extension.cpp`, add the call in all three entry points:

1. In `WzExtension::Load` (line 18), after `RegisterIntoWzFunction(db);`:
```cpp
    RegisterMoveToMssqlFunction(db);
```

2. In `wz_duckdb_cpp_init` (line 37), after `duckdb::RegisterIntoWzFunction(db);`:
```cpp
    duckdb::RegisterMoveToMssqlFunction(db);
```

3. In `wz_init` (line 43), after `duckdb::RegisterIntoWzFunction(db);`:
```cpp
    duckdb::RegisterMoveToMssqlFunction(db);
```

**Step 3: Commit**

```bash
git add src/include/wz_extension.hpp src/wz_extension.cpp
git commit -m "feat: register move_to_mssql function in extension entry points"
```

---

### Task 4: Create `move_to_mssql_function.cpp`

This is the main implementation file. It contains the bind function, state machine execute, type mapping, table discovery, and per-table transfer logic.

**Files:**
- Create: `src/move_to_mssql_function.cpp`

**Step 1: Write the complete implementation**

Create `src/move_to_mssql_function.cpp` with the following content. This is a large file (~600 lines), broken into sections:

**Section A: Includes, constants, type mapping**

```cpp
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
#include <sstream>

namespace duckdb {

// ============================================================================
// DuckDB type → MSSQL type mapping
// ============================================================================

static string MapDuckDBTypeToMssql(const string &duckdb_type) {
    // Normalize to uppercase for matching
    string upper = duckdb_type;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

    if (upper == "INTEGER" || upper == "INT") return "INT";
    if (upper == "BIGINT" || upper == "INT64" || upper == "LONG") return "BIGINT";
    if (upper == "SMALLINT" || upper == "INT16" || upper == "SHORT") return "SMALLINT";
    if (upper == "TINYINT" || upper == "INT8") return "TINYINT";
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

    // DECIMAL(p,s) — pass through
    if (upper.substr(0, 7) == "DECIMAL") return duckdb_type;

    // VARCHAR(n) — map to NVARCHAR(n) or NVARCHAR(MAX)
    if (upper.substr(0, 7) == "VARCHAR") {
        if (upper == "VARCHAR") return "NVARCHAR(MAX)";
        // Extract size: VARCHAR(123)
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
```

**Section B: Result struct and bind data**

```cpp
// ============================================================================
// Transfer result (one per table)
// ============================================================================

struct TableTransferResult {
    string table_name;
    int64_t rows_transferred;
    string method;       // "BCP", "INSERT", or ""
    string duration;     // hh:mm:ss
    bool success;
    string error_message;
};

// ============================================================================
// Column info for a discovered table
// ============================================================================

struct ColumnInfo {
    string name;
    string duckdb_type;
    string mssql_type;
};

// ============================================================================
// Table info for transfer queue
// ============================================================================

struct TableInfo {
    string name;
    vector<ColumnInfo> columns;
};

// ============================================================================
// Bind data
// ============================================================================

struct MoveToMssqlBindData : public TableFunctionData {
    string secret_name;
    string target_schema;
    vector<string> table_names;  // resolved list of tables to transfer

    vector<TableTransferResult> results;
    bool executed;
};

// ============================================================================
// Global state (state machine)
// ============================================================================

enum class MovePhase {
    DISCOVER_TABLES,    // Resolve table list + column info
    PROCESS_TABLES,     // Transfer one table per Execute call
    OUTPUT_RESULTS,     // Drain result rows
    DONE
};

struct MoveToMssqlGlobalState : public GlobalTableFunctionState {
    MovePhase phase;
    idx_t current_table_idx;   // which table we're processing
    idx_t current_result_idx;  // for output pagination
    vector<TableInfo> tables;  // discovered tables with column info

    std::chrono::high_resolution_clock::time_point total_start;

    MoveToMssqlGlobalState()
        : phase(MovePhase::DISCOVER_TABLES), current_table_idx(0), current_result_idx(0) {}
};
```

**Section C: Bind function**

```cpp
// ============================================================================
// Bind
// ============================================================================

static unique_ptr<FunctionData> MoveToMssqlBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto bind_data = make_uniq<MoveToMssqlBindData>();
    bind_data->executed = false;

    // Defaults
    bind_data->secret_name = "mssql_conn";
    bind_data->target_schema = "dbo";
    bool all_tables = true;
    vector<string> explicit_tables;
    vector<string> exclude_tables;

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        auto lower_name = kv.first;
        std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);

        if (lower_name == "secret") {
            bind_data->secret_name = kv.second.GetValue<string>();
        } else if (lower_name == "schema") {
            bind_data->target_schema = kv.second.GetValue<string>();
        } else if (lower_name == "all_tables") {
            all_tables = kv.second.GetValue<bool>();
        } else if (lower_name == "tables") {
            auto &list_value = kv.second;
            auto &children = ListValue::GetChildren(list_value);
            for (auto &child : children) {
                explicit_tables.push_back(child.GetValue<string>());
            }
        } else if (lower_name == "exclude") {
            auto &list_value = kv.second;
            auto &children = ListValue::GetChildren(list_value);
            for (auto &child : children) {
                exclude_tables.push_back(child.GetValue<string>());
            }
        }
    }

    // If explicit tables given, override all_tables
    if (!explicit_tables.empty()) {
        all_tables = false;
    }

    // Store resolved parameters for Execute phase to use
    // (actual table discovery happens in Execute to avoid long Bind)
    if (all_tables) {
        // Store exclude list; discovery happens in Execute
        bind_data->table_names.clear();
        // Use a sentinel: empty table_names + all_tables flag
        // We'll store excludes as negative markers
        for (auto &ex : exclude_tables) {
            string lower_ex = ex;
            std::transform(lower_ex.begin(), lower_ex.end(), lower_ex.begin(), ::tolower);
            bind_data->table_names.push_back("__EXCLUDE__:" + lower_ex);
        }
    } else {
        bind_data->table_names = std::move(explicit_tables);
    }

    // Validate secret name
    if (!IsValidSqlIdentifier(bind_data->secret_name)) {
        throw BinderException("move_to_mssql: invalid secret name: " + bind_data->secret_name);
    }

    // Validate schema
    if (!IsValidSqlIdentifier(bind_data->target_schema)) {
        throw BinderException("move_to_mssql: invalid schema name: " + bind_data->target_schema);
    }

    // Define return columns
    names = {"table_name", "rows_transferred", "method", "duration", "success", "error_message"};
    return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR,
                    LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR};

    return std::move(bind_data);
}
```

**Section D: Init function**

```cpp
// ============================================================================
// Init
// ============================================================================

static unique_ptr<GlobalTableFunctionState> MoveToMssqlInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
    return make_uniq<MoveToMssqlGlobalState>();
}
```

**Section E: Per-table transfer helpers**

```cpp
// ============================================================================
// Per-table transfer: BCP path
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

    // 1. Get connection info
    MssqlConnInfo conn_info;
    if (!GetMssqlConnInfo(context, secret_name, conn_info)) {
        error_message = "Could not extract MSSQL connection info from secret";
        return false;
    }

    // 2. Build column name list for export
    vector<string> col_names;
    col_names.reserve(columns.size());
    for (auto &c : columns) {
        col_names.push_back(c.name);
    }
    string col_list;
    for (size_t i = 0; i < col_names.size(); i++) {
        if (i > 0) col_list += ", ";
        col_list += "\"" + col_names[i] + "\"";
    }

    // 3. Export source table to TSV
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

    // 4. Generate format file
    if (!GenerateBcpFormatFile(fmt_path, col_names, error_message)) {
        std::filesystem::remove(csv_path);
        return false;
    }

    // 5. Invoke BCP
    string full_table = conn_info.database + "." + schema + "." + mssql_table_name;
    bool success = InvokeBcp(conn_info, full_table, csv_path, fmt_path, error_message, rows_transferred);

    // 6. Cleanup
    std::filesystem::remove(csv_path);
    std::filesystem::remove(fmt_path);

    return success;
}

// ============================================================================
// Per-table transfer: Batched INSERT VALUES fallback
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

    // Build column list
    string col_list;
    for (size_t i = 0; i < columns.size(); i++) {
        if (i > 0) col_list += ", ";
        col_list += "[" + columns[i].name + "]";
    }

    // Build SELECT column list (with quoting for DuckDB)
    string select_cols;
    for (size_t i = 0; i < columns.size(); i++) {
        if (i > 0) select_cols += ", ";
        select_cols += "\"" + columns[i].name + "\"";
    }

    // Read all rows from source
    string select_sql = "SELECT " + select_cols + " FROM \"" + source_table + "\"";
    auto result = local_conn.Query(select_sql);
    if (result->HasError()) {
        error_message = "Failed to read source table: " + result->GetError();
        return false;
    }

    idx_t col_count = result->types.size();
    string insert_prefix = "INSERT INTO " + db_name + "." + schema + "." +
                           mssql_table_name + " (" + col_list + ") VALUES ";

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
            if (rows_in_batch > 0) {
                values_sql += ',';
            }
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
```

**Section F: Execute function (state machine)**

```cpp
// ============================================================================
// Execute (state machine)
// ============================================================================

static void MoveToMssqlExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<MoveToMssqlBindData>();
    auto &state = data_p.global_state->Cast<MoveToMssqlGlobalState>();

    // Helper: output results to DataChunk
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

        // Determine table list
        vector<string> tables_to_transfer;
        set<string> exclude_set;

        bool discover_all = false;
        for (auto &name : bind_data.table_names) {
            if (name.substr(0, 11) == "__EXCLUDE__:") {
                exclude_set.insert(name.substr(11));
                discover_all = true;
            }
        }
        if (bind_data.table_names.empty()) {
            discover_all = true;
        }

        if (discover_all) {
            // Query information_schema for all base tables
            auto result = conn.Query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
                "ORDER BY table_name");
            if (result->HasError()) {
                TableTransferResult err_result;
                err_result.table_name = "ERROR";
                err_result.rows_transferred = 0;
                err_result.method = "";
                err_result.duration = "00:00:00";
                err_result.success = false;
                err_result.error_message = "Failed to discover tables: " + result->GetError();
                bind_data.results.push_back(std::move(err_result));
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
            tables_to_transfer = bind_data.table_names;
        }

        if (tables_to_transfer.empty()) {
            TableTransferResult err_result;
            err_result.table_name = "ERROR";
            err_result.rows_transferred = 0;
            err_result.method = "";
            err_result.duration = "00:00:00";
            err_result.success = false;
            err_result.error_message = "No tables found to transfer";
            bind_data.results.push_back(std::move(err_result));
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
                // Skip this table, add error result
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
                    col.name = chunk.data[0].GetValue(row).ToString();       // column_name
                    col.duckdb_type = chunk.data[1].GetValue(row).ToString(); // column_type
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

        // Output a progress indicator (yield back to DuckDB)
        output.SetCardinality(0);
        return;
    }

    // -----------------------------------------------------------------
    // PROCESS_TABLES — one table per Execute call
    // -----------------------------------------------------------------
    case MovePhase::PROCESS_TABLES: {
        if (state.current_table_idx >= state.tables.size()) {
            // All tables processed, add summary and move to output
            auto total_end = std::chrono::high_resolution_clock::now();
            double total_dur = std::chrono::duration<double>(total_end - state.total_start).count();

            int64_t total_rows = 0;
            int success_count = 0;
            int total_count = static_cast<int>(bind_data.results.size());
            for (auto &r : bind_data.results) {
                if (r.success) {
                    total_rows += r.rows_transferred;
                    success_count++;
                }
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
        Connection mssql_conn(db);

        TableTransferResult result;
        result.table_name = table.name;
        result.rows_transferred = 0;
        result.success = false;

        string error_msg;

        // 1. DROP + CREATE on MSSQL
        string drop_sql = "DROP TABLE IF EXISTS " + bind_data.secret_name +
                          ".[" + bind_data.target_schema + "].[" + table.name + "]";
        if (!ExecuteMssqlStatement(mssql_conn, drop_sql, error_msg)) {
            // Table might not exist, continue anyway
        }

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

        // 2. Try BCP transfer
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
            string insert_error;
            int64_t insert_rows = 0;

            // Get MSSQL connection info for database name
            MssqlConnInfo conn_info;
            string db_name = bind_data.secret_name;
            if (GetMssqlConnInfo(context, bind_data.secret_name, conn_info)) {
                db_name = conn_info.database;
            }

            Connection local_conn2(db);
            Connection mssql_conn2(db);
            bool insert_ok = InsertFallbackTransfer(local_conn2, mssql_conn2,
                                                     table.name, bind_data.secret_name,
                                                     "[" + bind_data.target_schema + "]",
                                                     "[" + table.name + "]",
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

        auto table_end = std::chrono::high_resolution_clock::now();
        result.duration = FormatDuration(std::chrono::duration<double>(table_end - table_start).count());
        bind_data.results.push_back(std::move(result));

        state.current_table_idx++;

        // Yield to DuckDB between tables (enables progress updates)
        output.SetCardinality(0);
        return;
    }

    // -----------------------------------------------------------------
    // OUTPUT_RESULTS — drain result rows
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
```

**Section G: Registration**

```cpp
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
```

**Step 2: Commit**

```bash
git add src/move_to_mssql_function.cpp
git commit -m "feat: add move_to_mssql table function for bulk DuckDB-to-MSSQL transfer"
```

---

### Task 5: Update CMakeLists.txt

**Files:**
- Modify: `CMakeLists.txt`

**Step 1: Add new source file**

In `CMakeLists.txt`, update the `EXTENSION_SOURCES` block (line 16-20) to:

```cmake
set(EXTENSION_SOURCES
    src/wz_extension.cpp
    src/into_wz_function.cpp
    src/constraint_checker.cpp
    src/move_to_mssql_function.cpp
)
```

**Step 2: Commit**

```bash
git add CMakeLists.txt
git commit -m "build: add move_to_mssql_function.cpp to extension sources"
```

---

### Task 6: Build and verify

**Files:** None (build verification only)

**Step 1: Clean build**

```bash
make clean && make release
```

Expected: Clean compilation, no errors. Output binary at `build/release/extension/wz/wz.duckdb_extension`.

**Step 2: Smoke test — extension loads**

```bash
duckdb -unsigned -c "LOAD 'build/release/extension/wz/wz.duckdb_extension'; SELECT 'OK';"
```

Expected: Outputs `OK`.

**Step 3: Smoke test — function exists**

```bash
duckdb -unsigned -c "
LOAD 'build/release/extension/wz/wz.duckdb_extension';
SELECT function_name FROM duckdb_functions() WHERE function_name = 'move_to_mssql';
"
```

Expected: Outputs `move_to_mssql`.

**Step 4: Smoke test — runs with no tables**

```bash
duckdb -unsigned -c "
LOAD 'build/release/extension/wz/wz.duckdb_extension';
CREATE SECRET mssql_conn (TYPE MSSQL, HOST 'localhost', DATABASE 'test', USER 'sa', PASSWORD 'test');
SELECT * FROM move_to_mssql(secret := 'mssql_conn');
"
```

Expected: Returns result with "No tables found to transfer" error or similar (since there are no base tables).

**Step 5: Fix any compilation errors**

If compilation fails, fix the errors and rebuild. Common issues:
- Missing includes
- Name collisions between `mssql_utils.hpp` inline functions and `into_wz_function.cpp` static functions (solution: ensure old static functions are fully removed)
- DuckDB API changes (check `result->types[i]` pattern, `ListValue::GetChildren`)

**Step 6: Commit fixes if any**

```bash
git add -A && git commit -m "fix: resolve compilation issues in move_to_mssql"
```

---

### Task 7: Full integration test with live MSSQL (manual)

This requires a live MSSQL connection. Test script:

```sql
-- Load extension
LOAD 'build/release/extension/wz/wz.duckdb_extension';

-- Create test tables
CREATE TABLE test_ints (id INTEGER, val BIGINT, flag BOOLEAN);
INSERT INTO test_ints VALUES (1, 100, true), (2, 200, false), (3, 300, true);

CREATE TABLE test_strings (name VARCHAR, description VARCHAR);
INSERT INTO test_strings VALUES ('hello', 'world'), ('foo', 'bar');

CREATE TABLE test_dates (dt DATE, ts TIMESTAMP, amount DECIMAL(10,2));
INSERT INTO test_dates VALUES ('2024-01-15', '2024-01-15 10:30:00', 99.95);

-- Transfer all tables
SELECT * FROM move_to_mssql(secret := 'mssql_conn');

-- Transfer specific tables
SELECT * FROM move_to_mssql(secret := 'mssql_conn', tables := ['test_ints']);

-- Transfer with exclusion
SELECT * FROM move_to_mssql(secret := 'mssql_conn', exclude := ['test_dates']);

-- Custom schema
SELECT * FROM move_to_mssql(secret := 'mssql_conn', schema := 'staging');
```

Expected per test:
- Result set with one row per table + SUMMARY row
- `success = true` for each table
- `method = BCP` (or `INSERT` if BCP not available)
- `rows_transferred` matches source row count
- Tables exist on MSSQL with correct column types and data

---

## Summary of files changed

| File | Action | Purpose |
|------|--------|---------|
| `src/include/mssql_utils.hpp` | CREATE | Shared BCP/MSSQL utilities |
| `src/into_wz_function.cpp` | MODIFY | Remove extracted code, use mssql_utils.hpp |
| `src/include/wz_extension.hpp` | MODIFY | Add RegisterMoveToMssqlFunction declaration |
| `src/wz_extension.cpp` | MODIFY | Call RegisterMoveToMssqlFunction in entry points |
| `src/move_to_mssql_function.cpp` | CREATE | New table function implementation |
| `CMakeLists.txt` | MODIFY | Add new source file |
