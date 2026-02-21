#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "wz_utils.hpp"
#include <algorithm>
#include <cstdio>
#include <fstream>
#include <sstream>

namespace duckdb {

// ============================================================================
// Batch size constants for MSSQL bulk operations
// ============================================================================

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

// ============================================================================
// Connection string parsing
// ============================================================================

// Parse MSSQL connection string into components.
// Supports both ODBC-style "Server=x;Database=y;Uid=z;Pwd=w" and
// key variations (UID/User ID, PWD/Password, etc.)
inline bool ParseConnectionString(const string &conn_str, MssqlConnInfo &info) {
    info.trusted = false;
    // Split by semicolons, parse key=value pairs
    std::istringstream ss(conn_str);
    string token;
    while (std::getline(ss, token, ';')) {
        auto eq_pos = token.find('=');
        if (eq_pos == string::npos) continue;
        string key = token.substr(0, eq_pos);
        string val = token.substr(eq_pos + 1);
        // Trim whitespace
        while (!key.empty() && key.front() == ' ') key.erase(key.begin());
        while (!key.empty() && key.back() == ' ') key.pop_back();
        // Case-insensitive key matching
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

// ============================================================================
// SecretManager-based connection info extraction
// ============================================================================

// Try to extract MSSQL connection info from a DuckDB secret using the C++ SecretManager API.
// This bypasses the duckdb_secrets(redact=false) SQL function which is blocked by default in v1.4+.
inline bool GetMssqlConnInfo(ClientContext &context, const string &secret_name, MssqlConnInfo &info) {
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &secret_manager = SecretManager::Get(db);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

    // Helper: extract host/database/user/password from a KeyValueSecret
    auto extract_info = [&](const KeyValueSecret *kv) -> bool {
        info = MssqlConnInfo();  // reset
        Value val;
        if (kv->TryGetValue("host", val) && !val.IsNull()) info.server = val.ToString();
        if (kv->TryGetValue("database", val) && !val.IsNull()) info.database = val.ToString();
        if (kv->TryGetValue("user", val) && !val.IsNull()) info.user = val.ToString();
        if (kv->TryGetValue("password", val) && !val.IsNull()) info.password = val.ToString();
        // Append non-default port to server (bcp uses "server,port" syntax)
        if (kv->TryGetValue("port", val) && !val.IsNull()) {
            string port_str = val.ToString();
            if (!port_str.empty() && port_str != "1433") {
                info.server += "," + port_str;
            }
        }
        return !info.server.empty() && !info.database.empty() &&
               (info.trusted || (!info.user.empty() && !info.password.empty()));
    };

    // Try 1: Look up secret by exact name (works when user passes the actual secret name)
    auto entry = secret_manager.GetSecretByName(transaction, secret_name);
    if (entry) {
        auto *kv = dynamic_cast<const KeyValueSecret *>(entry->secret.get());
        if (kv && extract_info(kv)) return true;
    }

    // Try 2: Scan all secrets for the first MSSQL-type secret
    // (handles the common case where secret param is the DB alias, not the secret name)
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

// Execute a SQL statement via a DuckDB Connection, capturing any error.
// Returns true on success, false on error (with error_message populated).
inline bool ExecuteMssqlStatement(Connection &conn,
                                  const string &sql,
                                  string &error_message) {
    auto result = conn.Query(sql);
    if (result->HasError()) {
        error_message = result->GetError();
        return false;
    }
    return true;
}

// ============================================================================
// BCP format file generation
// ============================================================================

// Generate a BCP format file for character-mode import.
// Uses \n (LF) as the row terminator to match DuckDB COPY TO output.
// Without this, BCP defaults to \r\n which misparses the last column.
// column_names: the ordered list of column names for the target table.
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

// ============================================================================
// BCP invocation
// ============================================================================

// Invoke bcp.exe to bulk-load a data file into a table using a format file.
// full_table_name: fully qualified table name (e.g. "mydb.dbo.mytable").
// Returns true on success, sets error_message on failure.
inline bool InvokeBcp(const MssqlConnInfo &info, const string &full_table_name,
                      const string &csv_path, const string &fmt_path,
                      string &error_message, int64_t &rows_loaded) {
    rows_loaded = 0;

    // Build bcp command using format file for correct \n row terminator
    // -C 65001: interpret data file as UTF-8 (DuckDB COPY TO outputs UTF-8)
    // bcp <full_table_name> in <file> -S <server> -f <fmt> -C 65001 -k -b 5000
    string cmd = "bcp " + full_table_name + " in \"" + csv_path + "\"" +
                 " -S " + info.server +
                 " -f \"" + fmt_path + "\" -C 65001 -k -b 5000";

    if (info.trusted) {
        cmd += " -T";
    } else {
        cmd += " -U " + info.user + " -P " + info.password;
    }

    // Redirect stderr to stdout to capture all output
    cmd += " 2>&1";

    // Execute and capture output
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
        // Truncate output for error message
        if (output.size() > 500) output = output.substr(0, 500) + "...";
        error_message = "bcp failed (exit " + std::to_string(exit_code) + "): " + output;
        return false;
    }

    // Parse "N rows copied" from bcp output
    auto pos = output.find("rows copied");
    if (pos != string::npos) {
        // Walk backwards from "rows copied" to find the number
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
