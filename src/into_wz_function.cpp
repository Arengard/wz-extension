#include "wz_extension.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_context.hpp"
#include "yyjson.hpp"
#include <chrono>

namespace duckdb {

// ============================================================================
// Bind data for into_wz function
// ============================================================================

struct IntoWzBindData : public TableFunctionData {
    string database;
    string gui_verfahren_id;
    int64_t lng_kanzlei_konten_rahmen_id;
    string str_angelegt;
    bool generate_vorlauf_id;

    // Results to return
    vector<InsertResult> results;
    idx_t current_result_idx;
    bool executed;
};

// ============================================================================
// Global state for into_wz function
// ============================================================================

struct IntoWzGlobalState : public GlobalTableFunctionState {
    idx_t current_idx;

    IntoWzGlobalState() : current_idx(0) {}
};

// ============================================================================
// Helper: Execute MSSQL query via mssql_scan
// ============================================================================

static unique_ptr<QueryResult> ExecuteMssqlQuery(ClientContext &context,
                                                  const string &secret_name,
                                                  const string &sql) {
    string query = "SELECT * FROM mssql_scan('" + secret_name + "', $$" + sql + "$$)";
    return context.Query(query, false);
}

// ============================================================================
// Helper: Execute MSSQL statement via mssql_exec
// ============================================================================

static unique_ptr<QueryResult> ExecuteMssqlStatement(ClientContext &context,
                                                      const string &secret_name,
                                                      const string &sql) {
    string query = "CALL mssql_exec('" + secret_name + "', $$" + sql + "$$)";
    return context.Query(query, false);
}

// ============================================================================
// Helper: Generate UUID v4
// ============================================================================

static string GenerateUUID() {
    return UUID::ToString(UUID::GenerateRandomUUID());
}

// ============================================================================
// Helper: Get current timestamp
// ============================================================================

static string GetCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;

    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&time));

    char result[64];
    snprintf(result, sizeof(result), "%s.%03d", buffer, static_cast<int>(ms.count()));
    return string(result);
}

// ============================================================================
// Helper: Find min/max dates in data
// ============================================================================

static pair<string, string> FindDateRange(DataChunk &chunk, idx_t date_col_idx) {
    string min_date, max_date;

    auto &date_vec = chunk.data[date_col_idx];
    for (idx_t i = 0; i < chunk.size(); i++) {
        auto val = date_vec.GetValue(i);
        if (!val.IsNull()) {
            string date_str = val.ToString();
            if (min_date.empty() || date_str < min_date) {
                min_date = date_str;
            }
            if (max_date.empty() || date_str > max_date) {
                max_date = date_str;
            }
        }
    }

    return {min_date, max_date};
}

// ============================================================================
// Helper: Escape SQL string
// ============================================================================

static string EscapeSqlString(const string &str) {
    string result;
    for (char c : str) {
        if (c == '\'') {
            result += "''";
        } else {
            result += c;
        }
    }
    return result;
}

// ============================================================================
// Helper: Format value for SQL
// ============================================================================

static string FormatSqlValue(const Value &val) {
    if (val.IsNull()) {
        return "NULL";
    }

    auto type = val.type().id();
    switch (type) {
        case LogicalTypeId::VARCHAR:
            return "'" + EscapeSqlString(val.ToString()) + "'";
        case LogicalTypeId::BOOLEAN:
            return val.GetValue<bool>() ? "1" : "0";
        case LogicalTypeId::DATE:
        case LogicalTypeId::TIMESTAMP:
            return "'" + val.ToString() + "'";
        default:
            return val.ToString();
    }
}

// ============================================================================
// Bind function
// ============================================================================

static unique_ptr<FunctionData> IntoWzBind(ClientContext &context,
                                            TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string> &names) {
    auto bind_data = make_uniq<IntoWzBindData>();

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "database") {
            bind_data->database = StringValue::Get(kv.second);
        } else if (kv.first == "gui_verfahren_id") {
            bind_data->gui_verfahren_id = StringValue::Get(kv.second);
        } else if (kv.first == "lng_kanzlei_konten_rahmen_id") {
            bind_data->lng_kanzlei_konten_rahmen_id = kv.second.GetValue<int64_t>();
        } else if (kv.first == "str_angelegt") {
            bind_data->str_angelegt = StringValue::Get(kv.second);
        } else if (kv.first == "generate_vorlauf_id") {
            bind_data->generate_vorlauf_id = kv.second.GetValue<bool>();
        }
    }

    // Set defaults
    if (bind_data->database.empty()) {
        bind_data->database = "finaldatabase";
    }
    if (bind_data->str_angelegt.empty()) {
        bind_data->str_angelegt = "wz_extension";
    }
    bind_data->generate_vorlauf_id = true;
    bind_data->executed = false;
    bind_data->current_result_idx = 0;

    // Define return columns
    names = {"table_name", "rows_inserted", "gui_vorlauf_id", "duration_seconds", "success", "error_message"};
    return_types = {
        LogicalType::VARCHAR,   // table_name
        LogicalType::BIGINT,    // rows_inserted
        LogicalType::VARCHAR,   // gui_vorlauf_id
        LogicalType::DOUBLE,    // duration_seconds
        LogicalType::BOOLEAN,   // success
        LogicalType::VARCHAR    // error_message
    };

    return std::move(bind_data);
}

// ============================================================================
// Init global state
// ============================================================================

static unique_ptr<GlobalTableFunctionState> IntoWzInitGlobal(ClientContext &context,
                                                              TableFunctionInitInput &input) {
    return make_uniq<IntoWzGlobalState>();
}

// ============================================================================
// Main execution function
// ============================================================================

static void IntoWzExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<IntoWzBindData>();
    auto &global_state = data_p.global_state->Cast<IntoWzGlobalState>();

    // If already executed, return results
    if (bind_data.executed) {
        if (global_state.current_idx >= bind_data.results.size()) {
            output.SetCardinality(0);
            return;
        }

        idx_t count = 0;
        while (global_state.current_idx < bind_data.results.size() && count < STANDARD_VECTOR_SIZE) {
            auto &result = bind_data.results[global_state.current_idx];
            output.SetValue(0, count, Value(result.table_name));
            output.SetValue(1, count, Value(result.rows_inserted));
            output.SetValue(2, count, Value(result.gui_vorlauf_id));
            output.SetValue(3, count, Value(result.duration_seconds));
            output.SetValue(4, count, Value(result.success));
            output.SetValue(5, count, Value(result.error_message));
            global_state.current_idx++;
            count++;
        }
        output.SetCardinality(count);
        return;
    }

    // Mark as executed
    bind_data.executed = true;

    // Validate required parameters
    if (bind_data.gui_verfahren_id.empty()) {
        InsertResult error_result;
        error_result.table_name = "ERROR";
        error_result.rows_inserted = 0;
        error_result.gui_vorlauf_id = "";
        error_result.duration_seconds = 0;
        error_result.success = false;
        error_result.error_message = "gui_verfahren_id is required";
        bind_data.results.push_back(error_result);

        output.SetValue(0, 0, Value(error_result.table_name));
        output.SetValue(1, 0, Value(error_result.rows_inserted));
        output.SetValue(2, 0, Value(error_result.gui_vorlauf_id));
        output.SetValue(3, 0, Value(error_result.duration_seconds));
        output.SetValue(4, 0, Value(error_result.success));
        output.SetValue(5, 0, Value(error_result.error_message));
        output.SetCardinality(1);
        global_state.current_idx = 1;
        return;
    }

    // Generate Vorlauf ID
    string vorlauf_id = GenerateUUID();
    string current_timestamp = GetCurrentTimestamp();

    // For now, create placeholder results
    // The actual implementation will query mssql_scan for constraints,
    // build the Vorlauf record, map Primanota records, and execute inserts

    InsertResult vorlauf_result;
    vorlauf_result.table_name = "tblVorlauf";
    vorlauf_result.rows_inserted = 1;
    vorlauf_result.gui_vorlauf_id = vorlauf_id;
    vorlauf_result.duration_seconds = 0.0;
    vorlauf_result.success = true;
    vorlauf_result.error_message = "";
    bind_data.results.push_back(vorlauf_result);

    InsertResult primanota_result;
    primanota_result.table_name = "tblPrimanota";
    primanota_result.rows_inserted = 0;  // Will be populated from actual data
    primanota_result.gui_vorlauf_id = vorlauf_id;
    primanota_result.duration_seconds = 0.0;
    primanota_result.success = true;
    primanota_result.error_message = "";
    bind_data.results.push_back(primanota_result);

    // Return first batch of results
    idx_t count = 0;
    while (global_state.current_idx < bind_data.results.size() && count < STANDARD_VECTOR_SIZE) {
        auto &result = bind_data.results[global_state.current_idx];
        output.SetValue(0, count, Value(result.table_name));
        output.SetValue(1, count, Value(result.rows_inserted));
        output.SetValue(2, count, Value(result.gui_vorlauf_id));
        output.SetValue(3, count, Value(result.duration_seconds));
        output.SetValue(4, count, Value(result.success));
        output.SetValue(5, count, Value(result.error_message));
        global_state.current_idx++;
        count++;
    }
    output.SetCardinality(count);
}

// ============================================================================
// Register the function
// ============================================================================

void RegisterIntoWzFunction(DatabaseInstance &db) {
    TableFunction into_wz_func("into_wz", {}, IntoWzExecute, IntoWzBind, IntoWzInitGlobal);

    // Add named parameters
    into_wz_func.named_parameters["database"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["gui_verfahren_id"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["lng_kanzlei_konten_rahmen_id"] = LogicalType::BIGINT;
    into_wz_func.named_parameters["str_angelegt"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["generate_vorlauf_id"] = LogicalType::BOOLEAN;

    ExtensionUtil::RegisterFunction(db, into_wz_func);
}

} // namespace duckdb
