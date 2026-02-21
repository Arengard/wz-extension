#include "wz_extension.hpp"
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

struct DropAllResult {
    string object_type;
    string object_name;
    string status;
};

struct StpsDropAllBindData : public TableFunctionData {
    vector<DropAllResult> results;
    bool executed;
    StpsDropAllBindData() : executed(false) {}
};

struct StpsDropAllGlobalState : public GlobalTableFunctionState {
    idx_t current_result_idx;
    StpsDropAllGlobalState() : current_result_idx(0) {}
};

// ============================================================================
// Bind
// ============================================================================

static unique_ptr<FunctionData> StpsDropAllBind(ClientContext &context,
                                                 TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types,
                                                 vector<string> &names) {
    names = {"object_type", "object_name", "status"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
    return make_uniq<StpsDropAllBindData>();
}

// ============================================================================
// Init
// ============================================================================

static unique_ptr<GlobalTableFunctionState> StpsDropAllInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    return make_uniq<StpsDropAllGlobalState>();
}

// ============================================================================
// Execute
// ============================================================================

static void StpsDropAllExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<StpsDropAllBindData>();
    auto &state = data_p.global_state->Cast<StpsDropAllGlobalState>();

    if (!bind_data.executed) {
        bind_data.executed = true;

        auto &db = DatabaseInstance::GetDatabase(context);
        Connection conn(db);

        // Step 1: Drop all views
        {
            auto result = conn.Query(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_type = 'VIEW' "
                "AND table_schema NOT IN ('information_schema', 'pg_catalog')");
            if (!result->HasError()) {
                for (auto &chunk : result->Collection().Chunks()) {
                    for (idx_t row = 0; row < chunk.size(); row++) {
                        string schema_name = chunk.data[0].GetValue(row).ToString();
                        string view_name = chunk.data[1].GetValue(row).ToString();
                        string qualified = "\"" + schema_name + "\".\"" + view_name + "\"";
                        string drop_sql = "DROP VIEW IF EXISTS " + qualified;

                        DropAllResult r;
                        r.object_type = "VIEW";
                        r.object_name = qualified;

                        auto drop_result = conn.Query(drop_sql);
                        r.status = drop_result->HasError() ? "ERROR: " + drop_result->GetError() : "DROPPED";
                        bind_data.results.push_back(std::move(r));
                    }
                }
            }
        }

        // Step 2: Drop all tables
        {
            auto result = conn.Query(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE' "
                "AND table_schema NOT IN ('information_schema', 'pg_catalog')");
            if (!result->HasError()) {
                for (auto &chunk : result->Collection().Chunks()) {
                    for (idx_t row = 0; row < chunk.size(); row++) {
                        string schema_name = chunk.data[0].GetValue(row).ToString();
                        string table_name = chunk.data[1].GetValue(row).ToString();
                        string qualified = "\"" + schema_name + "\".\"" + table_name + "\"";
                        string drop_sql = "DROP TABLE IF EXISTS " + qualified + " CASCADE";

                        DropAllResult r;
                        r.object_type = "TABLE";
                        r.object_name = qualified;

                        auto drop_result = conn.Query(drop_sql);
                        r.status = drop_result->HasError() ? "ERROR: " + drop_result->GetError() : "DROPPED";
                        bind_data.results.push_back(std::move(r));
                    }
                }
            }
        }

        // Step 3: Drop all non-default schemas
        {
            auto result = conn.Query(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT IN ('main', 'information_schema', 'pg_catalog')");
            if (!result->HasError()) {
                for (auto &chunk : result->Collection().Chunks()) {
                    for (idx_t row = 0; row < chunk.size(); row++) {
                        string schema_name = chunk.data[0].GetValue(row).ToString();
                        string drop_sql = "DROP SCHEMA IF EXISTS \"" + schema_name + "\" CASCADE";

                        DropAllResult r;
                        r.object_type = "SCHEMA";
                        r.object_name = schema_name;

                        auto drop_result = conn.Query(drop_sql);
                        r.status = drop_result->HasError() ? "ERROR: " + drop_result->GetError() : "DROPPED";
                        bind_data.results.push_back(std::move(r));
                    }
                }
            }
        }

        // Step 4: Detach all non-default databases
        {
            auto result = conn.Query(
                "SELECT database_name FROM duckdb_databases() "
                "WHERE internal = false AND NOT is_default");
            if (!result->HasError()) {
                for (auto &chunk : result->Collection().Chunks()) {
                    for (idx_t row = 0; row < chunk.size(); row++) {
                        string db_name = chunk.data[0].GetValue(row).ToString();
                        string detach_sql = "DETACH \"" + db_name + "\"";

                        DropAllResult r;
                        r.object_type = "DATABASE";
                        r.object_name = db_name;

                        auto detach_result = conn.Query(detach_sql);
                        r.status = detach_result->HasError() ? "ERROR: " + detach_result->GetError() : "DETACHED";
                        bind_data.results.push_back(std::move(r));
                    }
                }
            }
        }

        // If nothing was found, add an informational row
        if (bind_data.results.empty()) {
            DropAllResult r;
            r.object_type = "INFO";
            r.object_name = "";
            r.status = "Nothing to drop — database is already clean";
            bind_data.results.push_back(std::move(r));
        }
    }

    // Output results
    idx_t count = 0;
    while (state.current_result_idx < bind_data.results.size() && count < STANDARD_VECTOR_SIZE) {
        auto &r = bind_data.results[state.current_result_idx];
        output.data[0].SetValue(count, Value(r.object_type));
        output.data[1].SetValue(count, Value(r.object_name));
        output.data[2].SetValue(count, Value(r.status));
        count++;
        state.current_result_idx++;
    }
    output.SetCardinality(count);
}

// ============================================================================
// Register
// ============================================================================

void RegisterStpsDropAllFunction(DatabaseInstance &db) {
    TableFunction func("stps_drop_all", {}, StpsDropAllExecute, StpsDropAllBind, StpsDropAllInitGlobal);

    Connection con(db);
    con.BeginTransaction();
    auto &catalog = Catalog::GetSystemCatalog(db);
    CreateTableFunctionInfo info(func);
    catalog.CreateFunction(*con.context, info);
    con.Commit();
}

} // namespace duckdb
