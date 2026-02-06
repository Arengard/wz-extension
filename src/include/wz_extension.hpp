#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Forward declarations
class WzExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override;
    std::string Version() const override;
};

// ============================================================================
// Constraint information
// ============================================================================

struct ForeignKeyConstraint {
    string constraint_name;
    string column_name;
    string referenced_table;
    string referenced_column;
};

struct ConstraintViolation {
    string constraint_name;
    string table_name;
    string column_name;
    string referenced_table;
    string referenced_column;
    vector<string> violating_values;
    vector<string> violating_row_ids;
};

// ============================================================================
// Insert result
// ============================================================================

struct InsertResult {
    string table_name;
    int64_t rows_inserted;
    string gui_vorlauf_id;
    double duration_seconds;
    bool success;
    string error_message;
};

// ============================================================================
// Function declarations
// ============================================================================

// Constraint checker functions
vector<ForeignKeyConstraint> GetForeignKeyConstraints(ClientContext &context,
                                                       const string &secret_name,
                                                       const string &table_name);

bool ValidateForeignKeys(ClientContext &context,
                          const string &db_name,
                          const vector<vector<Value>> &source_rows,
                          const vector<string> &source_columns,
                          string &error_message);

// Table function registration
void RegisterIntoWzFunction(DatabaseInstance &db);

} // namespace duckdb
