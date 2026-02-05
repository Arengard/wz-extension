#include "wz_extension.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

// ============================================================================
// SQL query to get foreign key constraints for a table
// ============================================================================

static const char *FK_QUERY_TEMPLATE = R"(
SELECT
    fk.name AS constraint_name,
    pc.name AS column_name,
    rt.name AS referenced_table,
    rc.name AS referenced_column
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc
    ON fk.object_id = fkc.constraint_object_id
JOIN sys.columns pc
    ON pc.object_id = fkc.parent_object_id
   AND pc.column_id = fkc.parent_column_id
JOIN sys.tables t
    ON t.object_id = fk.parent_object_id
JOIN sys.tables rt
    ON rt.object_id = fk.referenced_object_id
JOIN sys.columns rc
    ON rc.object_id = fkc.referenced_object_id
   AND rc.column_id = fkc.referenced_column_id
WHERE t.name = '%s'
)";

// ============================================================================
// Get foreign key constraints for a table
// ============================================================================

vector<ForeignKeyConstraint> GetForeignKeyConstraints(ClientContext &context,
                                                       const string &secret_name,
                                                       const string &table_name) {
    vector<ForeignKeyConstraint> constraints;

    // Build the query
    char query_buffer[2048];
    snprintf(query_buffer, sizeof(query_buffer), FK_QUERY_TEMPLATE, table_name.c_str());

    // Execute via mssql_scan
    string full_query = "SELECT * FROM mssql_scan('" + secret_name + "', $$" + string(query_buffer) + "$$)";

    auto result = context.Query(full_query, false);
    if (result->HasError()) {
        // Return empty - caller should handle
        return constraints;
    }

    // Process results
    auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
    for (auto &chunk : materialized->Collection().Chunks()) {
        for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
            ForeignKeyConstraint fk;
            fk.constraint_name = chunk.data[0].GetValue(row_idx).ToString();
            fk.column_name = chunk.data[1].GetValue(row_idx).ToString();
            fk.referenced_table = chunk.data[2].GetValue(row_idx).ToString();
            fk.referenced_column = chunk.data[3].GetValue(row_idx).ToString();
            constraints.push_back(fk);
        }
    }

    return constraints;
}

// ============================================================================
// Check for duplicate Primanota IDs
// ============================================================================

bool CheckDuplicatePrimanotaIds(ClientContext &context,
                                const string &secret_name,
                                const vector<string> &primanota_ids,
                                vector<string> &existing_ids) {
    if (primanota_ids.empty()) {
        return false;
    }

    // Build IN clause
    string in_clause;
    for (size_t i = 0; i < primanota_ids.size(); i++) {
        if (i > 0) {
            in_clause += ", ";
        }
        in_clause += "'" + primanota_ids[i] + "'";
    }

    string query = "SELECT guiPrimanotaID FROM tblPrimanota WHERE guiPrimanotaID IN (" + in_clause + ")";
    string full_query = "SELECT * FROM mssql_scan('" + secret_name + "', $$" + query + "$$)";

    auto result = context.Query(full_query, false);
    if (result->HasError()) {
        return false;
    }

    auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
    for (auto &chunk : materialized->Collection().Chunks()) {
        for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
            existing_ids.push_back(chunk.data[0].GetValue(row_idx).ToString());
        }
    }

    return !existing_ids.empty();
}

// ============================================================================
// Validate constraints before insert
// ============================================================================

vector<ConstraintViolation> ValidateConstraints(ClientContext &context,
                                                 const string &secret_name,
                                                 const string &table_name,
                                                 DataChunk &data) {
    vector<ConstraintViolation> violations;

    // Get FK constraints for this table
    auto constraints = GetForeignKeyConstraints(context, secret_name, table_name);

    // For each constraint, check if the referenced values exist
    for (auto &fk : constraints) {
        // Find the column in the data chunk
        // This is a simplified implementation - real code would need column name mapping

        ConstraintViolation violation;
        violation.constraint_name = fk.constraint_name;
        violation.table_name = table_name;
        violation.column_name = fk.column_name;
        violation.referenced_table = fk.referenced_table;
        violation.referenced_column = fk.referenced_column;

        // TODO: Actually validate the values against the referenced table
        // For now, we assume validation passes
    }

    return violations;
}

} // namespace duckdb
