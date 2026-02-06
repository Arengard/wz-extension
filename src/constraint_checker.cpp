#include "wz_extension.hpp"
#include "wz_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include <set>

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

    // Build the query using std::string (no snprintf buffer overflow risk)
    string fk_query = string(FK_QUERY_TEMPLATE);
    size_t pos = fk_query.find("%s");
    if (pos != string::npos) {
        fk_query.replace(pos, 2, table_name);
    }

    // Execute via mssql_scan
    string full_query = "SELECT * FROM mssql_scan('" + secret_name + "', $$" + fk_query + "$$)";

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
// Helper: Escape single quotes for SQL strings
// ============================================================================

static string EscapeSqlString(const string &str) {
    string result;
    result.reserve(str.size() * 2);
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
// Helper: Map MSSQL FK column name to source column index
// Uses the same fallback aliases as BuildPrimanotaInsertSQL
// Returns DConstants::INVALID_INDEX if no alias matches (skip this FK)
// ============================================================================

static idx_t FindSourceColumnForFK(const string &fk_column_name,
                                    const vector<string> &source_columns) {
    // Convert FK column name to lowercase for comparison
    string fk_lower = fk_column_name;
    std::transform(fk_lower.begin(), fk_lower.end(), fk_lower.begin(), ::tolower);

    if (fk_lower == "deckontonr") {
        // decKontoNr aliases (same as BuildPrimanotaInsertSQL)
        const char *aliases[] = {"decKontoNr", "konto", "kontonr", "konto_nr", "account"};
        for (const char *alias : aliases) {
            idx_t idx = FindColumnIndex(source_columns, alias);
            if (idx != DConstants::INVALID_INDEX) {
                return idx;
            }
        }
    } else if (fk_lower == "decgegenkontonr") {
        // decGegenkontoNr aliases
        const char *aliases[] = {"decGegenkontoNr", "gegenkonto", "gegenkontonr", "gegenkonto_nr", "counter_account"};
        for (const char *alias : aliases) {
            idx_t idx = FindColumnIndex(source_columns, alias);
            if (idx != DConstants::INVALID_INDEX) {
                return idx;
            }
        }
    } else if (fk_lower == "deceakontonr") {
        // decEaKontoNr aliases
        const char *aliases[] = {"decEaKontoNr", "eakonto", "ea_konto", "eakontonr", "ea_konto_nr"};
        for (const char *alias : aliases) {
            idx_t idx = FindColumnIndex(source_columns, alias);
            if (idx != DConstants::INVALID_INDEX) {
                return idx;
            }
        }
    } else {
        // For any other FK column, try direct name match
        return FindColumnIndex(source_columns, fk_column_name);
    }

    return DConstants::INVALID_INDEX;
}

// ============================================================================
// Helper: Check which values from a set exist in a referenced MSSQL table
// Populates missing_values with values not found in the referenced table
// ============================================================================

static void CheckValueExistence(ClientContext &context,
                                 const string &db_name,
                                 const ForeignKeyConstraint &fk,
                                 const std::set<string> &distinct_values,
                                 vector<string> &missing_values) {
    // Batch in groups of 100 (same pattern as CheckDuplicates)
    vector<string> all_values(distinct_values.begin(), distinct_values.end());

    for (size_t batch_start = 0; batch_start < all_values.size(); batch_start += 100) {
        size_t batch_end = std::min(batch_start + 100, all_values.size());

        // Build IN clause
        string in_clause;
        for (size_t i = batch_start; i < batch_end; i++) {
            if (i > batch_start) {
                in_clause += ", ";
            }
            in_clause += "'" + EscapeSqlString(all_values[i]) + "'";
        }

        // Query referenced table using CAST AS VARCHAR on both sides to avoid type mismatch
        string query = "SELECT DISTINCT CAST(" + fk.referenced_column + " AS VARCHAR) AS val FROM " +
                        db_name + ".dbo." + fk.referenced_table +
                        " WHERE CAST(" + fk.referenced_column + " AS VARCHAR) IN (" + in_clause + ")";

        auto result = context.Query(query, false);
        if (result->HasError()) {
            // Skip this constraint silently if query fails (e.g., referenced table doesn't exist)
            return;
        }

        // Collect returned values
        std::set<string> found_values;
        auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
        for (auto &chunk : materialized->Collection().Chunks()) {
            for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
                found_values.insert(chunk.data[0].GetValue(row_idx).ToString());
            }
        }

        // Find missing values in this batch
        for (size_t i = batch_start; i < batch_end; i++) {
            if (found_values.find(all_values[i]) == found_values.end()) {
                missing_values.push_back(all_values[i]);
            }
        }
    }
}

// ============================================================================
// Validate foreign key constraints against MSSQL reference tables
// Returns true if validation passes (or no FK constraints found)
// Returns false if violations detected (with error_message set)
// ============================================================================

bool ValidateForeignKeys(ClientContext &context,
                          const string &db_name,
                          const vector<vector<Value>> &source_rows,
                          const vector<string> &source_columns,
                          string &error_message) {
    // Discover FK constraints for tblPrimanota
    auto constraints = GetForeignKeyConstraints(context, db_name, "tblPrimanota");
    if (constraints.empty()) {
        return true;  // No constraints to validate, or metadata query failed
    }

    vector<string> violation_messages;

    for (auto &fk : constraints) {
        // Find the source column index using alias mapping
        idx_t source_col_idx = FindSourceColumnForFK(fk.column_name, source_columns);
        if (source_col_idx == DConstants::INVALID_INDEX) {
            // Source doesn't have this column -- skip silently
            continue;
        }

        // Collect distinct non-null values from source data
        std::set<string> distinct_values;
        for (const auto &row : source_rows) {
            if (source_col_idx < row.size() && !row[source_col_idx].IsNull()) {
                string val = row[source_col_idx].ToString();
                if (!val.empty()) {
                    distinct_values.insert(val);
                }
            }
        }

        if (distinct_values.empty()) {
            // No values to check (all NULL or empty) -- skip
            continue;
        }

        // Check which values exist in the referenced table
        vector<string> missing_values;
        CheckValueExistence(context, db_name, fk, distinct_values, missing_values);

        if (!missing_values.empty()) {
            // Format violation message: show up to 10 values
            string values_str;
            size_t show_count = std::min(missing_values.size(), size_t(10));
            for (size_t i = 0; i < show_count; i++) {
                if (i > 0) {
                    values_str += ", ";
                }
                values_str += missing_values[i];
            }
            if (missing_values.size() > 10) {
                values_str += " (and " + std::to_string(missing_values.size() - 10) + " more)";
            }

            violation_messages.push_back(
                fk.column_name + ": values [" + values_str + "] not found in " +
                fk.referenced_table + "." + fk.referenced_column
            );
        }
    }

    if (!violation_messages.empty()) {
        error_message = "Foreign key validation failed:\n";
        for (const auto &msg : violation_messages) {
            error_message += "  - " + msg + "\n";
        }
        error_message += "0 rows written.";
        return false;
    }

    return true;
}

} // namespace duckdb
