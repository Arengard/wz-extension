# Phase 3: Constraint Validation - Research

**Researched:** 2026-02-06
**Domain:** Pre-insert FK validation for MSSQL tblPrimanota via DuckDB C++ extension, MSSQL system catalog queries, batch value existence checking
**Confidence:** HIGH

## Summary

Phase 3 completes the partially-implemented constraint validation layer in `constraint_checker.cpp`. The existing code already has the infrastructure: a `GetForeignKeyConstraints()` function that queries MSSQL `sys.foreign_keys` to discover FK metadata, `ForeignKeyConstraint` and `ConstraintViolation` structs in the header, and a `ValidateConstraints()` stub. The work is to complete the validation logic and wire it into `IntoWzExecute()` before any inserts execute.

The core challenge is efficiently checking whether source column values exist in their referenced MSSQL tables. The source data is already fully materialized in `bind_data.source_rows` (from `LoadSourceData()`), so we know all values upfront. The approach is: (1) discover FK constraints on `tblPrimanota` via MSSQL metadata, (2) for each FK, extract the distinct source values for that column, (3) query the referenced MSSQL table to find which values do NOT exist, (4) if any violations found, fail early with a clear error listing column names and missing values.

**Primary recommendation:** Complete the existing `ValidateConstraints()` function in `constraint_checker.cpp` to perform actual value-existence checks against MSSQL referenced tables, using the same `mssql_scan` pattern already used by `GetForeignKeyConstraints()`. Wire validation into `IntoWzExecute()` as a new step between duplicate checking (Step 3) and Vorlauf preparation (Step 4). Use `NOT IN` or `LEFT JOIN ... WHERE NULL` SQL patterns to identify non-existent values efficiently.

## Standard Stack

This phase does not introduce new libraries. It completes existing code using the same DuckDB C++ API and MSSQL extension already in use.

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| DuckDB C++ API | v1.4.4 | `Connection`, `Query`, `MaterializedQueryResult` for executing validation queries | Already in use throughout the extension |
| hugr-lab/mssql-extension | latest | Provides `mssql_scan()` for querying MSSQL system catalogs and reference tables | Already in use by `GetForeignKeyConstraints()` |
| MSSQL System Views | N/A | `sys.foreign_keys`, `sys.foreign_key_columns`, `sys.columns`, `sys.tables` for FK metadata discovery | Standard SQL Server metadata interface, already queried by existing code |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Dynamic FK discovery from `sys.foreign_keys` | Hard-coded FK definitions (e.g., "decKontoNr must exist in tblKonto") | Hard-coding is simpler but breaks if the MSSQL schema changes; dynamic discovery adapts automatically |
| `NOT IN` subquery to find missing values | `LEFT JOIN ... WHERE IS NULL` pattern | Both work; `NOT IN` is simpler for the DuckDB-to-MSSQL bridge pattern (`mssql_scan`) and easier to construct programmatically |
| Per-constraint separate queries | Single query with multiple JOINs | Per-constraint queries are simpler, easier to generate error messages per-column, and avoid complex multi-table JOIN construction; the number of FK constraints on tblPrimanota is small (likely 2-5), so per-constraint overhead is negligible |

## Architecture Patterns

### Pattern 1: Value-Existence Check via NOT IN Query
**What:** For each FK constraint, extract distinct values from the source data for that column, then query the referenced MSSQL table to see which values do NOT exist.
**When to use:** When the set of source values is bounded (thousands, not millions) and the referenced table has an index on the referenced column.
**Why this approach:** The source data is already materialized in memory (`bind_data.source_rows`). We can extract distinct values, build an IN clause, and check existence efficiently.

```cpp
// Source: Pattern derived from existing CheckDuplicates() in into_wz_function.cpp
// For a FK constraint: decKontoNr -> tblKonto.decKontoNr
// Query: "SELECT CAST(val AS VARCHAR) FROM (VALUES (...)) AS v(val)
//         WHERE val NOT IN (SELECT decKontoNr FROM tblKonto)"
// Or more reliably via mssql_scan:

// Step 1: Collect distinct non-null values for the FK column from source data
std::set<string> distinct_values;
for (const auto &row : source_rows) {
    if (fk_col_idx < row.size() && !row[fk_col_idx].IsNull()) {
        distinct_values.insert(row[fk_col_idx].ToString());
    }
}

// Step 2: Query MSSQL to check which values exist in the referenced table
// Build: SELECT DISTINCT decKontoNr FROM tblKonto WHERE decKontoNr IN (val1, val2, ...)
string in_clause = /* build from distinct_values */;
string query = "SELECT DISTINCT " + referenced_column +
    " FROM " + referenced_table +
    " WHERE " + referenced_column + " IN (" + in_clause + ")";

// Step 3: Compare returned values against source values -- the difference is violations
```

### Pattern 2: Validation Step in IntoWzExecute Pipeline
**What:** Insert FK validation as a new step between duplicate checking and Vorlauf preparation in the existing 7-step pipeline.
**When to use:** Always -- this is the integration point.

```
Current pipeline:
  Step 1: Validate parameters
  Step 2: Load source data
  Step 3: Check duplicate Primanota IDs
  Step 4: Prepare Vorlauf data
  Steps 5-7: Transaction (insert Vorlauf, insert Primanota, commit)

New pipeline:
  Step 1: Validate parameters
  Step 2: Load source data
  Step 3: Check duplicate Primanota IDs
  Step 3.5 (NEW): Validate FK constraints  <-- NEW
  Step 4: Prepare Vorlauf data
  Steps 5-7: Transaction (insert Vorlauf, insert Primanota, commit)
```

### Pattern 3: Column Name Mapping Awareness
**What:** The FK constraint from MSSQL uses the target column name (e.g., `decKontoNr`), but the source data may use alternative names (e.g., `konto`, `kontonr`). The validation must use the same column-mapping logic as the insert.
**When to use:** Always -- source columns have flexible naming.

```cpp
// The existing findColumnWithFallbacks lambda in BuildPrimanotaInsertSQL
// maps source column names to target names.
// FK validation must use the SAME mapping.
// e.g., FK says "decKontoNr must exist in tblKonto.decKontoNr"
//   -> look for source column: decKontoNr, konto, kontonr, konto_nr, account
//   -> validate those values against tblKonto.decKontoNr
```

### Pattern 4: Error Message Format for Violations
**What:** When FK violations are found, produce a clear error listing every violating column and the specific values that don't exist.
**When to use:** On validation failure.

```
Format:
"Foreign key validation failed. The following values do not exist in their referenced tables:
  - decKontoNr: values [1234, 5678] not found in tblKonto.decKontoNr
  - decGegenkontoNr: values [9999] not found in tblKonto.decKontoNr
0 rows written."
```

### Anti-Patterns to Avoid
- **Validating inside the transaction:** FK validation must happen BEFORE `BeginTransaction()`. If validation queries are run on the transaction connection, they become part of the transaction and complicate rollback. Use a separate connection for validation (matching the existing pattern where `LoadSourceData()` and `ValidateDuplicates()` use separate connections).
- **Validating against the attached database name instead of the actual table name:** The FK metadata gives us `referenced_table` (e.g., `tblKonto`), but queries go through the attached database. Use `db_name.dbo.referenced_table` syntax consistently.
- **Not handling the column name mapping:** If the FK constraint says `decKontoNr` but the source column is `konto`, the validation must find the source column via the same fallback chain used by `BuildPrimanotaInsertSQL`.
- **Querying per-row instead of per-column-batch:** Never check one row at a time. Collect all distinct values for a column and check them in one query.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| FK constraint discovery | Hard-coded list of FK columns | `GetForeignKeyConstraints()` querying `sys.foreign_keys` | Already implemented; adapts to schema changes automatically |
| Value existence checking | Row-by-row validation | Batch `IN` clause query against referenced table | Performance: one query per FK constraint vs. one query per row |
| Column name resolution | New mapping logic | Reuse or extract the `findColumnWithFallbacks` pattern from `BuildPrimanotaInsertSQL` | Ensures validation checks the same columns that will actually be inserted |
| Error message formatting | Ad-hoc string building | Structured `ConstraintViolation` accumulation then formatted output | `ConstraintViolation` struct already exists in `wz_extension.hpp` with `violating_values` vector |

**Key insight:** Most of the infrastructure already exists. The `ForeignKeyConstraint` struct, `ConstraintViolation` struct, `GetForeignKeyConstraints()` function, and `ValidateConstraints()` stub are all in place. The work is completing the stub and wiring it into the pipeline.

## Common Pitfalls

### Pitfall 1: Source Column Not Found for FK Constraint
**What goes wrong:** MSSQL reports a FK constraint on column `decKontoNr`, but the source data has no column matching that name (or any alias).
**Why it happens:** Source data might only have some FK columns, or use different names not in the fallback list.
**How to avoid:** If the FK column has no corresponding source column, skip validation for that constraint. The source data simply doesn't provide that column, so NULL will be inserted -- which either violates a NOT NULL constraint (caught at insert time) or is acceptable.
**Warning signs:** Validation passes but INSERT fails due to NULL in FK column.

### Pitfall 2: Data Type Mismatch in IN Clause
**What goes wrong:** The source value is a numeric type (e.g., `DECIMAL 1234.00`) but the FK query compares it as a string, missing the match due to format differences (e.g., `'1234.00'` vs `'1234'`).
**Why it happens:** `Value::ToString()` formats numbers differently than MSSQL's native representation.
**How to avoid:** Use `CAST` in the validation query to ensure type compatibility. For numeric FK columns like `decKontoNr`, cast both sides to the same type: `WHERE CAST(decKontoNr AS DECIMAL) IN (1234, 5678)`.
**Warning signs:** Validation reports false violations -- values exist in the referenced table but aren't matched.

### Pitfall 3: Large Number of Distinct Values Exceeding IN Clause Limits
**What goes wrong:** Source data has thousands of distinct values for a FK column, and the IN clause becomes too large for MSSQL to handle.
**Why it happens:** MSSQL has no hard limit on IN clause size, but very large queries can hit network/TDS packet size limits or slow down parsing.
**How to avoid:** Batch the IN clause checks (same pattern as existing `CheckDuplicates` which batches in groups of 100). For very large sets, consider using a temp table approach.
**Warning signs:** Query timeout or "query too complex" errors during validation.

### Pitfall 4: Validating Against Wrong Table via Dynamic Discovery
**What goes wrong:** `sys.foreign_keys` returns FK constraints that reference internal system tables or tables irrelevant to the insert operation, causing unnecessary validation and confusing error messages.
**Why it happens:** `GetForeignKeyConstraints()` queries ALL FK constraints on `tblPrimanota`. Some might reference tables the user doesn't care about.
**How to avoid:** Only validate FK constraints where the source data actually provides a value for the FK column. If a FK column isn't in the source data, skip it silently.
**Warning signs:** Validation failing on FK columns the user never specified.

### Pitfall 5: NULL Values Causing False Violations
**What goes wrong:** NULL values in the FK column are treated as "value not found in referenced table" and flagged as violations.
**Why it happens:** NULLs are collected and included in the IN clause, or compared against the referenced table.
**How to avoid:** Always filter out NULL values before building the IN clause. NULL FK values typically mean "no reference" which is acceptable unless the column has a NOT NULL constraint (a separate concern).
**Warning signs:** Error message lists NULL as a violating value.

### Pitfall 6: mssql_scan vs Attached Database Syntax
**What goes wrong:** Validation queries use `mssql_scan(secret_name, $$query$$)` but insert queries use `db_name.dbo.table` (attached database syntax). The two approaches have different semantics.
**Why it happens:** The existing `GetForeignKeyConstraints()` uses `mssql_scan` while the existing `CheckDuplicates()` uses attached database syntax. This inconsistency could cause issues.
**How to avoid:** Use a consistent approach. The existing `CheckDuplicates()` in `into_wz_function.cpp` uses attached database syntax (`db_name.dbo.tblPrimanota`), while `constraint_checker.cpp` uses `mssql_scan`. For validation, prefer the attached database syntax since it's the same connection mechanism used for inserts and works within the same transaction framework. However, the current `GetForeignKeyConstraints()` uses `mssql_scan` -- this should be evaluated for consistency.
**Warning signs:** Queries succeeding on one approach but failing on the other due to different connection contexts.

## Code Examples

### Example 1: Completing ValidateConstraints for tblPrimanota
```cpp
// Approach: Validate FK constraints for source data against MSSQL referenced tables
// Uses separate connection (same pattern as CheckDuplicates/LoadSourceData)

static bool ValidateForeignKeys(ClientContext &context,
                                const string &db_name,
                                const vector<vector<Value>> &source_rows,
                                const vector<string> &source_columns,
                                string &error_message) {
    // Step 1: Get FK constraints for tblPrimanota
    auto constraints = GetForeignKeyConstraints(context, db_name, "tblPrimanota");
    if (constraints.empty()) {
        return true;  // No FK constraints to validate (or query failed)
    }

    // Step 2: Create connection for validation queries
    auto &db = DatabaseInstance::GetDatabase(context);
    Connection conn(db);

    vector<string> violation_messages;

    // Step 3: For each FK constraint, check values
    for (const auto &fk : constraints) {
        // Find the source column that maps to this FK column
        idx_t source_col_idx = FindSourceColumnForFK(source_columns, fk.column_name);
        if (source_col_idx == DConstants::INVALID_INDEX) {
            continue;  // Source doesn't provide this FK column, skip
        }

        // Collect distinct non-null values
        std::set<string> distinct_values;
        for (const auto &row : source_rows) {
            if (source_col_idx < row.size() && !row[source_col_idx].IsNull()) {
                distinct_values.insert(row[source_col_idx].ToString());
            }
        }
        if (distinct_values.empty()) {
            continue;  // All NULLs for this column, skip
        }

        // Check existence in batches
        vector<string> missing_values;
        CheckValueExistence(conn, db_name, fk, distinct_values, missing_values);

        if (!missing_values.empty()) {
            string msg = fk.column_name + ": values [";
            for (size_t i = 0; i < std::min(missing_values.size(), size_t(10)); i++) {
                if (i > 0) msg += ", ";
                msg += missing_values[i];
            }
            if (missing_values.size() > 10) {
                msg += " (and " + std::to_string(missing_values.size() - 10) + " more)";
            }
            msg += "] not found in " + fk.referenced_table + "." + fk.referenced_column;
            violation_messages.push_back(msg);
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
```

### Example 2: Finding Source Column for FK Column (with mapping awareness)
```cpp
// Reuse the same column-name fallback logic as BuildPrimanotaInsertSQL
// Map MSSQL FK column names to their source aliases

static idx_t FindSourceColumnForFK(const vector<string> &source_columns,
                                    const string &fk_column_name) {
    // Direct match first
    idx_t idx = FindColumnIndex(source_columns, fk_column_name);
    if (idx != DConstants::INVALID_INDEX) return idx;

    // Try known aliases (same as BuildPrimanotaInsertSQL fallbacks)
    static const std::unordered_map<string, vector<string>> FK_COLUMN_ALIASES = {
        {"decKontoNr", {"konto", "kontonr", "konto_nr", "account"}},
        {"decGegenkontoNr", {"gegenkonto", "gegenkontonr", "gegenkonto_nr", "counter_account"}},
        {"decEaKontoNr", {"eakonto", "ea_konto", "eakontonr", "ea_konto_nr"}},
    };

    string lower_fk = fk_column_name;
    std::transform(lower_fk.begin(), lower_fk.end(), lower_fk.begin(), ::tolower);

    auto it = FK_COLUMN_ALIASES.find(fk_column_name);
    if (it != FK_COLUMN_ALIASES.end()) {
        for (const auto &alias : it->second) {
            idx = FindColumnIndex(source_columns, alias);
            if (idx != DConstants::INVALID_INDEX) return idx;
        }
    }

    return DConstants::INVALID_INDEX;
}
```

### Example 3: Batch Value Existence Check
```cpp
// Check which values from distinct_values exist in referenced_table.referenced_column
// Uses attached database syntax for consistency with insert queries

static void CheckValueExistence(Connection &conn,
                                 const string &db_name,
                                 const ForeignKeyConstraint &fk,
                                 const std::set<string> &distinct_values,
                                 vector<string> &missing_values) {
    const size_t BATCH_SIZE = 100;
    vector<string> values_vec(distinct_values.begin(), distinct_values.end());

    for (size_t batch_start = 0; batch_start < values_vec.size(); batch_start += BATCH_SIZE) {
        size_t batch_end = std::min(batch_start + BATCH_SIZE, values_vec.size());

        // Build IN clause
        string in_clause;
        for (size_t i = batch_start; i < batch_end; i++) {
            if (i > batch_start) in_clause += ", ";
            in_clause += "'" + EscapeSqlString(values_vec[i]) + "'";
        }

        // Query: find which of our values exist
        string query = "SELECT DISTINCT CAST(" + fk.referenced_column +
            " AS VARCHAR) FROM " + db_name + ".dbo." + fk.referenced_table +
            " WHERE CAST(" + fk.referenced_column + " AS VARCHAR) IN (" + in_clause + ")";

        auto result = conn.Query(query);
        if (result->HasError()) {
            // If query fails, skip this constraint (log warning if possible)
            continue;
        }

        // Collect existing values
        std::set<string> existing;
        auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
        for (auto &chunk : materialized->Collection().Chunks()) {
            for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
                existing.insert(chunk.data[0].GetValue(row_idx).ToString());
            }
        }

        // Find missing
        for (size_t i = batch_start; i < batch_end; i++) {
            if (existing.find(values_vec[i]) == existing.end()) {
                missing_values.push_back(values_vec[i]);
            }
        }
    }
}
```

### Example 4: Integration Point in IntoWzExecute
```cpp
// In IntoWzExecute, after Step 3 (duplicate check), before Step 4 (Vorlauf prep):

    // =========================================================================
    // Step 3.5 (NEW): Validate foreign key constraints
    // =========================================================================

    if (!ValidateForeignKeys(context, bind_data.secret_name,
                             bind_data.source_rows, bind_data.source_columns, error_msg)) {
        AddErrorResult(bind_data, "ERROR", error_msg);
        OutputResults(bind_data, global_state, output);
        return;
    }
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No FK validation; rely on MSSQL to reject bad data at INSERT time | Pre-validate FK values before INSERT | This phase | Fails early with clear error instead of mid-transaction MSSQL error that requires rollback |
| `ValidateConstraints()` stub with TODO comment | Complete implementation with actual value-existence checking | This phase | Resolves CST-01 requirement |
| `mssql_scan` for metadata queries | Evaluate whether to switch to attached database syntax for consistency | This phase | Simplifies query construction and aligns with insert query patterns |

## Implementation Analysis

### What Needs to Change

1. **Complete `ValidateConstraints()` in `constraint_checker.cpp`** (or replace with new function):
   - Current: Retrieves FK constraints but never validates values (line 134 TODO)
   - Target: Actually query referenced tables to check value existence
   - The existing function signature takes `DataChunk &data` which doesn't match the current data model (`vector<vector<Value>>` in `bind_data.source_rows`). The signature needs updating.

2. **Update `ValidateConstraints()` signature** in both `constraint_checker.cpp` and `wz_extension.hpp`:
   - Current: `ValidateConstraints(ClientContext&, string&, string&, DataChunk&)`
   - New: Should accept `vector<vector<Value>> &source_rows` and `vector<string> &source_columns` instead of `DataChunk`, matching how data is stored in `IntoWzBindData`

3. **Add column-name mapping for FK columns**:
   - Must map FK column names (e.g., `decKontoNr`) to source column names (e.g., `konto`)
   - Can extract or share the fallback logic from `BuildPrimanotaInsertSQL`

4. **Wire into `IntoWzExecute()`**:
   - Add validation call between Step 3 (duplicate check) and Step 4 (Vorlauf prep)
   - Follow the same early-return error pattern as duplicate checking

5. **Fix `snprintf` buffer overflow risk** in `GetForeignKeyConstraints()`:
   - Current: Uses `char query_buffer[2048]` with `snprintf`
   - Fix: Use `std::string` concatenation (flagged in CONCERNS.md)

### What Should NOT Change

- `ForeignKeyConstraint` struct -- already correct
- `ConstraintViolation` struct -- already has the right fields
- `FK_QUERY_TEMPLATE` SQL -- already correct for discovering FK metadata
- `GetForeignKeyConstraints()` logic -- already working, just needs the buffer fix
- Existing pipeline steps 1-7 in `IntoWzExecute()`
- Transaction handling (Phase 2 work)
- Column mapping in `BuildPrimanotaInsertSQL()`

### What to Remove

- `CheckDuplicatePrimanotaIds()` from `constraint_checker.cpp`: This is dead code. The actual duplicate checking is done by `CheckDuplicates()` in `into_wz_function.cpp`. The constraint_checker version was never wired in. Remove it and its declaration from `wz_extension.hpp`.

### Scope of Change

- **`src/constraint_checker.cpp`**: Major changes -- complete `ValidateConstraints()`, fix buffer overflow, remove dead `CheckDuplicatePrimanotaIds()`
- **`src/include/wz_extension.hpp`**: Minor changes -- update `ValidateConstraints()` signature, remove `CheckDuplicatePrimanotaIds()` declaration
- **`src/into_wz_function.cpp`**: Minor changes -- add validation call (~10 lines) between steps 3 and 4

Estimated: **1 plan, ~100-150 lines changed** across 3 files.

## Open Questions

1. **Should FK validation use `mssql_scan` or attached database syntax?**
   - What we know: `GetForeignKeyConstraints()` uses `mssql_scan(secret_name, $$query$$)` while `CheckDuplicates()` uses attached database syntax (`db_name.dbo.table`). The `bind_data.secret_name` stores the attached database name (e.g., `mssql_conn`), not a secret name per se.
   - What's unclear: Whether `mssql_scan` works with the attached database name or requires the original secret name. The existing code uses `bind_data.secret_name` for both.
   - Recommendation: Use attached database syntax (`db_name.dbo.table`) for consistency with the insert queries and the duplicate check. This uses the same connection mechanism. If the existing `GetForeignKeyConstraints()` works with `mssql_scan(secret_name, ...)`, keep it for metadata queries but use attached database syntax for value-existence queries.

2. **Which FK constraints actually exist on tblPrimanota in practice?**
   - What we know: The code dynamically discovers FK constraints via `sys.foreign_keys`. The columns `decKontoNr`, `decGegenkontoNr`, and `decEaKontoNr` are likely FK references to a Konto (account) table. `guiVerfahrenID` is a parameter (not from source data) so it wouldn't be found in source columns. `lngKanzleiKontenRahmenID` is also a parameter.
   - What's unclear: The exact FK definitions in the actual MSSQL WZ schema. The dynamic discovery approach handles this, but understanding typical constraints helps with testing.
   - Recommendation: The dynamic discovery approach is correct. Let the schema define what gets validated. Parameters like `guiVerfahrenID` and `lngKanzleiKontenRahmenID` won't match any source columns and will be silently skipped.

3. **Should `guiVerfahrenID` and `lngKanzleiKontenRahmenID` be validated?**
   - What we know: These are function parameters, not source data columns. They reference `tblVerfahren` and a KontenRahmen table respectively. They could have invalid values.
   - What's unclear: Whether the requirement CST-01 ("foreign key column values in source data") includes function parameters or only source table data.
   - Recommendation: Validate only source data column FK values as CST-01 specifies "source data". Parameter validation (verfahren_id, konten_rahmen_id) is a separate concern and could be added later. However, it would be a small addition to also validate these parameters since the FK discovery would reveal them. Mark as Claude's discretion.

4. **CAST semantics for numeric FK columns**
   - What we know: `decKontoNr` is a DECIMAL type in MSSQL. Source data may provide it as an integer, float, or string. The `Value::ToString()` representation may not match MSSQL's representation exactly (e.g., `1234` vs `1234.00` vs `1234.0000`).
   - What's unclear: Whether MSSQL's `IN` clause handles implicit type conversion for `DECIMAL IN (VARCHAR values)`.
   - Recommendation: Cast the referenced column to a consistent format in the validation query. Use numeric comparison if possible: `WHERE referenced_column IN (1234, 5678)` without quotes for numeric types. Alternatively, round-trip through `CAST(referenced_column AS VARCHAR)` and `CAST(source_value AS VARCHAR)` after normalizing.

## Sources

### Primary (HIGH confidence)
- `src/constraint_checker.cpp` -- Existing FK discovery implementation, `ValidateConstraints()` stub with TODO
- `src/include/wz_extension.hpp` -- `ForeignKeyConstraint`, `ConstraintViolation` struct definitions
- `src/into_wz_function.cpp` -- Current pipeline structure, `CheckDuplicates()` pattern for batch existence checking, column mapping fallback logic
- `.planning/codebase/CONCERNS.md` -- Documents `ValidateConstraints()` as incomplete tech debt, buffer overflow risk
- `.planning/codebase/ARCHITECTURE.md` -- Confirms "Foreign key checking implemented but not enforced"

### Secondary (MEDIUM confidence)
- MSSQL `sys.foreign_keys` documentation -- Standard SQL Server system view for FK metadata discovery. The existing `FK_QUERY_TEMPLATE` in constraint_checker.cpp correctly queries the necessary system views.
- DuckDB attached database syntax -- Verified through existing working code in `CheckDuplicates()` and all insert operations

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- No new libraries needed; all infrastructure already exists in the codebase
- Architecture: HIGH -- Pattern directly follows existing `CheckDuplicates()` approach; integration point is clear
- Pitfalls: HIGH -- Identified from analyzing existing code patterns and the MSSQL/DuckDB bridge behavior

**Research date:** 2026-02-06
**Valid until:** 2026-03-06 (stable -- no external dependencies changing)
