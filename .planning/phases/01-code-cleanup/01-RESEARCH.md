# Phase 1: Code Cleanup - Research

**Researched:** 2026-02-05
**Domain:** C++ codebase refactoring (DuckDB extension)
**Confidence:** HIGH

## Summary

This phase targets three cleanup tasks in a small C++ DuckDB extension codebase (6 source files, ~1,400 lines of custom code): removing dead files, consolidating duplicate helpers, and eliminating goto-based control flow in the main execution function. The research focused on understanding the exact code dependencies, identifying what can be safely removed vs. what must be relocated, and mapping the goto control flow to determine the correct early-return replacement pattern.

The codebase analysis reveals clear boundaries. Two files (`primanota_mapper.cpp`, `vorlauf_builder.cpp`) are entirely dead except for one function (`DeriveVorlaufBezeichnung`) that must be relocated before deletion. Three helper functions are duplicated across files. The main `IntoWzExecute` function uses 9 goto statements all jumping to a single label, making the conversion to early returns straightforward. The function is 260 lines long with 7 logical steps, well-suited for extraction into sub-functions.

Key insight: the goto replacement and function extraction are tightly coupled -- extracting sub-functions naturally eliminates the gotos because each extracted function can return errors directly. The two plans (dead code + helpers, then goto refactoring) should execute in that order since dead file removal simplifies the codebase before the more complex restructuring.

**Primary recommendation:** Execute dead code removal and helper consolidation first (Plan 01-01), then restructure IntoWzExecute with early returns and extracted sub-functions (Plan 01-02). The `DeriveVorlaufBezeichnung` function must be moved to `wz_utils.hpp` before deleting `vorlauf_builder.cpp`.

## Standard Stack

This phase does not introduce new libraries. It is a refactoring of existing C++ code within the DuckDB extension framework.

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| DuckDB C++ API | v1.0+ | Extension framework, types, connection management | Required -- the extension is built against this |
| C++17 standard library | C++17 | `<chrono>`, `<sstream>`, `<algorithm>`, smart pointers | Already required by CMakeLists.txt |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| yyjson (via DuckDB) | bundled | JSON parsing (linked but currently unused by extension) | Not needed for this phase |

### Alternatives Considered

Not applicable -- this phase is refactoring, not adding dependencies.

## Architecture Patterns

### Current File Structure (Before Cleanup)
```
src/
  include/
    wz_extension.hpp          # Shared types + all function declarations
  wz_extension.cpp             # Extension entry point (keep as-is)
  into_wz_function.cpp         # Main logic -- 946 lines, has gotos (MODIFY)
  primanota_mapper.cpp         # Dead file (DELETE)
  vorlauf_builder.cpp          # Mostly dead, one used function (DELETE after relocation)
  constraint_checker.cpp       # Phase 3 scope (DO NOT TOUCH)
```

### Target File Structure (After Cleanup)
```
src/
  include/
    wz_extension.hpp          # Trimmed: remove dead struct/function declarations
    wz_utils.hpp              # NEW: FindColumnIndex, GetCurrentTimestamp, DeriveVorlaufBezeichnung
  wz_extension.cpp             # Unchanged
  into_wz_function.cpp         # Restructured: extracted sub-functions, early returns, no gotos
  constraint_checker.cpp       # Unchanged (Phase 3)
```

### Pattern 1: Early Return for Error Handling
**What:** Replace goto-to-common-exit with check-and-return-error at each validation step
**When to use:** When a function has multiple sequential validation/execution steps that should abort on first failure
**Why it works here:** All 9 gotos jump to the same label (`output_results`) which outputs accumulated results. Each goto is preceded by `AddErrorResult(...)`. The replacement is: call AddErrorResult, then output results and return immediately.

```cpp
// BEFORE (current goto pattern):
if (bind_data.gui_verfahren_id.empty()) {
    AddErrorResult(bind_data, "ERROR", "gui_verfahren_id is required");
    goto output_results;
}
// ... more code ...
output_results:
    // output results to DataChunk

// AFTER (early return pattern):
if (bind_data.gui_verfahren_id.empty()) {
    AddErrorResult(bind_data, "ERROR", "gui_verfahren_id is required");
    OutputResults(bind_data, global_state, output);
    return;
}
```

### Pattern 2: Extract Sub-Functions by Concern
**What:** Break monolithic IntoWzExecute into logical sub-functions
**When to use:** When a function exceeds ~50 lines and has clear logical boundaries
**Why it works here:** IntoWzExecute has 7 clearly commented steps

Recommended extractions:
```cpp
// Helper to output accumulated results to the DataChunk
static void OutputResults(IntoWzBindData &bind_data,
                          IntoWzGlobalState &global_state,
                          DataChunk &output);

// Step 2: Load source data from DuckDB table
static bool LoadSourceData(ClientContext &context,
                           IntoWzBindData &bind_data,
                           string &error_message);

// Step 3: Check for duplicate Primanota IDs
static bool ValidateDuplicates(ClientContext &context,
                               IntoWzBindData &bind_data,
                               string &error_message);

// Step 5: Insert Vorlauf header record
static bool InsertVorlauf(Connection &txn_conn,
                          IntoWzBindData &bind_data,
                          const string &vorlauf_id,
                          const string &date_from,
                          const string &date_to,
                          string &error_message);

// Step 6: Insert Primanota rows in batches
static bool InsertPrimanota(Connection &txn_conn,
                            IntoWzBindData &bind_data,
                            const string &vorlauf_id,
                            const string &date_to,
                            int64_t &total_rows,
                            string &error_message);
```

### Pattern 3: Utility Header for Shared Functions
**What:** Consolidate duplicated helpers into a single header-only utility file
**When to use:** When the same function exists in multiple translation units
**Why header-only:** These functions are small (< 20 lines each), used in few files, and benefit from being inline to avoid linker complexity. The extension is small enough that header-only is simpler than a separate .cpp file.

```cpp
// wz_utils.hpp
#pragma once
#include "duckdb.hpp"
#include <chrono>
#include <algorithm>

namespace duckdb {

// Case-insensitive column index lookup. Returns DConstants::INVALID_INDEX if not found.
inline idx_t FindColumnIndex(const vector<string> &columns, const string &name) {
    string name_lower = name;
    std::transform(name_lower.begin(), name_lower.end(), name_lower.begin(), ::tolower);
    for (idx_t i = 0; i < columns.size(); i++) {
        string col_lower = columns[i];
        std::transform(col_lower.begin(), col_lower.end(), col_lower.begin(), ::tolower);
        if (col_lower == name_lower) {
            return i;
        }
    }
    return DConstants::INVALID_INDEX;
}

// Generate MSSQL-formatted timestamp with centisecond precision.
inline string GetCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&time));
    char result[64];
    snprintf(result, sizeof(result), "%s.%02d", buffer, static_cast<int>(ms.count() / 10));
    return string(result);
}

// Derive Vorlauf description from date range (e.g., "Vorlauf 01/2025-03/2025").
inline string DeriveVorlaufBezeichnung(const string &date_from, const string &date_to) {
    // Extract month/year from YYYY-MM-DD format
    int month_from = 0, year_from = 0, month_to = 0, year_to = 0;
    if (date_from.length() >= 7) {
        year_from = std::stoi(date_from.substr(0, 4));
        month_from = std::stoi(date_from.substr(5, 2));
    }
    if (date_to.length() >= 7) {
        year_to = std::stoi(date_to.substr(0, 4));
        month_to = std::stoi(date_to.substr(5, 2));
    }
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "Vorlauf %02d/%d-%02d/%d",
             month_from, year_from, month_to, year_to);
    return string(buffer);
}

} // namespace duckdb
```

### Anti-Patterns to Avoid
- **Changing constraint_checker.cpp:** This file is Phase 3 scope. Do not modify, remove, or refactor it even though its functions are currently unused. The header declarations for constraint types (`ForeignKeyConstraint`, `ConstraintViolation`) must remain.
- **Splitting into_wz_function.cpp into multiple files:** User decision locks extracted functions into the same file.
- **Removing commented-out code or unused includes:** User decision explicitly excludes these from scope.
- **Creating a wz_utils.cpp file:** Keep it header-only for simplicity. The functions are small enough for inline.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Case-insensitive string compare | Custom tolower loop per call site | Single `FindColumnIndex` in `wz_utils.hpp` | Already exists 3 times, consolidate |
| MSSQL timestamp formatting | Per-file timestamp functions | Single `GetCurrentTimestamp` in `wz_utils.hpp` | Currently 3 slightly different implementations |
| UUID generation | Custom implementation | DuckDB's `UUID::GenerateRandomUUID()` | Already used correctly, just keep it |

**Key insight:** The "don't hand-roll" items in this phase are all about eliminating the existing hand-rolled duplicates, not about finding external solutions.

## Common Pitfalls

### Pitfall 1: Forgetting DeriveVorlaufBezeichnung When Deleting vorlauf_builder.cpp
**What goes wrong:** Deleting `vorlauf_builder.cpp` breaks the build because `into_wz_function.cpp:804` calls `DeriveVorlaufBezeichnung` which is defined there.
**Why it happens:** The file appears fully dead at a glance, but one function is actively used.
**How to avoid:** Move `DeriveVorlaufBezeichnung` to `wz_utils.hpp` BEFORE deleting `vorlauf_builder.cpp`. Build and verify.
**Warning signs:** Linker error: "undefined reference to `DeriveVorlaufBezeichnung`"

### Pitfall 2: Leaving Dead Declarations in wz_extension.hpp
**What goes wrong:** After deleting dead files, `wz_extension.hpp` still declares functions that no longer exist (`MapDataToPrimanota`, `GeneratePrimanotaInsertSQL`, `BuildVorlaufFromData`), and structs that are no longer used (`VorlaufRecord`, `PrimanotaRecord`, `WzConfig`).
**Why it happens:** Header cleanup is easy to forget when focused on .cpp files.
**How to avoid:** After deleting dead .cpp files, audit `wz_extension.hpp` and remove declarations for deleted functions. Remove `VorlaufRecord`, `PrimanotaRecord`, and `WzConfig` structs. Keep `ForeignKeyConstraint`, `ConstraintViolation`, `InsertResult`, and constraint checker declarations (Phase 3 needs them).
**Warning signs:** Compiler warnings about unused declarations (though most compilers won't warn about this).

### Pitfall 3: Breaking the output_results Duplication
**What goes wrong:** The current code has the result-output logic duplicated: once at the top of IntoWzExecute (lines 664-685, for re-entry when already executed) and once at the bottom (lines 906-918, the goto target). When refactoring, forgetting to handle the re-entry path breaks subsequent calls.
**Why it happens:** DuckDB table functions can be called multiple times (for pagination). The first call executes, subsequent calls just return remaining results.
**How to avoid:** Extract `OutputResults` as a shared helper. Use it both in the re-entry path and after each early return.
**Warning signs:** Only first batch of results returned; subsequent calls return empty.

### Pitfall 4: CMakeLists.txt Not Updated After File Deletion
**What goes wrong:** Build system still tries to compile deleted files.
**Why it happens:** CMakeLists.txt explicitly lists EXTENSION_SOURCES. Deleting .cpp files without removing them from the list causes build failure.
**How to avoid:** Remove `src/primanota_mapper.cpp` and `src/vorlauf_builder.cpp` from EXTENSION_SOURCES in CMakeLists.txt. No need to add `wz_utils.hpp` (headers are not listed in EXTENSION_SOURCES).
**Warning signs:** CMake error: "Cannot find source file: src/primanota_mapper.cpp"

### Pitfall 5: Transaction Rollback Not Handled in Early Return Path
**What goes wrong:** If an error occurs after `BEGIN TRANSACTION` but the early return path does not issue `ROLLBACK`, the transaction hangs open.
**Why it happens:** The goto pattern ensured a single exit point, making it easy to reason about transaction state. Early returns create multiple exit points.
**How to avoid:** Use a transaction RAII guard or ensure every error path after BEGIN TRANSACTION issues ROLLBACK before returning. The extracted `InsertVorlauf` and `InsertPrimanota` functions should return success/failure, and the caller handles ROLLBACK on failure.
**Warning signs:** "Cannot rollback - no transaction is active" errors or locked MSSQL tables.

### Pitfall 6: Static Function Visibility When Extracting
**What goes wrong:** Extracted functions declared `static` in `into_wz_function.cpp` cannot access `IntoWzBindData` fields if it was forward-declared or private.
**Why it happens:** All current helpers are `static` (file-scope). Extracted functions should also be `static` to maintain the same visibility.
**How to avoid:** Keep extracted functions as `static` functions in the same file, placed above `IntoWzExecute`. Since `IntoWzBindData` and `IntoWzGlobalState` are structs defined in the same file, they are accessible.
**Warning signs:** Compilation errors about incomplete types or inaccessible members.

## Code Examples

### Example 1: Refactored IntoWzExecute Structure (Skeleton)
```cpp
// Source: Derived from analysis of into_wz_function.cpp lines 660-919

// Extract: Output accumulated results to DataChunk
static void OutputResults(IntoWzBindData &bind_data,
                          IntoWzGlobalState &global_state,
                          DataChunk &output) {
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

static void IntoWzExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<IntoWzBindData>();
    auto &global_state = data_p.global_state->Cast<IntoWzGlobalState>();

    // Re-entry: return remaining results
    if (bind_data.executed) {
        OutputResults(bind_data, global_state, output);
        return;
    }
    bind_data.executed = true;

    // Step 1: Validate required parameters
    if (bind_data.gui_verfahren_id.empty()) {
        AddErrorResult(bind_data, "ERROR", "gui_verfahren_id is required");
        OutputResults(bind_data, global_state, output);
        return;
    }
    if (bind_data.source_table.empty()) {
        AddErrorResult(bind_data, "ERROR", "source_table is required");
        OutputResults(bind_data, global_state, output);
        return;
    }

    // Step 2: Load source data
    string error_msg;
    if (!LoadSourceData(context, bind_data, error_msg)) {
        AddErrorResult(bind_data, "ERROR", error_msg);
        OutputResults(bind_data, global_state, output);
        return;
    }

    // Step 3: Check duplicates
    if (!ValidateDuplicates(context, bind_data, error_msg)) {
        AddErrorResult(bind_data, "ERROR", error_msg);
        OutputResults(bind_data, global_state, output);
        return;
    }

    // Step 4: Prepare Vorlauf data
    string vorlauf_id = GenerateUUID();
    auto date_range = FindDateRange(bind_data.source_rows, bind_data.source_columns);
    // ... date defaults ...

    // Step 5-7: Transaction block
    auto &db = DatabaseInstance::GetDatabase(context);
    Connection txn_conn(db);

    if (!ExecuteMssqlStatementWithConn(txn_conn, "BEGIN TRANSACTION", error_msg)) {
        AddErrorResult(bind_data, "ERROR", "Failed to begin transaction: " + error_msg);
        OutputResults(bind_data, global_state, output);
        return;
    }

    // Insert Vorlauf
    auto vorlauf_start = std::chrono::high_resolution_clock::now();
    if (!InsertVorlauf(txn_conn, bind_data, vorlauf_id, date_from, date_to, error_msg)) {
        string rollback_err;
        ExecuteMssqlStatementWithConn(txn_conn, "ROLLBACK", rollback_err);
        AddErrorResult(bind_data, "tblVorlauf", "Failed to insert Vorlauf: " + error_msg, vorlauf_id);
        OutputResults(bind_data, global_state, output);
        return;
    }
    // ... timing and success result ...

    // Insert Primanota batches
    int64_t total_rows = 0;
    if (!InsertPrimanota(txn_conn, bind_data, vorlauf_id, date_to, total_rows, error_msg)) {
        string rollback_err;
        ExecuteMssqlStatementWithConn(txn_conn, "ROLLBACK", rollback_err);
        AddErrorResult(bind_data, "tblPrimanota", error_msg, vorlauf_id);
        OutputResults(bind_data, global_state, output);
        return;
    }

    // Commit
    if (!ExecuteMssqlStatementWithConn(txn_conn, "COMMIT", error_msg)) {
        AddErrorResult(bind_data, "ERROR", "Failed to commit transaction: " + error_msg, vorlauf_id);
        OutputResults(bind_data, global_state, output);
        return;
    }

    // ... success result ...
    OutputResults(bind_data, global_state, output);
}
```

### Example 2: Error Message Format
```cpp
// Source: User decision from CONTEXT.md
// Format: "Failed to <action> <table> row <N>: <MSSQL error>"

// Vorlauf insert failure:
"Failed to insert Vorlauf: <MSSQL error text>"

// Primanota batch insert failure (include batch start row):
"Failed to insert tblPrimanota rows 200-299: <MSSQL error text>"

// Parameter validation:
"gui_verfahren_id is required"

// Source data issue:
"Failed to read source table: <DuckDB error>"
"Source table is empty"
"Duplicate guiPrimanotaID found: id1, id2, id3. 3 records already exist in tblPrimanota."
```

### Example 3: CMakeLists.txt After Dead File Removal
```cmake
set(EXTENSION_SOURCES
    src/wz_extension.cpp
    src/into_wz_function.cpp
    src/constraint_checker.cpp
)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| goto for error handling in C++ | Early returns, RAII, or `std::expected` (C++23) | Long-standing best practice | gotos are a recognized anti-pattern in modern C++ |
| Duplicated static helpers per file | Header-only inline utilities | Standard practice | Eliminates drift between implementations |
| Monolithic functions | Extract-by-concern sub-functions | Standard practice | Testability, readability |

**Note on C++23 std::expected:** While `std::expected<T, E>` would be the most modern error handling pattern, this project uses C++17 (per CMakeLists.txt). The early-return-with-bool pattern is the appropriate C++17 approach and matches the existing `ExecuteMssqlStatementWithConn` convention (returns bool, takes error string by reference).

## Open Questions

1. **Should unused structs in wz_extension.hpp be removed?**
   - What we know: `VorlaufRecord`, `PrimanotaRecord`, and `WzConfig` are only used by the dead files being deleted. `constraint_checker.cpp` does NOT use them.
   - What's unclear: Whether the user wants to keep them for potential future use or remove them for cleanliness.
   - Recommendation: Remove them. The decision says "remove clearly unused functions/variables" and these structs become unused once the dead files are deleted. They can always be re-added from git history. Phase 3 will define its own data structures if needed.

2. **Should ExtractMonthYear stay inlined in DeriveVorlaufBezeichnung or be a separate utility?**
   - What we know: `ExtractMonthYear` is a private helper in `vorlauf_builder.cpp` used only by `DeriveVorlaufBezeichnung`.
   - Recommendation: Inline the logic directly into `DeriveVorlaufBezeichnung` in `wz_utils.hpp` (as shown in the code example above). No need for a separate function since it has only one caller.

3. **Exactly which additional functions beyond InsertVorlauf/InsertPrimanota should be extracted?**
   - What we know: User named InsertVorlauf and InsertPrimanota explicitly. The function has 7 logical steps.
   - Recommendation: Extract 5 functions total: `OutputResults`, `LoadSourceData`, `ValidateDuplicates`, `InsertVorlauf`, `InsertPrimanota`. Keep `FindDateRange`, parameter validation, and date defaults inline in IntoWzExecute since they are short and sequential. This is Claude's discretion per CONTEXT.md.

## Detailed Dependency Map

### What Calls What (Cross-File Dependencies)

```
into_wz_function.cpp
  calls: DeriveVorlaufBezeichnung (from vorlauf_builder.cpp) -- MUST RELOCATE
  uses:  InsertResult (from wz_extension.hpp) -- KEEP
  uses:  FindColumnIndex (local static) -- MOVE TO wz_utils.hpp
  uses:  GetCurrentTimestamp (local static) -- MOVE TO wz_utils.hpp

vorlauf_builder.cpp  (TO DELETE)
  defines: DeriveVorlaufBezeichnung -- RELOCATE to wz_utils.hpp
  defines: BuildVorlaufFromData -- DEAD (no callers)
  defines: ExtractMonthYear -- private helper, inline into DeriveVorlaufBezeichnung
  defines: GetCurrentTimestampString -- DEAD (same as GetCurrentTimestamp)
  defines: FindDateRangeInData -- DEAD (into_wz_function.cpp has its own)
  uses:    VorlaufRecord, WzConfig -- DEAD structs after file deletion

primanota_mapper.cpp  (TO DELETE)
  defines: MapDataToPrimanota -- DEAD (no callers)
  defines: GeneratePrimanotaInsertSQL -- DEAD (no callers)
  defines: FindColumnIndex -- DEAD duplicate
  defines: GetTimestampNow -- DEAD duplicate

constraint_checker.cpp  (DO NOT TOUCH - Phase 3)
  defines: GetForeignKeyConstraints -- not called yet (Phase 3)
  defines: CheckDuplicatePrimanotaIds -- not called yet (Phase 3)
  defines: ValidateConstraints -- stub (Phase 3)
  uses:    ForeignKeyConstraint, ConstraintViolation -- KEEP in header

wz_extension.hpp  (MODIFY)
  KEEP: WzExtension class
  KEEP: ForeignKeyConstraint, ConstraintViolation, InsertResult structs
  KEEP: Constraint checker function declarations
  KEEP: RegisterIntoWzFunction declaration
  KEEP: DeriveVorlaufBezeichnung declaration (moves to wz_utils.hpp, remove from here)
  REMOVE: VorlaufRecord, PrimanotaRecord, WzConfig structs
  REMOVE: BuildVorlaufFromData, MapDataToPrimanota declarations
```

### Goto Map (All 9 Instances)

| Line | Context | Trigger | Error Added |
|------|---------|---------|-------------|
| 697 | Step 1 | gui_verfahren_id empty | "gui_verfahren_id is required" |
| 702 | Step 1 | source_table empty | "source_table is required" |
| 719 | Step 2 | Source query failed | "Failed to read source table: ..." |
| 744 | Step 2 | Source table empty | "Source table is empty" |
| 774 | Step 3 | Duplicates found | "Duplicate guiPrimanotaID found: ..." |
| 821 | Step 5 | BEGIN TRANSACTION failed | "Failed to begin transaction: ..." |
| 842 | Step 5 | Vorlauf INSERT failed | "Insert failed: ..." (after ROLLBACK) |
| 883 | Step 6 | Primanota INSERT failed | "Insert failed at row N: ..." (after ROLLBACK) |
| 895 | Step 7 | COMMIT failed | "Failed to commit transaction: ..." |

All jump to `output_results:` at line 904.

## Sources

### Primary (HIGH confidence)
- Direct source code analysis of all 6 files in `C:\wz-extension\src\`
- `CMakeLists.txt` -- build configuration, C++17 requirement, EXTENSION_SOURCES list
- `.planning/codebase/ARCHITECTURE.md` -- data flow and layer analysis
- `.planning/codebase/CONCERNS.md` -- documented tech debt and duplicate code
- `.planning/codebase/STRUCTURE.md` -- file layout and naming conventions
- `.planning/phases/01-code-cleanup/01-CONTEXT.md` -- user decisions

### Secondary (MEDIUM confidence)
- None needed -- all findings based on direct codebase analysis

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - direct codebase analysis, no external libraries involved
- Architecture patterns: HIGH - patterns derived from actual code structure and user decisions
- Pitfalls: HIGH - identified through dependency tracing across all source files
- Code examples: HIGH - derived directly from existing code patterns

**Research date:** 2026-02-05
**Valid until:** No expiry (codebase-specific research, not library-version-dependent)
