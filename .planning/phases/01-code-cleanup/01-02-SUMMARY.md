---
phase: 01-code-cleanup
plan: 02
subsystem: core-refactor
tags: [goto-removal, early-returns, sub-function-extraction, bug-fix, guiVorlaufID]

dependency_graph:
  requires:
    - "01-01 (wz_utils.hpp, clean baseline)"
  provides:
    - "Zero-goto IntoWzExecute with 5 extracted sub-functions"
    - "OutputResults, LoadSourceData, ValidateDuplicates, InsertVorlauf, InsertPrimanota"
    - "guiVorlaufID bug fix: uses source column value when present"
  affects:
    - "Phase 2 (transaction fix) - cleaner function boundaries for transaction rework"

tech_stack:
  added: []
  patterns:
    - "Early return error pattern: AddErrorResult + OutputResults + return"
    - "Sub-function extraction with bool return + error_message out-param"

file_tracking:
  created: []
  modified:
    - src/into_wz_function.cpp
  deleted: []

decisions:
  - id: "01-02-D1"
    decision: "InsertVorlauf error message prefixed with 'Failed to insert Vorlauf:' inside sub-function"
    reason: "Keeps error context self-contained in the sub-function rather than requiring callers to format messages"

metrics:
  duration: "~5 minutes"
  completed: "2026-02-05"
---

# Phase 01 Plan 02: Goto Replacement and guiVorlaufID Fix Summary

Extracted 5 static sub-functions from IntoWzExecute, replaced all 9 goto statements with early returns using OutputResults(), removed the output_results: label, and fixed a bug where guiVorlaufID was always generated as a new UUID even when source data provided one. Net result: -116 lines, zero gotos, flat control flow.

## Tasks Completed

| Task | Name | Commit | Key Changes |
|------|------|--------|-------------|
| 1 | Extract sub-functions from IntoWzExecute | e70db7f | Added OutputResults, LoadSourceData, ValidateDuplicates, InsertVorlauf, InsertPrimanota above IntoWzExecute |
| 2 | Rewrite IntoWzExecute with early returns and fix guiVorlaufID bug | 7b2d062 | Replaced 9 gotos with early returns, removed output_results: label, removed extra braces, fixed guiVorlaufID |

## What Changed

### Modified: src/into_wz_function.cpp

**5 new static sub-functions** (inserted between AddSuccessResult and IntoWzExecute):

1. **OutputResults** - Outputs accumulated InsertResult entries to DataChunk with pagination via global_state.current_idx. Handles the empty case (sets cardinality 0).

2. **LoadSourceData** - Reads source table via a new Connection, populates bind_data.source_columns, source_types, and source_rows. Returns false with descriptive error on query failure or empty result.

3. **ValidateDuplicates** - Finds guiPrimanotaID column, extracts IDs, calls CheckDuplicates. Returns true if no column found (nothing to check). Returns false with formatted duplicate list on failure.

4. **InsertVorlauf** - Calls BuildVorlaufInsertSQL and ExecuteMssqlStatementWithConn. Prefixes error message with "Failed to insert Vorlauf:". Does not rollback (caller handles).

5. **InsertPrimanota** - Iterates source_rows in batches of 100, builds and executes INSERT SQL. Sets total_rows on success. Error message includes batch range. Does not rollback (caller handles).

**IntoWzExecute rewritten:**
- Re-entry path: single `OutputResults()` call (was 15-line inline block)
- All 9 goto statements replaced with `OutputResults(); return;`
- `output_results:` label removed entirely
- Extra `{...}` wrapper block around steps 2-7 removed (was needed for goto scope)
- Step 4 now checks source data for `guiVorlaufID` column before generating UUID

**guiVorlaufID bug fix (Step 4):**
- Before: `string vorlauf_id = GenerateUUID();` (always generated new UUID)
- After: Checks source data for `guiVorlaufID` column via `FindColumnIndex`. If column exists and first row has a non-null value, uses that value. Otherwise falls back to `GenerateUUID()`.

## Deviations from Plan

None - plan executed exactly as written.

## Decisions Made

1. **Error message formatting in InsertVorlauf** (01-02-D1): The extracted InsertVorlauf function prepends "Failed to insert Vorlauf: " to the MSSQL error message inside itself, keeping error context self-contained. The caller passes the sub-function's error_message directly to AddErrorResult without additional formatting.

## Verification Results

All success criteria met:
- `goto` count in into_wz_function.cpp: 0 (was 9)
- `output_results:` label count: 0 (removed)
- `OutputResults` occurrences: 11 (1 definition + 10 call sites)
- 5 extracted sub-functions exist with doc comments
- guiVorlaufID column checked before UUID generation (line 870)
- All transaction error paths (Vorlauf insert, Primanota insert) include ROLLBACK
- RegisterIntoWzFunction and all other functions unchanged

## Next Phase Readiness

Phase 1 (Code Cleanup) is now complete. Phase 2 (Transaction Fix) can proceed. The codebase now has:
- Clean, flat control flow in IntoWzExecute (no gotos, no nested braces)
- Modular sub-functions that can be individually tested or replaced
- Transaction error paths with proper ROLLBACK handling
- Known issue: DuckDB auto-manages transactions on Connection objects, which may conflict with explicit BEGIN/COMMIT SQL (Phase 2 scope)
