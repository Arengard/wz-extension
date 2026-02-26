# Design: batch_into_wz() Function

## Overview

New DuckDB table function that processes multiple `gui_verfahren_id` values from source data in a single call. Groups rows by their `gui_verfahren_id` column, creates a Vorlauf for each group, and inserts the corresponding Primanota rows - all within a single MSSQL transaction for all-or-nothing atomicity.

## User Interface

```sql
SELECT * FROM batch_into_wz(
    secret := 'ms',
    source_table := 'winsolvenz_ready',
    lng_kanzlei_konten_rahmen_id := 55,
    str_angelegt := 'rl'
);
```

No `gui_verfahren_id` parameter - read from a column in the source table.

### Parameters

| Parameter | Type | Required | Default | Notes |
|-----------|------|----------|---------|-------|
| secret | VARCHAR | No | "mssql_conn" | MSSQL connection secret |
| source_table | VARCHAR | Yes | - | Source table/query with gui_verfahren_id column |
| lng_kanzlei_konten_rahmen_id | BIGINT | Yes | - | Account framework ID |
| str_angelegt | VARCHAR | No | "wz_extension" | Creator label |
| monatsvorlauf | BOOLEAN | No | false | Monthly grouping within each verfahren |
| generate_vorlauf_id | BOOLEAN | No | true | Auto-generate Vorlauf UUIDs |
| skip_duplicate_check | BOOLEAN | No | false | Skip duplicate guiPrimanotaID check |
| skip_fk_check | BOOLEAN | No | false | Skip FK constraint validation |

### Key Difference from into_wz()

- `gui_verfahren_id` is NOT a parameter - it comes from a column in the source data
- Each unique `gui_verfahren_id` value in the source gets its own Vorlauf(s)
- All groups processed in a single MSSQL transaction (all-or-nothing)

## Data Flow

```
BIND -> Parse parameters (no gui_verfahren_id param)
  |
LOAD_DATA -> SELECT * FROM source_table, find gui_verfahren_id column
  |
GROUP -> Group rows by gui_verfahren_id value -> Map<string, vector<row_indices>>
  |
VALIDATE -> For each group: duplicate check + FK validation
  |
BEGIN TRANSACTION (single MSSQL transaction for all groups)
  |
FOR EACH gui_verfahren_id group:
  |-- PRE_COMPUTE -> UUID generation, date ranges, Vorlauf description
  |-- CREATE_VORLAUF -> Insert tblVorlauf record
  +-- TRANSFER_PRIMANOTA -> INSERT tblPrimanota rows (no BCP - must stay in transaction)
  |
COMMIT (all succeed) or ROLLBACK (any fails)
  |
OUTPUT_RESULTS -> Per-group results
```

## Architecture

### New File: src/batch_into_wz_function.cpp

Contains:
- `BatchIntoWzBind()` - Parameter parsing (same as into_wz minus gui_verfahren_id)
- `BatchIntoWzState` - State machine with phases
- `RegisterBatchIntoWzFunction()` - DuckDB function registration

### Reused from into_wz_function.cpp

Functions that need to be extracted/exposed for sharing:
- `LoadSourceData()` - Load all rows from source table
- `PopulateStagingTable()` - Populate staging table per group
- `BuildVorlaufInsertSQL()` - Generate Vorlauf INSERT statement
- `BulkTransferPrimanota()` - INSERT-based batch transfer
- `ValidateDuplicates()` - Per-group duplicate check
- Column mapping via `FindColumnWithAliases()` from wz_utils.hpp

### Registration in wz_extension.cpp

Add `RegisterBatchIntoWzFunction(instance)` call alongside existing registrations.

## Transaction Strategy

- INSERT-only transfer (no BCP) to maintain transaction integrity
- Single `BEGIN TRANSACTION` before processing any group
- All Vorlauf inserts and Primanota transfers within this transaction
- `ROLLBACK` on any group failure - undoes all changes
- `COMMIT` only after all groups complete successfully

## gui_verfahren_id Column Detection

Add aliases to wz_utils.hpp `FindColumnWithAliases()`:
- `guiVerfahrenID`, `gui_verfahren_id`, `guiverfahrenid`, `verfahren_id`, `verfahrenid`

Error with clear message if no matching column found in source data.

## Return Schema

Same as into_wz():
```
(table_name VARCHAR, rows_inserted BIGINT, gui_vorlauf_id VARCHAR,
 duration VARCHAR, success BOOLEAN, error_message VARCHAR)
```

Results show per-group progress:
```
GROUP:abc-123  | 0    |        |       | true | Processing group 1/5
tblVorlauf     | 1    | uuid-1 | 00:00 | true |
tblPrimanota   | 150  | uuid-1 | 00:01 | true |
GROUP:def-456  | 0    |        |       | true | Processing group 2/5
tblVorlauf     | 1    | uuid-2 | 00:00 | true |
tblPrimanota   | 230  | uuid-2 | 00:01 | true |
TOTAL          | 1200 |        | 00:05 | true | 5 groups processed
```

## Error Handling

- Missing gui_verfahren_id column: fail at LOAD phase with descriptive error
- Invalid UUID format in gui_verfahren_id column: fail at VALIDATE phase, report which rows
- Duplicate guiPrimanotaID (unless skipped): fail at VALIDATE phase before any inserts
- FK validation failure (unless skipped): fail at VALIDATE phase before any inserts
- MSSQL insert failure: ROLLBACK entire transaction, report which group failed
- Empty source table: fail at LOAD phase

## Decisions

- INSERT-only (no BCP) to maintain all-or-nothing atomicity within single transaction
- gui_verfahren_id column uses alias system for flexible column name matching
- Other parameters (konten_rahmen_id, str_angelegt) are shared across all groups
- New file rather than modifying into_wz_function.cpp to keep functions separate
- Shared logic extracted via header declarations
