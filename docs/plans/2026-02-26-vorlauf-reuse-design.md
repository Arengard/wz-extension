# Vorlauf Reuse Design

**Date:** 2026-02-26
**Status:** Approved

## Problem

Both `into_wz` and `batch_into_wz` always generate a new Vorlauf UUID for each invocation (when `generate_vorlauf_id=true`). If an existing Vorlauf already covers the data's date range for the same Verfahren, the function should reuse it instead of creating a duplicate.

## Decision

Use a query-first lookup approach: before generating a new UUID, query MSSQL for an existing Vorlauf that matches the guiVerfahrenID and covers the data's date range.

## Lookup Query

```sql
SELECT TOP 1 CAST(guiVorlaufID AS VARCHAR(36)) AS guiVorlaufID
FROM {db}.dbo.tblVorlauf
WHERE guiVerfahrenID = '{verfahren_id}'
  AND dtmVorlaufDatumVon <= '{data_date_from} 00:00:00'
  AND dtmVorlaufDatumBis >= '{data_date_to} 00:00:00'
ORDER BY dtmAngelegt DESC
```

Returns the most recently created Vorlauf whose date range fully contains the source data's date range. If no match, returns empty — caller generates a new UUID.

## Matching Rules

- **Match by:** guiVerfahrenID + date range containment (data dates within Vorlauf Von..Bis)
- **Multiple matches:** Use most recent (ORDER BY dtmAngelegt DESC, TOP 1)
- **Dates outside range:** Create a new Vorlauf (do NOT extend existing range)
- **No parameter:** Always reuse by default, no opt-out parameter

## Changed Flow

### Before (current)
1. Compute date range from source data
2. Generate UUIDv5 from row data hash
3. Check if that UUID exists in tblVorlauf
4. If exists: UPDATE; if not: INSERT

### After (new)
1. Compute date range from source data
2. **Query `FindExistingVorlauf(verfahren_id, date_from, date_to)`**
3. If found: use existing vorlauf_id, UPDATE date/bezeichnung
4. If not found: generate UUIDv5, INSERT new Vorlauf

## Scope

- **`into_wz`:** Applied when `generate_vorlauf_id=true` (default). When `generate_vorlauf_id=false` (user provides vorlauf_id in source), behavior unchanged.
- **`batch_into_wz`:** Applied per group. Each group's date range is checked independently.

## New Helper

```cpp
static bool FindExistingVorlauf(Connection &conn,
                                const string &db_name,
                                const string &verfahren_id,
                                const string &date_from,
                                const string &date_to,
                                string &existing_vorlauf_id,
                                string &error_message);
```

Shared by both `into_wz` and `batch_into_wz`. Returns `true` on success (SQL executed), `false` on SQL error. Sets `existing_vorlauf_id` to the found UUID or empty string if none.

## Error Handling

No new error cases. SQL errors propagate via existing `error_message` pattern.
