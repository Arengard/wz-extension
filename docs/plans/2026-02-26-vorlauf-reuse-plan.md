# Vorlauf Reuse Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reuse existing Vorlauf records when the data's date range fits within an existing Vorlauf's Von..Bis range, instead of always creating new ones.

**Architecture:** Add a `FindExistingVorlauf` helper that queries MSSQL for a matching Vorlauf by guiVerfahrenID + date range containment. Integrate it into both `into_wz` (single-vorlauf path, `generate_vorlauf_id=true`) and `batch_into_wz` (per-group Vorlauf creation). When a match is found, skip UUID generation and use the existing vorlauf_id; UPDATE its bezeichnung/dates rather than INSERTing.

**Tech Stack:** C++17, DuckDB extension API, MSSQL via attached database

**Design doc:** `docs/plans/2026-02-26-vorlauf-reuse-design.md`

---

### Task 1: Add `FindExistingVorlauf` helper function

**Files:**
- Modify: `src/into_wz_function.cpp` — add after `VorlaufExists` (line ~949)

**Step 1: Write the helper function**

Add `FindExistingVorlauf` immediately after the `VorlaufExists` function (after line 949):

```cpp
// ============================================================================
// Find an existing tblVorlauf record whose date range covers the given dates.
// Returns true on success (SQL OK), false on SQL error.
// Sets existing_vorlauf_id to the found UUID, or empty string if none.
// ============================================================================

static bool FindExistingVorlauf(Connection &conn,
                                const string &db_name,
                                const string &verfahren_id,
                                const string &date_from,
                                const string &date_to,
                                string &existing_vorlauf_id,
                                string &error_message) {
    existing_vorlauf_id.clear();

    string sql = "SELECT TOP 1 CAST(guiVorlaufID AS VARCHAR(36)) AS guiVorlaufID"
                 " FROM " + db_name + ".dbo.tblVorlauf"
                 " WHERE guiVerfahrenID = '" + EscapeSqlString(verfahren_id) + "'"
                 " AND dtmVorlaufDatumVon <= '" + EscapeSqlString(date_from) + " 00:00:00'"
                 " AND dtmVorlaufDatumBis >= '" + EscapeSqlString(date_to) + " 00:00:00'"
                 " ORDER BY dtmAngelegt DESC";

    auto result = conn.Query(sql);
    if (result->HasError()) {
        error_message = "Failed to query existing tblVorlauf: " + result->GetError();
        return false;
    }

    auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
    if (materialized->Collection().Count() > 0) {
        for (auto &chunk : materialized->Collection().Chunks()) {
            if (chunk.size() > 0) {
                existing_vorlauf_id = chunk.data[0].GetValue(0).ToString();
                break;
            }
        }
    }

    return true;
}
```

**Step 2: Build to verify compilation**

Run: `make release` (from project root)
Expected: Compiles without errors (function is defined but not yet called)

**Step 3: Commit**

```
git add src/into_wz_function.cpp
git commit -m "feat: add FindExistingVorlauf helper for Vorlauf reuse lookup"
```

---

### Task 2: Integrate Vorlauf reuse into `into_wz` single-vorlauf path

**Files:**
- Modify: `src/into_wz_function.cpp` — single-vorlauf UUID generation (~lines 1896-1910) and VORLAUF_RECORDS phase (~lines 2087-2139)

This task has two parts: (A) attempt reuse before generating a UUID, and (B) adjust the insert/update logic.

**Step 1: Modify UUID generation to try reuse first**

In the `VALIDATE_DATA` phase, the single-vorlauf pre-computation section (around line 1896-1910) currently does:

```cpp
} else {
    // --- Single-vorlauf pre-computation ---
    if (!bind_data.generate_vorlauf_id) {
        // ... reads vorlauf_id from source column ...
        state.vorlauf_id_from_source = true;
    }
    if (state.vorlauf_id.empty()) {
        state.vorlauf_id = GenerateVorlaufUUID(bind_data.row_keys, bind_data.gui_verfahren_id);
    }

    auto date_range = FindDateRange(bind_data.source_rows, bind_data.source_columns);
    state.date_from = date_range.first;
    state.date_to = date_range.second;
```

Replace this section with:

```cpp
} else {
    // --- Single-vorlauf pre-computation ---

    // Compute date range first (needed for Vorlauf reuse lookup)
    auto date_range = FindDateRange(bind_data.source_rows, bind_data.source_columns);
    state.date_from = date_range.first;
    state.date_to = date_range.second;
    if (state.date_from.empty()) state.date_from = GetCurrentMonthStart();
    if (state.date_to.empty()) state.date_to = GetCurrentDate();

    if (!bind_data.generate_vorlauf_id) {
        // User provided vorlauf_id in source column — respect it
        idx_t vorlauf_id_col = FindVorlaufIdColumn(bind_data.source_columns);
        if (vorlauf_id_col != DConstants::INVALID_INDEX
            && !bind_data.source_rows.empty()
            && vorlauf_id_col < bind_data.source_rows[0].size()
            && !bind_data.source_rows[0][vorlauf_id_col].IsNull()) {
            state.vorlauf_id = bind_data.source_rows[0][vorlauf_id_col].ToString();
            state.vorlauf_id_from_source = true;
        }
    }

    // If no source-provided ID, try to reuse an existing Vorlauf
    if (state.vorlauf_id.empty()) {
        // Reuse lookup needs a connection — defer to VORLAUF_RECORDS phase
        // by setting a flag. The actual query runs inside the transaction.
        state.try_vorlauf_reuse = true;
    }

    if (state.vorlauf_id.empty() && !state.try_vorlauf_reuse) {
        state.vorlauf_id = GenerateVorlaufUUID(bind_data.row_keys, bind_data.gui_verfahren_id);
    }
```

Note: Date range computation moves BEFORE UUID generation (it was already there but after, with duplicated empty checks at lines 1915-1916 — those are now removed since we do the check inline above).

**Step 2: Add `try_vorlauf_reuse` flag to `IntoWzGlobalState`**

Find `IntoWzGlobalState` struct (around line 310) and add the flag:

```cpp
bool try_vorlauf_reuse = false;
```

Add it near the existing `vorlauf_id_from_source` and `skip_vorlauf_insert` flags.

**Step 3: Modify VORLAUF_RECORDS phase to do reuse lookup**

In the single-vorlauf path of VORLAUF_RECORDS (lines ~2087-2139), replace the section from "Check if vorlauf exists" through "Insert or update Vorlauf" with:

```cpp
            // Try to reuse an existing Vorlauf (query-first lookup)
            if (state.try_vorlauf_reuse) {
                string existing_id;
                if (!FindExistingVorlauf(*state.txn_conn, bind_data.secret_name,
                                          bind_data.gui_verfahren_id,
                                          state.date_from, state.date_to,
                                          existing_id, error_msg)) {
                    CleanupAndError(state, bind_data, output, "tblVorlauf", error_msg);
                    return;
                }
                if (!existing_id.empty()) {
                    state.vorlauf_id = existing_id;
                    state.skip_vorlauf_insert = true;
                } else {
                    // No reusable Vorlauf found — generate new UUID
                    state.vorlauf_id = GenerateVorlaufUUID(bind_data.row_keys, bind_data.gui_verfahren_id);
                }
            }

            // Check if vorlauf exists (for source-provided IDs)
            if (state.vorlauf_id_from_source && !state.skip_vorlauf_insert) {
                bool exists = false;
                if (!VorlaufExists(*state.txn_conn, bind_data.secret_name, state.vorlauf_id, exists, error_msg)) {
                    CleanupAndError(state, bind_data, output, "tblVorlauf", error_msg, state.vorlauf_id);
                    return;
                }
                state.skip_vorlauf_insert = exists;
            }

            // Insert or update Vorlauf
            if (!state.skip_vorlauf_insert) {
                // ... existing InsertVorlauf code unchanged ...
            } else {
                // ... existing UpdateVorlauf code unchanged ...
            }
```

The existing InsertVorlauf/UpdateVorlauf blocks (lines 2119-2137) stay exactly as-is. Only the section above them changes.

**Step 4: Remove the now-redundant date range empty checks**

The old code at lines 1915-1916 had:
```cpp
if (state.date_from.empty()) state.date_from = GetCurrentMonthStart();
if (state.date_to.empty()) state.date_to = GetCurrentDate();
```

These are now handled in step 1 above. Remove these two lines if they remain after the edit.

**Step 5: Build and verify**

Run: `make release`
Expected: Compiles without errors

**Step 6: Commit**

```
git add src/into_wz_function.cpp
git commit -m "feat: integrate Vorlauf reuse into into_wz single-vorlauf path"
```

---

### Task 3: Integrate Vorlauf reuse into `batch_into_wz` PROCESS_GROUPS

**Files:**
- Modify: `src/into_wz_function.cpp` — batch PROCESS_GROUPS phase (~lines 2873-2946)

**Step 1: Modify per-group Vorlauf logic**

In PROCESS_GROUPS, the current code (lines 2873-2946) does:
1. Generate Vorlauf UUID (step 3)
2. Derive bezeichnung (step 4)
3. VorlaufExists check → insert or update (step 5)

Replace step 3 (lines 2873-2896) and step 5 (lines 2911-2946) with:

Replace the "3. Generate Vorlauf UUID" block (lines 2873-2896):

```cpp
        // 3. Find existing Vorlauf or generate new UUID
        {
            string existing_id;
            if (bind_data.generate_vorlauf_id) {
                // Try reuse first
                if (!FindExistingVorlauf(*state.txn_conn, bind_data.secret_name,
                                          group.gui_verfahren_id,
                                          group.date_from, group.date_to,
                                          existing_id, error_msg)) {
                    BatchCleanupAndError(state, bind_data, output, "ERROR",
                        "Failed to find existing Vorlauf for group " + group.gui_verfahren_id + ": " + error_msg);
                    return;
                }
            }

            if (!existing_id.empty()) {
                group.vorlauf_id = existing_id;
                group.reuse_existing = true;
            } else if (bind_data.generate_vorlauf_id) {
                // No reusable Vorlauf — generate new UUID
                string combined_key = "vorlauf:" + group.gui_verfahren_id + ":";
                for (size_t i = 0; i < group.row_indices.size(); i++) {
                    if (i > 0) combined_key += "||";
                    combined_key += bind_data.row_keys[group.row_indices[i]];
                }
                group.vorlauf_id = GenerateUUIDv5(combined_key);
            } else {
                // Try to read from source
                idx_t vorlauf_col = FindVorlaufIdColumn(bind_data.source_columns);
                if (vorlauf_col != DConstants::INVALID_INDEX) {
                    const auto &first_row = bind_data.source_rows[group.row_indices[0]];
                    if (vorlauf_col < first_row.size() && !first_row[vorlauf_col].IsNull()) {
                        group.vorlauf_id = first_row[vorlauf_col].ToString();
                    }
                }
                if (group.vorlauf_id.empty()) {
                    BatchCleanupAndError(state, bind_data, output, "ERROR",
                        "generate_vorlauf_id=false but no guiVorlaufID column found for group " + group.gui_verfahren_id);
                    return;
                }
            }
        }
```

Replace the "5. Insert or update Vorlauf record" block (lines 2911-2946):

```cpp
        // 5. Insert or update Vorlauf record
        {
            double vorlauf_start_t = std::chrono::duration<double>(
                std::chrono::high_resolution_clock::now() - group_start).count();

            if (group.reuse_existing) {
                // Reusing existing Vorlauf — update bezeichnung
                if (!UpdateVorlauf(*state.txn_conn, bind_data.secret_name,
                                    group.vorlauf_id, group.date_to,
                                    group.bezeichnung, bind_data.str_angelegt, error_msg)) {
                    BatchCleanupAndError(state, bind_data, output, "ERROR", error_msg);
                    return;
                }
                double dur = std::chrono::duration<double>(
                    std::chrono::high_resolution_clock::now() - group_start).count() - vorlauf_start_t;
                BatchAddSuccessResult(bind_data, "tblVorlauf (reused)", 0, group.vorlauf_id, dur);
            } else {
                bool exists = false;
                if (!VorlaufExists(*state.txn_conn, bind_data.secret_name, group.vorlauf_id, exists, error_msg)) {
                    BatchCleanupAndError(state, bind_data, output, "ERROR", error_msg);
                    return;
                }

                if (exists) {
                    if (!UpdateVorlauf(*state.txn_conn, bind_data.secret_name,
                                        group.vorlauf_id, group.date_to,
                                        group.bezeichnung, bind_data.str_angelegt, error_msg)) {
                        BatchCleanupAndError(state, bind_data, output, "ERROR", error_msg);
                        return;
                    }
                    double dur = std::chrono::duration<double>(
                        std::chrono::high_resolution_clock::now() - group_start).count() - vorlauf_start_t;
                    BatchAddSuccessResult(bind_data, "tblVorlauf (updated)", 0, group.vorlauf_id, dur);
                } else {
                    string vorlauf_sql = BuildVorlaufInsertSQL(
                        bind_data.secret_name, group.vorlauf_id, group.gui_verfahren_id,
                        bind_data.lng_kanzlei_konten_rahmen_id, bind_data.str_angelegt,
                        group.date_from, group.date_to, group.bezeichnung);

                    if (!ExecuteMssqlStatement(*state.txn_conn, vorlauf_sql, error_msg)) {
                        BatchCleanupAndError(state, bind_data, output, "ERROR",
                            "Failed to insert tblVorlauf for group " + group.gui_verfahren_id + ": " + error_msg);
                        return;
                    }
                    double dur = std::chrono::duration<double>(
                        std::chrono::high_resolution_clock::now() - group_start).count() - vorlauf_start_t;
                    BatchAddSuccessResult(bind_data, "tblVorlauf", 1, group.vorlauf_id, dur);
                }
            }
        }
```

**Step 2: Add `reuse_existing` flag to `VerfahrenGroup` struct**

Find `VerfahrenGroup` struct (around line 241) and add:

```cpp
bool reuse_existing = false;
```

**Step 3: Build and verify**

Run: `make release`
Expected: Compiles without errors

**Step 4: Commit**

```
git add src/into_wz_function.cpp
git commit -m "feat: integrate Vorlauf reuse into batch_into_wz per-group processing"
```

---

### Task 4: Update README and deploy

**Files:**
- Modify: `README.md` — update function documentation to mention Vorlauf reuse behavior

**Step 1: Update README**

Add a note to both `into_wz` and `batch_into_wz` documentation sections explaining:
- The function now checks for an existing Vorlauf matching the same guiVerfahrenID whose date range covers the source data
- If found, reuses the existing Vorlauf (appends Primanota rows to it) instead of creating a new one
- If no matching Vorlauf exists, creates a new one as before

**Step 2: Build final extension**

Run: `make release`
Copy: `cp build/release/extension/wz/wz.duckdb_extension dist/`

**Step 3: Commit and push**

```
git add README.md
git commit -m "docs: document Vorlauf reuse behavior in into_wz and batch_into_wz"
git push
```
