# batch_into_wz() Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create a new `batch_into_wz()` DuckDB table function that reads `gui_verfahren_id` from a column in the source data, groups rows by that column, and creates a Vorlauf + inserts Primanota per group within a single all-or-nothing MSSQL transaction.

**Architecture:** The new function lives in the same file as `into_wz()` (`src/into_wz_function.cpp`) to directly reuse all static helper functions without refactoring. A new bind function, global state struct, and execute function are added at the bottom of the file, before `RegisterIntoWzFunction`. The function is registered alongside `into_wz()` in `wz_extension.cpp`.

**Tech Stack:** C++17, DuckDB extension API, MSSQL attached database via DuckDB MSSQL extension

**Key design decisions:**
- Same-file approach avoids risky refactoring of 2200-line production file with no test suite
- INSERT-only transfer (no BCP) to maintain single-transaction atomicity
- `gui_verfahren_id` column found via alias system (case-insensitive, multiple names)
- All other parameters (konten_rahmen_id, str_angelegt, etc.) shared across all groups

---

### Task 1: Add gui_verfahren_id alias group to wz_utils.hpp

**Files:**
- Modify: `src/include/wz_utils.hpp:86-97` (ALIAS_GROUPS array)

**Step 1: Add the alias group**

In `FindColumnWithAliases()`, add a new entry to the `ALIAS_GROUPS` array after the existing entries (before the closing `};`):

```cpp
{"guiverfahrenid",   {"guiVerfahrenID", "gui_verfahren_id", "guiverfahrenid", "verfahren_id", "verfahrenid", nullptr}},
```

This goes right after line 96 (the `dtmbelegdatum` entry), before line 97's closing `};`.

Also increase the aliases array size from `[12]` to `[12]` (already sufficient - max 6 aliases).

**Step 2: Build and verify compilation**

Run: `make release 2>&1 | tail -5`
Expected: Build succeeds (no errors from the header change)

**Step 3: Commit**

```bash
git add src/include/wz_utils.hpp
git commit -m "feat: add guiVerfahrenID alias group to FindColumnWithAliases"
```

---

### Task 2: Add BatchIntoWzBindData struct

**Files:**
- Modify: `src/into_wz_function.cpp` (add after line 197, after IntoWzBindData)

**Step 1: Add the bind data struct**

Insert after the closing `};` of `IntoWzBindData` (line 197):

```cpp
// ============================================================================
// Bind data for batch_into_wz function
// ============================================================================

struct BatchIntoWzBindData : public TableFunctionData {
    string secret_name;
    string source_table;
    int64_t lng_kanzlei_konten_rahmen_id;
    string str_angelegt;
    bool generate_vorlauf_id;
    bool monatsvorlauf;
    bool skip_duplicate_check;
    bool skip_fk_check;

    // Source data
    vector<string> source_columns;
    vector<vector<Value>> source_rows;
    vector<string> row_keys;

    // Column index for gui_verfahren_id in source data
    idx_t verfahren_id_col;

    // Results
    vector<InsertResult> results;
    bool executed;
};
```

Note: No `gui_verfahren_id` field - it comes from the data. The `verfahren_id_col` field stores the column index.

**Step 2: Verify compilation**

Run: `make release 2>&1 | tail -5`
Expected: Build succeeds

---

### Task 3: Add batch execution state and phase enum

**Files:**
- Modify: `src/into_wz_function.cpp` (add after BatchIntoWzBindData)

**Step 1: Add the phase enum and global state**

```cpp
// ============================================================================
// Execution phases for batch_into_wz
// ============================================================================

enum class BatchExecutionPhase {
    VALIDATE_PARAMS,
    LOAD_DATA,
    GROUP_DATA,
    VALIDATE_GROUPS,
    PROCESS_GROUPS,
    FINALIZE,
    OUTPUT_RESULTS,
    DONE
};

// Per-group data structure
struct VerfahrenGroup {
    string gui_verfahren_id;
    vector<idx_t> row_indices;
    // Per-group computed data (filled during PROCESS_GROUPS)
    string vorlauf_id;
    string date_from;
    string date_to;
    string bezeichnung;
    vector<string> primanota_ids;
};

struct BatchIntoWzGlobalState : public GlobalTableFunctionState {
    idx_t current_idx;
    BatchExecutionPhase phase;
    double progress;

    std::chrono::high_resolution_clock::time_point total_start;

    PrimanotaColumnIndices col_idx;
    unique_ptr<Connection> staging_conn;
    unique_ptr<Connection> txn_conn;

    // Grouping
    vector<VerfahrenGroup> groups;
    size_t current_group_idx;

    BatchIntoWzGlobalState() : current_idx(0),
                                 phase(BatchExecutionPhase::VALIDATE_PARAMS),
                                 progress(0.0), current_group_idx(0) {}
};
```

**Step 2: Verify compilation**

Run: `make release 2>&1 | tail -5`
Expected: Build succeeds

---

### Task 4: Add batch bind and init functions

**Files:**
- Modify: `src/into_wz_function.cpp` (add before `RegisterIntoWzFunction` at line 2206)

**Step 1: Add bind function**

Insert before `RegisterIntoWzFunction`:

```cpp
// ============================================================================
// batch_into_wz: Bind function
// ============================================================================

static unique_ptr<FunctionData> BatchIntoWzBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto bind_data = make_uniq<BatchIntoWzBindData>();

    // Set defaults
    bind_data->generate_vorlauf_id = true;
    bind_data->monatsvorlauf = false;
    bind_data->skip_duplicate_check = false;
    bind_data->skip_fk_check = false;
    bind_data->lng_kanzlei_konten_rahmen_id = 0;
    bind_data->verfahren_id_col = DConstants::INVALID_INDEX;

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.second.IsNull()) continue;

        if (kv.first == "secret") {
            bind_data->secret_name = StringValue::Get(kv.second);
        } else if (kv.first == "source_table") {
            bind_data->source_table = StringValue::Get(kv.second);
        } else if (kv.first == "lng_kanzlei_konten_rahmen_id") {
            bind_data->lng_kanzlei_konten_rahmen_id = kv.second.GetValue<int64_t>();
        } else if (kv.first == "str_angelegt") {
            bind_data->str_angelegt = StringValue::Get(kv.second);
        } else if (kv.first == "generate_vorlauf_id") {
            bind_data->generate_vorlauf_id = kv.second.GetValue<bool>();
        } else if (kv.first == "monatsvorlauf") {
            bind_data->monatsvorlauf = kv.second.GetValue<bool>();
        } else if (kv.first == "skip_duplicate_check") {
            bind_data->skip_duplicate_check = kv.second.GetValue<bool>();
        } else if (kv.first == "skip_fk_check") {
            bind_data->skip_fk_check = kv.second.GetValue<bool>();
        }
    }

    // Set defaults
    if (bind_data->secret_name.empty()) {
        bind_data->secret_name = "mssql_conn";
    }
    if (bind_data->str_angelegt.empty()) {
        bind_data->str_angelegt = "wz_extension";
    }
    bind_data->executed = false;

    // Same return schema as into_wz
    names = {"table_name", "rows_inserted", "gui_vorlauf_id", "duration", "success", "error_message"};
    return_types = {
        LogicalType::VARCHAR,
        LogicalType::BIGINT,
        LogicalType::VARCHAR,
        LogicalType::VARCHAR,
        LogicalType::BOOLEAN,
        LogicalType::VARCHAR
    };

    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> BatchIntoWzInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
    return make_uniq<BatchIntoWzGlobalState>();
}

static double BatchIntoWzProgress(ClientContext &context, const FunctionData *bind_data_p,
                                    const GlobalTableFunctionState *global_state_p) {
    auto &state = global_state_p->Cast<BatchIntoWzGlobalState>();
    return state.progress;
}
```

**Step 2: Verify compilation**

Run: `make release 2>&1 | tail -5`
Expected: Build succeeds

---

### Task 5: Add batch helper functions

**Files:**
- Modify: `src/into_wz_function.cpp` (add after batch bind/init, before execute)

**Step 1: Add batch-specific helper functions**

These helpers work with `BatchIntoWzBindData` (not `IntoWzBindData`):

```cpp
// ============================================================================
// batch_into_wz helpers
// ============================================================================

static void BatchAddErrorResult(BatchIntoWzBindData &bind_data, const string &table_name,
                                 const string &error_message, const string &vorlauf_id = "") {
    InsertResult result;
    result.table_name = table_name;
    result.rows_inserted = 0;
    result.gui_vorlauf_id = vorlauf_id;
    result.duration = "00:00:00";
    result.success = false;
    result.error_message = error_message;
    bind_data.results.push_back(result);
}

static void BatchAddSuccessResult(BatchIntoWzBindData &bind_data, const string &table_name,
                                    int64_t rows, const string &vorlauf_id, double duration) {
    InsertResult result;
    result.table_name = table_name;
    result.rows_inserted = rows;
    result.gui_vorlauf_id = vorlauf_id;
    result.duration = FormatDuration(duration);
    result.success = true;
    result.error_message = "";
    bind_data.results.push_back(result);
}

static void BatchOutputResults(BatchIntoWzBindData &bind_data,
                                BatchIntoWzGlobalState &global_state,
                                DataChunk &output) {
    if (global_state.current_idx >= bind_data.results.size()) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    while (global_state.current_idx < bind_data.results.size() && count < STANDARD_VECTOR_SIZE) {
        auto &result = bind_data.results[global_state.current_idx];
        output.SetValue(0, count, Value(result.table_name));
        output.SetValue(1, count, Value(result.rows_inserted));
        output.SetValue(2, count, Value(result.gui_vorlauf_id));
        output.SetValue(3, count, Value(result.duration));
        output.SetValue(4, count, Value(result.success));
        output.SetValue(5, count, Value(result.error_message));
        global_state.current_idx++;
        count++;
    }
    output.SetCardinality(count);
}

static bool BatchLoadSourceData(ClientContext &context,
                                 BatchIntoWzBindData &bind_data,
                                 string &error_message) {
    auto &db = DatabaseInstance::GetDatabase(context);
    Connection conn(db);

    string source_query = "SELECT * FROM " + bind_data.source_table;
    auto source_result = conn.Query(source_query);

    if (source_result->HasError()) {
        error_message = "Failed to read source table: " + source_result->GetError();
        return false;
    }

    auto source_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(source_result));

    bind_data.source_columns.reserve(source_materialized->ColumnCount());
    for (idx_t i = 0; i < source_materialized->ColumnCount(); i++) {
        bind_data.source_columns.push_back(source_materialized->ColumnName(i));
    }

    auto &collection = source_materialized->Collection();
    bind_data.source_rows.reserve(collection.Count());
    for (auto &chunk : collection.Chunks()) {
        for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
            vector<Value> row;
            row.reserve(chunk.ColumnCount());
            for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
                row.push_back(chunk.data[col_idx].GetValue(row_idx));
            }
            bind_data.source_rows.push_back(std::move(row));
        }
    }

    if (bind_data.source_rows.empty()) {
        error_message = "Source table is empty";
        return false;
    }

    return true;
}

// Populate staging table for a batch group. Reuses the same Appender logic as into_wz
// but reads from BatchIntoWzBindData.
static bool BatchPopulateStagingTable(Connection &staging_conn,
                                        BatchIntoWzBindData &bind_data,
                                        const vector<idx_t> &row_indices,
                                        const vector<string> &primanota_ids,
                                        const string &vorlauf_id,
                                        const string &verfahren_id,
                                        const string &str_angelegt,
                                        const string &vorlauf_datum_bis,
                                        const string &timestamp,
                                        const string &date_fallback,
                                        const PrimanotaColumnIndices &col_idx,
                                        string &error_message) {
    if (row_indices.empty()) return true;

    try {
        Appender appender(staging_conn, string(STAGING_TABLE_NAME));

        for (size_t i = 0; i < row_indices.size(); i++) {
            size_t row_idx = row_indices[i];
            const auto &row = bind_data.source_rows[row_idx];

            auto getValue = [&row](idx_t idx) -> const Value& {
                static const Value nv;
                if (idx == DConstants::INVALID_INDEX || idx >= row.size()) return nv;
                return row[idx];
            };

            const string &primanota_id = primanota_ids[i];

            const Value &beleg_datum_val = getValue(col_idx.col_beleg_datum);
            string beleg_datum = beleg_datum_val.IsNull() ? date_fallback : beleg_datum_val.ToString();
            if (beleg_datum.length() > 10) beleg_datum = beleg_datum.substr(0, 10);
            beleg_datum += " 00:00:00";

            bool is_soll = ParseSollHaben(getValue(col_idx.col_ysn_soll));
            const Value &eingabe_betrag_val = getValue(col_idx.col_eingabe_betrag);
            const Value &basis_betrag_ref = getValue(col_idx.col_basis_betrag);
            const Value &basis_betrag_val = (basis_betrag_ref.IsNull() && !eingabe_betrag_val.IsNull())
                ? eingabe_betrag_val : basis_betrag_ref;

            appender.BeginRow();
            appender.Append<int32_t>(0);                                    // lngTimestamp
            appender.Append(Value(str_angelegt));                           // strAngelegt
            appender.Append(Value(timestamp));                              // dtmAngelegt
            appender.Append(Value());                                       // strGeaendert (NULL)
            appender.Append(Value());                                       // dtmGeaendert (NULL)
            appender.Append(Value(primanota_id));                           // guiPrimanotaID
            appender.Append(Value(vorlauf_id));                             // guiVorlaufID
            appender.Append<int32_t>(1);                                    // lngStatus
            appender.Append(Value());                                       // lngZeilenNr (NULL)
            appender.Append<int32_t>(1);                                    // lngEingabeWaehrungID
            appender.Append(Value());                                       // lngBu (NULL)
            appender.Append(ToVarchar(getValue(col_idx.col_gegenkonto_nr)));// decGegenkontoNr
            appender.Append(ToVarchar(getValue(col_idx.col_konto_nr)));     // decKontoNr
            appender.Append(ToVarchar(getValue(col_idx.col_ea_konto_nr)));  // decEaKontoNr
            appender.Append(Value(vorlauf_datum_bis));                      // dtmVorlaufDatumBis
            appender.Append(Value(beleg_datum));                            // dtmBelegDatum
            appender.Append<int32_t>(is_soll ? 1 : 0);                     // ysnSoll
            appender.Append(ToVarchar(eingabe_betrag_val));                 // curEingabeBetrag
            appender.Append(ToVarchar(basis_betrag_val));                   // curBasisBetrag
            appender.Append(Value());                                       // curSkontoBetrag (NULL)
            appender.Append(Value());                                       // curSkontoBasisBetrag (NULL)
            appender.Append(Value());                                       // decKostMenge (NULL)
            appender.Append(Value());                                       // decWaehrungskurs (NULL)
            appender.Append(ToVarchar(getValue(col_idx.col_beleg1)));       // strBeleg1
            appender.Append(ToVarchar(getValue(col_idx.col_beleg2)));       // strBeleg2
            appender.Append(ToVarchar(getValue(col_idx.col_buch_text)));    // strBuchText
            appender.Append(Value());                                       // strKost1 (NULL)
            appender.Append(Value());                                       // strKost2 (NULL)
            appender.Append(Value());                                       // strEuLand (NULL)
            appender.Append(Value());                                       // strUstId (NULL)
            appender.Append(Value());                                       // decEuSteuersatz (NULL)
            appender.Append(Value());                                       // dtmZusatzDatum (NULL)
            appender.Append(Value(verfahren_id));                           // guiVerfahrenID
            appender.Append(Value());                                       // decEaSteuersatz (NULL)
            appender.Append<int32_t>(0);                                    // ysnEaTransaktionenManuell
            appender.Append(Value());                                       // decEaNummer (NULL)
            appender.Append(Value());                                       // lngSachverhalt13b (NULL)
            appender.Append(Value());                                       // dtmLeistung (NULL)
            appender.Append<int32_t>(0);                                    // ysnIstversteuerungInSollversteuerung
            appender.Append(Value());                                       // lngSkontoSachverhaltWarenRHB (NULL)
            appender.Append<int32_t>(0);                                    // ysnVStBeiZahlung
            appender.Append(Value());                                       // guiParentPrimanota (NULL)
            appender.Append<int32_t>(0);                                    // ysnGeneralUmkehr
            appender.Append(Value());                                       // decSteuersatzManuell (NULL)
            appender.Append<int32_t>(0);                                    // ysnMitUrsprungsland
            appender.Append(Value());                                       // strUrsprungsland (NULL)
            appender.Append(Value());                                       // strUrsprungslandUstId (NULL)
            appender.Append(Value());                                       // decUrsprungslandSteuersatz (NULL)
            appender.Append(Value());                                       // year_month (not used in batch)
            appender.EndRow();
        }

        appender.Close();
    } catch (const std::exception &e) {
        error_message = "Failed to populate staging table: " + string(e.what());
        return false;
    }

    return true;
}

static void BatchCleanupAndError(BatchIntoWzGlobalState &state, BatchIntoWzBindData &bind_data,
                                   DataChunk &output, const string &table_name,
                                   const string &error_message) {
    if (state.txn_conn) {
        string rb_err;
        ExecuteMssqlStatement(*state.txn_conn, "ROLLBACK", rb_err);
        state.txn_conn.reset();
    }
    if (state.staging_conn) {
        DropStagingTable(*state.staging_conn);
        state.staging_conn.reset();
    }
    BatchAddErrorResult(bind_data, table_name, error_message);
    state.phase = BatchExecutionPhase::OUTPUT_RESULTS;
    BatchOutputResults(bind_data, state, output);
}
```

**Step 2: Verify compilation**

Run: `make release 2>&1 | tail -5`
Expected: Build succeeds

---

### Task 6: Implement BatchIntoWzExecute - the main state machine

**Files:**
- Modify: `src/into_wz_function.cpp` (add after batch helpers, before RegisterIntoWzFunction)

**Step 1: Add the execute function**

This is the core logic. It follows the same state-machine pattern as `IntoWzExecute` but processes multiple groups:

```cpp
// ============================================================================
// batch_into_wz: Main execution function
// ============================================================================

static void BatchIntoWzExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<BatchIntoWzBindData>();
    auto &state = data_p.global_state->Cast<BatchIntoWzGlobalState>();

    if (state.phase == BatchExecutionPhase::DONE) {
        output.SetCardinality(0);
        return;
    }

    if (state.phase == BatchExecutionPhase::OUTPUT_RESULTS) {
        BatchOutputResults(bind_data, state, output);
        if (output.size() == 0) {
            state.phase = BatchExecutionPhase::DONE;
        }
        state.progress = 100.0;
        return;
    }

    string error_msg;

    switch (state.phase) {

    // ----------------------------------------------------------------
    // VALIDATE_PARAMS
    // ----------------------------------------------------------------
    case BatchExecutionPhase::VALIDATE_PARAMS: {
        state.total_start = std::chrono::high_resolution_clock::now();

        if (bind_data.source_table.empty()) {
            BatchCleanupAndError(state, bind_data, output, "ERROR", "source_table is required");
            return;
        }
        if (bind_data.lng_kanzlei_konten_rahmen_id <= 0) {
            BatchCleanupAndError(state, bind_data, output, "ERROR",
                "lng_kanzlei_konten_rahmen_id is required and must be a positive integer");
            return;
        }

        BatchAddSuccessResult(bind_data, "PROGRESS:validating", 0, "", 0.0);
        state.phase = BatchExecutionPhase::LOAD_DATA;
        state.progress = 5.0;
        BatchOutputResults(bind_data, state, output);
        return;
    }

    // ----------------------------------------------------------------
    // LOAD_DATA
    // ----------------------------------------------------------------
    case BatchExecutionPhase::LOAD_DATA: {
        if (!BatchLoadSourceData(context, bind_data, error_msg)) {
            BatchCleanupAndError(state, bind_data, output, "ERROR", error_msg);
            return;
        }

        // Find gui_verfahren_id column using alias system
        bind_data.verfahren_id_col = FindColumnWithAliases(bind_data.source_columns, "guiVerfahrenID");
        if (bind_data.verfahren_id_col == DConstants::INVALID_INDEX) {
            // Also try direct name lookup with common variations
            static const vector<string> verfahren_candidates = {
                "guiVerfahrenID", "gui_verfahren_id", "guiverfahrenid", "verfahren_id", "verfahrenid"
            };
            for (const auto &candidate : verfahren_candidates) {
                bind_data.verfahren_id_col = FindColumnIndex(bind_data.source_columns, candidate);
                if (bind_data.verfahren_id_col != DConstants::INVALID_INDEX) break;
            }
        }

        if (bind_data.verfahren_id_col == DConstants::INVALID_INDEX) {
            BatchCleanupAndError(state, bind_data, output, "ERROR",
                "Source table must contain a gui_verfahren_id column "
                "(accepted names: guiVerfahrenID, gui_verfahren_id, guiverfahrenid, verfahren_id, verfahrenid)");
            return;
        }

        // Pre-compute row keys
        bind_data.row_keys.reserve(bind_data.source_rows.size());
        for (const auto &row : bind_data.source_rows) {
            bind_data.row_keys.push_back(BuildRowKey(row));
        }

        // Build column indices
        state.col_idx = PrimanotaColumnIndices::Build(bind_data.source_columns);

        BatchAddSuccessResult(bind_data, "PROGRESS:loaded", static_cast<int64_t>(bind_data.source_rows.size()), "", 0.0);
        state.phase = BatchExecutionPhase::GROUP_DATA;
        state.progress = 20.0;
        BatchOutputResults(bind_data, state, output);
        return;
    }

    // ----------------------------------------------------------------
    // GROUP_DATA
    // ----------------------------------------------------------------
    case BatchExecutionPhase::GROUP_DATA: {
        // Group rows by gui_verfahren_id value
        std::map<string, vector<idx_t>> group_map;

        for (idx_t i = 0; i < bind_data.source_rows.size(); i++) {
            const auto &row = bind_data.source_rows[i];
            if (bind_data.verfahren_id_col >= row.size() || row[bind_data.verfahren_id_col].IsNull()) {
                BatchCleanupAndError(state, bind_data, output, "ERROR",
                    "Row " + std::to_string(i + 1) + " has NULL gui_verfahren_id. All rows must have a valid gui_verfahren_id.");
                return;
            }
            string verfahren_id = row[bind_data.verfahren_id_col].ToString();

            // Validate UUID format
            if (!IsValidUuidFormat(verfahren_id)) {
                BatchCleanupAndError(state, bind_data, output, "ERROR",
                    "Row " + std::to_string(i + 1) + " has invalid gui_verfahren_id format: '" + verfahren_id +
                    "'. Expected UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx");
                return;
            }

            group_map[verfahren_id].push_back(i);
        }

        // Convert to ordered vector
        state.groups.reserve(group_map.size());
        for (auto &[verfahren_id, indices] : group_map) {
            VerfahrenGroup group;
            group.gui_verfahren_id = verfahren_id;
            group.row_indices = std::move(indices);
            state.groups.push_back(std::move(group));
        }

        BatchAddSuccessResult(bind_data,
            "PROGRESS:grouped",
            static_cast<int64_t>(state.groups.size()), "",
            0.0);

        state.phase = BatchExecutionPhase::VALIDATE_GROUPS;
        state.progress = 30.0;
        BatchOutputResults(bind_data, state, output);
        return;
    }

    // ----------------------------------------------------------------
    // VALIDATE_GROUPS
    // ----------------------------------------------------------------
    case BatchExecutionPhase::VALIDATE_GROUPS: {
        // Validate duplicates for all groups combined
        if (!bind_data.skip_duplicate_check) {
            idx_t primanota_id_col = FindColumnIndex(bind_data.source_columns, "guiPrimanotaID");
            if (primanota_id_col != DConstants::INVALID_INDEX) {
                vector<string> all_primanota_ids;
                for (const auto &row : bind_data.source_rows) {
                    if (primanota_id_col < row.size() && !row[primanota_id_col].IsNull()) {
                        all_primanota_ids.push_back(row[primanota_id_col].ToString());
                    }
                }

                auto duplicates = CheckDuplicates(context, bind_data.secret_name, all_primanota_ids);
                if (!duplicates.empty()) {
                    string dup_list;
                    size_t display_count = std::min(duplicates.size(), MAX_DUPLICATES_TO_DISPLAY);
                    for (size_t i = 0; i < display_count; i++) {
                        if (i > 0) dup_list += ", ";
                        dup_list += duplicates[i];
                    }
                    if (duplicates.size() > MAX_DUPLICATES_TO_DISPLAY) {
                        dup_list += " (and " + std::to_string(duplicates.size() - MAX_DUPLICATES_TO_DISPLAY) + " more)";
                    }
                    BatchCleanupAndError(state, bind_data, output, "ERROR",
                        "Duplicate guiPrimanotaID found: " + dup_list +
                        ". " + std::to_string(duplicates.size()) + " records already exist in tblPrimanota.");
                    return;
                }
            }
        }

        // Validate FK constraints for all rows
        if (!bind_data.skip_fk_check) {
            bool fk_valid = ValidateForeignKeys(context, bind_data.secret_name,
                                                  bind_data.source_rows, bind_data.source_columns,
                                                  error_msg);
            if (!fk_valid) {
                // FK validation returns warnings, not hard errors - add as info
                BatchAddSuccessResult(bind_data, "WARNING:fk_validation", 0, "", 0.0);
                bind_data.results.back().error_message = error_msg;
            }
        }

        BatchAddSuccessResult(bind_data, "PROGRESS:validated", 0, "", 0.0);
        state.phase = BatchExecutionPhase::PROCESS_GROUPS;
        state.progress = 40.0;
        state.current_group_idx = 0;
        BatchOutputResults(bind_data, state, output);
        return;
    }

    // ----------------------------------------------------------------
    // PROCESS_GROUPS
    // ----------------------------------------------------------------
    case BatchExecutionPhase::PROCESS_GROUPS: {
        auto &db = DatabaseInstance::GetDatabase(context);

        // Create connections on first entry
        if (!state.txn_conn) {
            state.txn_conn = make_uniq<Connection>(db);
            state.staging_conn = make_uniq<Connection>(db);

            // Begin the all-or-nothing transaction
            if (!ExecuteMssqlStatement(*state.txn_conn, "BEGIN TRANSACTION", error_msg)) {
                BatchCleanupAndError(state, bind_data, output, "ERROR",
                    "Failed to begin transaction: " + error_msg);
                return;
            }
        }

        // Process one group per Execute call to allow progress updates
        if (state.current_group_idx >= state.groups.size()) {
            // All groups processed - commit
            state.phase = BatchExecutionPhase::FINALIZE;
            // Fall through to FINALIZE on next call
            BatchOutputResults(bind_data, state, output);
            return;
        }

        auto &group = state.groups[state.current_group_idx];
        size_t group_num = state.current_group_idx + 1;
        size_t total_groups = state.groups.size();

        BatchAddSuccessResult(bind_data, "GROUP:" + group.gui_verfahren_id,
            static_cast<int64_t>(group.row_indices.size()), "",
            0.0);
        bind_data.results.back().error_message =
            "Processing group " + std::to_string(group_num) + "/" + std::to_string(total_groups);

        auto group_start = std::chrono::high_resolution_clock::now();

        // 1. Pre-compute row keys and primanota IDs for this group
        idx_t primanota_id_col = FindColumnIndex(bind_data.source_columns, "guiPrimanotaID");
        group.primanota_ids = PreComputePrimanotaIds(
            bind_data.source_rows, bind_data.row_keys,
            group.row_indices.size(), group.row_indices.data(),
            primanota_id_col);

        // Check for intra-group duplicate primanota IDs
        {
            std::set<string> seen;
            for (size_t i = 0; i < group.primanota_ids.size(); i++) {
                if (!seen.insert(group.primanota_ids[i]).second) {
                    // Rollback and error
                    BatchCleanupAndError(state, bind_data, output, "ERROR",
                        "Duplicate primanota ID '" + group.primanota_ids[i] +
                        "' within group " + group.gui_verfahren_id);
                    return;
                }
            }
        }

        // 2. Compute date range for this group
        {
            string min_date, max_date;
            idx_t date_col = FindColumnWithAliases(bind_data.source_columns, "dtmBelegDatum");
            if (date_col != DConstants::INVALID_INDEX) {
                for (const auto &row_idx : group.row_indices) {
                    const auto &row = bind_data.source_rows[row_idx];
                    if (date_col < row.size() && !row[date_col].IsNull()) {
                        string date_str = row[date_col].ToString();
                        if (date_str.length() >= 10) date_str = date_str.substr(0, 10);
                        if (min_date.empty() || date_str < min_date) min_date = date_str;
                        if (max_date.empty() || date_str > max_date) max_date = date_str;
                    }
                }
            }
            if (min_date.empty()) min_date = GetCurrentMonthStart();
            if (max_date.empty()) max_date = GetCurrentDate();
            group.date_from = min_date;
            group.date_to = max_date;
        }

        // 3. Generate Vorlauf UUID
        if (bind_data.generate_vorlauf_id) {
            // Build combined key from this group's row keys
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

        // 4. Derive bezeichnung from MSSQL (fetch AZ prefix)
        if (!DeriveBezeichnungFromMssql(*state.txn_conn, bind_data.secret_name,
                                          group.gui_verfahren_id,
                                          group.date_from, group.date_to,
                                          group.bezeichnung, error_msg)) {
            BatchCleanupAndError(state, bind_data, output, "ERROR",
                "Failed to derive bezeichnung for group " + group.gui_verfahren_id + ": " + error_msg);
            return;
        }
        if (group.bezeichnung.empty()) {
            group.bezeichnung = DeriveVorlaufBezeichnung(group.date_from, group.date_to);
        }

        // 5. Insert or update Vorlauf record
        {
            bool exists = false;
            if (!VorlaufExists(*state.txn_conn, bind_data.secret_name, group.vorlauf_id, exists, error_msg)) {
                BatchCleanupAndError(state, bind_data, output, "ERROR", error_msg);
                return;
            }

            double vorlauf_start_t = std::chrono::duration<double>(
                std::chrono::high_resolution_clock::now() - group_start).count();

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

        // 6. Create staging table, populate, transfer, drop
        {
            if (!CreateStagingTable(*state.staging_conn, error_msg)) {
                BatchCleanupAndError(state, bind_data, output, "ERROR", error_msg);
                return;
            }

            string timestamp = GetCurrentTimestamp();
            string date_fallback = group.date_to;
            string vorlauf_datum_bis = group.date_to + " 00:00:00";

            if (!BatchPopulateStagingTable(*state.staging_conn, bind_data,
                                            group.row_indices, group.primanota_ids,
                                            group.vorlauf_id, group.gui_verfahren_id,
                                            bind_data.str_angelegt, vorlauf_datum_bis,
                                            timestamp, date_fallback,
                                            state.col_idx, error_msg)) {
                DropStagingTable(*state.staging_conn);
                BatchCleanupAndError(state, bind_data, output, "ERROR", error_msg);
                return;
            }

            // Transfer via INSERT (not BCP - must stay in transaction)
            auto transfer_start = std::chrono::high_resolution_clock::now();
            if (!BulkTransferPrimanota(*state.txn_conn, *state.staging_conn,
                                        bind_data.secret_name, error_msg)) {
                DropStagingTable(*state.staging_conn);
                BatchCleanupAndError(state, bind_data, output, "ERROR",
                    "Failed to transfer primanota for group " + group.gui_verfahren_id + ": " + error_msg);
                return;
            }
            double transfer_dur = std::chrono::duration<double>(
                std::chrono::high_resolution_clock::now() - transfer_start).count();

            DropStagingTable(*state.staging_conn);

            BatchAddSuccessResult(bind_data, "tblPrimanota",
                static_cast<int64_t>(group.row_indices.size()),
                group.vorlauf_id, transfer_dur);
        }

        // Advance to next group
        state.current_group_idx++;
        double group_progress = 40.0 + (55.0 * static_cast<double>(state.current_group_idx) / static_cast<double>(state.groups.size()));
        state.progress = group_progress;

        BatchOutputResults(bind_data, state, output);
        return;
    }

    // ----------------------------------------------------------------
    // FINALIZE
    // ----------------------------------------------------------------
    case BatchExecutionPhase::FINALIZE: {
        // Commit the all-or-nothing transaction
        if (state.txn_conn) {
            if (!ExecuteMssqlStatement(*state.txn_conn, "COMMIT", error_msg)) {
                BatchCleanupAndError(state, bind_data, output, "ERROR",
                    "Failed to commit transaction: " + error_msg);
                return;
            }
            state.txn_conn.reset();
        }
        if (state.staging_conn) {
            state.staging_conn.reset();
        }

        // Total timing
        auto total_end = std::chrono::high_resolution_clock::now();
        double total_dur = std::chrono::duration<double>(total_end - state.total_start).count();

        int64_t total_rows = 0;
        for (const auto &group : state.groups) {
            total_rows += static_cast<int64_t>(group.row_indices.size());
        }

        BatchAddSuccessResult(bind_data, "TOTAL", total_rows, "", total_dur);
        bind_data.results.back().error_message =
            std::to_string(state.groups.size()) + " groups processed successfully";

        state.phase = BatchExecutionPhase::OUTPUT_RESULTS;
        state.progress = 100.0;
        BatchOutputResults(bind_data, state, output);
        return;
    }

    default:
        output.SetCardinality(0);
        return;
    }
}
```

**Step 2: Verify compilation**

Run: `make release 2>&1 | tail -5`
Expected: Build succeeds

---

### Task 7: Register batch_into_wz function

**Files:**
- Modify: `src/into_wz_function.cpp:2206-2230` (add RegisterBatchIntoWzFunction)
- Modify: `src/include/wz_extension.hpp:63` (add declaration)
- Modify: `src/wz_extension.cpp:20-26` (call registration)

**Step 1: Add RegisterBatchIntoWzFunction in into_wz_function.cpp**

Add after the existing `RegisterIntoWzFunction` function (before the closing `} // namespace duckdb`):

```cpp
void RegisterBatchIntoWzFunction(DatabaseInstance &db) {
    TableFunction batch_func("batch_into_wz", {}, BatchIntoWzExecute, BatchIntoWzBind, BatchIntoWzInitGlobal);

    batch_func.table_scan_progress = BatchIntoWzProgress;

    // Same parameters as into_wz minus gui_verfahren_id
    batch_func.named_parameters["secret"] = LogicalType::VARCHAR;
    batch_func.named_parameters["source_table"] = LogicalType::VARCHAR;
    batch_func.named_parameters["lng_kanzlei_konten_rahmen_id"] = LogicalType::BIGINT;
    batch_func.named_parameters["str_angelegt"] = LogicalType::VARCHAR;
    batch_func.named_parameters["generate_vorlauf_id"] = LogicalType::BOOLEAN;
    batch_func.named_parameters["monatsvorlauf"] = LogicalType::BOOLEAN;
    batch_func.named_parameters["skip_duplicate_check"] = LogicalType::BOOLEAN;
    batch_func.named_parameters["skip_fk_check"] = LogicalType::BOOLEAN;

    Connection con(db);
    con.BeginTransaction();
    auto &catalog = Catalog::GetSystemCatalog(db);
    CreateTableFunctionInfo info(batch_func);
    catalog.CreateFunction(*con.context, info);
    con.Commit();
}
```

**Step 2: Add declaration to wz_extension.hpp**

Add after line 61 (`void RegisterIntoWzFunction(DatabaseInstance &db);`):

```cpp
void RegisterBatchIntoWzFunction(DatabaseInstance &db);
```

**Step 3: Call registration in wz_extension.cpp**

In `WzExtension::Load()`, add after `RegisterIntoWzFunction(db);`:

```cpp
RegisterBatchIntoWzFunction(db);
```

**Step 4: Build and verify**

Run: `make release 2>&1 | tail -5`
Expected: Build succeeds

**Step 5: Verify function is registered**

Run: `./build/release/duckdb -unsigned -c "LOAD 'build/release/extension/wz/wz.duckdb_extension'; SELECT function_name FROM duckdb_functions() WHERE function_name IN ('into_wz', 'batch_into_wz');"`

Expected: Both `into_wz` and `batch_into_wz` appear.

**Step 6: Commit**

```bash
git add src/into_wz_function.cpp src/include/wz_extension.hpp src/wz_extension.cpp
git commit -m "feat: add batch_into_wz function for multi-verfahren batch imports"
```

---

### Task 8: Copy built extension to dist/ and verify

**Files:**
- Copy: `build/release/extension/wz/wz.duckdb_extension` -> `dist/wz.duckdb_extension`

**Step 1: Copy extension**

```bash
cp build/release/extension/wz/wz.duckdb_extension dist/wz.duckdb_extension
```

**Step 2: Verify with DuckDB CLI**

```bash
./build/release/duckdb -unsigned -c "
LOAD 'dist/wz.duckdb_extension';
SELECT function_name FROM duckdb_functions() WHERE function_name = 'batch_into_wz';
"
```

Expected: `batch_into_wz` appears in output.

---

### Task 9: Update README.md

**Files:**
- Modify: `README.md`

**Step 1: Add batch_into_wz documentation**

Add a new section documenting the `batch_into_wz()` function usage:

```markdown
### batch_into_wz()

Processes multiple gui_verfahren_id values from source data in a single call. Groups rows by their `gui_verfahren_id` column, creates a Vorlauf for each group, and inserts the corresponding Primanota rows within a single all-or-nothing MSSQL transaction.

```sql
SELECT * FROM batch_into_wz(
    secret := 'ms',
    source_table := 'winsolvenz_ready',
    lng_kanzlei_konten_rahmen_id := 55,
    str_angelegt := 'rl'
);
```

The source table must contain a `gui_verfahren_id` column (accepted names: guiVerfahrenID, gui_verfahren_id, guiverfahrenid, verfahren_id, verfahrenid).

**Parameters:** Same as `into_wz()` except `gui_verfahren_id` is read from the source data, not passed as a parameter.
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add batch_into_wz usage to README"
```

---

### Task 10: Final commit and push

**Step 1: Final verification build**

```bash
make release 2>&1 | tail -5
```

**Step 2: Push**

```bash
git push
```
