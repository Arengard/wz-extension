# Phase 2: Transaction Fix - Research

**Researched:** 2026-02-06
**Domain:** DuckDB C++ transaction API, MetaTransaction system, attached MSSQL database transaction propagation
**Confidence:** HIGH

## Summary

The current `into_wz` function issues `BEGIN TRANSACTION`, `COMMIT`, and `ROLLBACK` as raw SQL strings through `conn.Query()`. While this technically works (DuckDB's `Connection::BeginTransaction()` itself does `Query("BEGIN TRANSACTION")` internally), the error handling flow is fragile: when a DuckDB query fails, DuckDB may invalidate the MetaTransaction, causing subsequent explicit `ROLLBACK` calls to hit "cannot rollback - no transaction is active".

DuckDB uses a **MetaTransaction** system that manages transactions across multiple attached databases. When you call `BEGIN TRANSACTION` on a DuckDB connection, DuckDB creates a MetaTransaction and sets `auto_commit = false`. When a query touches an attached MSSQL database, the MetaTransaction lazily starts an MSSQLTransaction through the mssql-extension's `MSSQLTransactionManager`. On `COMMIT`, the MetaTransaction iterates all participating databases and commits each one. On `ROLLBACK`, it does the same with rollback. The mssql-extension handles the actual `COMMIT TRANSACTION` / `ROLLBACK TRANSACTION` SQL Server commands via its pinned TDS connection.

**Primary recommendation:** Replace raw SQL transaction strings with DuckDB's C++ `Connection` API (`BeginTransaction()`, `Commit()`, `Rollback()`) and wrap the entire insert flow in proper try/catch with structured error handling. The C++ API does the same thing but provides cleaner exception-based control flow that works correctly with DuckDB's transaction lifecycle.

## Standard Stack

This phase does not introduce new libraries. It refactors existing code to use DuckDB's built-in C++ API correctly.

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| DuckDB C++ API | v1.4.4 | `Connection::BeginTransaction/Commit/Rollback` | These are the official methods for transaction control in DuckDB C++ extensions |
| hugr-lab/mssql-extension | latest | MSSQL attached database with transaction propagation | Already in use; supports `BEGIN/COMMIT/ROLLBACK` with connection pinning |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| C++ API (`BeginTransaction()`) | Raw SQL strings (`Query("BEGIN TRANSACTION")`) | Raw SQL is what the code does today and what `BeginTransaction()` does internally -- but the C++ API provides cleaner exception semantics and matches DuckDB community patterns |
| DuckDB MetaTransaction | Direct MSSQL transaction via `mssql_query()` | Would bypass DuckDB's transaction system entirely; loses integration with DuckDB's MetaTransaction that correctly propagates commit/rollback to the mssql-extension |

## Architecture Patterns

### Pattern 1: Connection-scoped Transaction with C++ API
**What:** Create a single `Connection` object, call `BeginTransaction()`, perform all inserts through that connection, then `Commit()` or `Rollback()`.
**When to use:** Whenever multiple SQL statements must be atomic within a single attached database.
**Why it works with MSSQL:** DuckDB's `MetaTransaction` lazily opens an `MSSQLTransaction` when the first query touches the MSSQL database. The `MSSQLTransactionManager::CommitTransaction()` sends `COMMIT TRANSACTION` to SQL Server. `RollbackTransaction()` sends `ROLLBACK TRANSACTION`. The mssql-extension's connection pinning ensures all queries in the transaction use the same SQL Server connection.

```cpp
// Source: DuckDB connection.cpp (lines 331-350) and meta_transaction.cpp
auto &db = DatabaseInstance::GetDatabase(context);
Connection txn_conn(db);

try {
    txn_conn.BeginTransaction();  // Sets auto_commit=false, creates MetaTransaction

    // All queries through txn_conn now participate in the same transaction
    auto result1 = txn_conn.Query(insert_vorlauf_sql);
    if (result1->HasError()) {
        throw std::runtime_error(result1->GetError());
    }

    auto result2 = txn_conn.Query(insert_primanota_sql);
    if (result2->HasError()) {
        throw std::runtime_error(result2->GetError());
    }

    txn_conn.Commit();  // Propagates to MSSQLTransactionManager::CommitTransaction
} catch (...) {
    // Rollback if transaction is still active
    if (txn_conn.HasActiveTransaction()) {
        txn_conn.Rollback();
    }
    // re-throw or handle error
}
```

### Pattern 2: Check HasActiveTransaction Before Rollback
**What:** Before calling `Rollback()`, check `HasActiveTransaction()` to avoid the "cannot rollback - no transaction is active" error.
**When to use:** Always, in the error/catch path.
**Why:** DuckDB may auto-rollback invalidated transactions. A failed query can invalidate the MetaTransaction, and `PhysicalTransaction` converts `COMMIT` to `ROLLBACK` for invalidated transactions. After auto-rollback, the transaction no longer exists.

```cpp
// Source: DuckDB transaction_context.cpp (lines 70-71)
// "failed to rollback: no transaction active" thrown when current_transaction is null
if (txn_conn.HasActiveTransaction()) {
    txn_conn.Rollback();
}
```

### Pattern 3: Error Message Enrichment with Table Name and Row Number
**What:** Wrap each INSERT operation with context (table name, batch row range) so error messages are actionable.
**When to use:** For every SQL execution that could fail.

```cpp
if (result->HasError()) {
    string enriched_error = "Failed to insert into " + table_name +
        " (rows " + std::to_string(batch_start) + "-" + std::to_string(batch_end - 1) +
        "): " + result->GetError();
    throw std::runtime_error(enriched_error);
}
```

### Anti-Patterns to Avoid
- **Raw SQL transaction strings without guarding rollback:** Current code sends `"ROLLBACK"` as SQL which throws if DuckDB already rolled back the transaction. Always use `HasActiveTransaction()` guard or catch the rollback exception.
- **Creating a new Connection per SQL statement:** Each `Connection` has its own `TransactionContext`. Creating multiple connections means each insert runs in its own auto-commit transaction, defeating atomicity. The current code correctly uses a single `txn_conn` for all operations -- this must be preserved.
- **Ignoring MetaTransaction's single-writer constraint:** DuckDB only allows writing to ONE attached database per transaction. Since this extension only writes to the MSSQL database, this is fine. But never try to also write to a local DuckDB table within the same transaction.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Transaction lifecycle | Custom transaction state tracking | `Connection::BeginTransaction/Commit/Rollback` + `HasActiveTransaction()` | DuckDB's MetaTransaction already handles all the complexity of propagating transactions to attached databases |
| MSSQL transaction propagation | Direct SQL Server `BEGIN TRANSACTION` via `mssql_query()` | DuckDB's attached database transaction system | The mssql-extension's `MSSQLTransactionManager` handles connection pinning, transaction descriptors, and commit/rollback propagation correctly |
| Error wrapping | Ad-hoc string concatenation in every call site | A single helper function that executes SQL and wraps errors with context | Keeps error formatting consistent and reduces duplication |

**Key insight:** DuckDB's MetaTransaction system already does the hard work of propagating transactions to attached databases. The mssql-extension already implements `MSSQLTransactionManager::CommitTransaction()` which sends `COMMIT TRANSACTION` to SQL Server, and `RollbackTransaction()` which sends `ROLLBACK TRANSACTION`. The wz-extension just needs to use DuckDB's standard C++ transaction API correctly.

## Common Pitfalls

### Pitfall 1: "cannot rollback - no transaction is active"
**What goes wrong:** After an INSERT fails, calling `ROLLBACK` throws because DuckDB has already invalidated/rolled back the MetaTransaction.
**Why it happens:** When a query fails within a transaction, DuckDB marks the `MetaTransaction` as invalidated via `ValidChecker`. In `PhysicalTransaction::GetData()`, if the transaction is invalidated and `COMMIT` is attempted, it converts to `ROLLBACK`. But if the error was severe enough, the transaction may already be auto-cleaned by `TransactionContext` destructor or connection cleanup. Also, the `Rollback()` method in `TransactionContext` (line 71) throws `TransactionException("failed to rollback: no transaction active")` when `current_transaction` is null.
**How to avoid:** Always check `HasActiveTransaction()` before calling `Rollback()`, or wrap `Rollback()` in a try/catch that swallows the "no transaction active" exception.
**Warning signs:** The error "cannot rollback - no transaction is active" appearing in production.

### Pitfall 2: Auto-commit consuming transactions
**What goes wrong:** Each `conn.Query()` call auto-begins and auto-commits a transaction when `auto_commit` is true (default). Without an explicit `BeginTransaction()`, each INSERT would be its own atomic unit -- no all-or-nothing guarantee.
**Why it happens:** DuckDB defaults to `auto_commit = true` on new connections. Each query goes through: begin auto-transaction -> execute -> commit. The `PhysicalTransaction` handler for `BEGIN TRANSACTION` sets `auto_commit = false` to prevent this.
**How to avoid:** Call `BeginTransaction()` (or send `BEGIN TRANSACTION` SQL) before the first INSERT. The current code already does this correctly.
**Warning signs:** Partial inserts surviving after errors (tblVorlauf inserted but tblPrimanota not, or vice versa).

### Pitfall 3: Using a different Connection for pre-checks vs inserts
**What goes wrong:** The duplicate check (`CheckDuplicates`) and source data loading (`LoadSourceData`) use separate connections, which is correct and intentional. But if these were mixed into the transaction connection, they could interfere with the transaction state.
**Why it happens:** DuckDB Connection objects have per-connection transaction state. Read operations on a separate connection don't affect the write transaction.
**How to avoid:** Keep read-only pre-checks (duplicate detection, source loading) on separate connections. Only use the transaction connection for the actual INSERT operations. The current code already does this correctly.
**Warning signs:** Deadlocks or unexpected transaction failures during pre-checks.

### Pitfall 4: Single-writer constraint across databases
**What goes wrong:** DuckDB's MetaTransaction only allows writing to ONE attached database per transaction. Attempting to write to both a local DuckDB table and the MSSQL database in the same transaction throws "a single transaction can only write to a single attached database."
**Why it happens:** `MetaTransaction::ModifyDatabase()` (meta_transaction.cpp line 219-242) checks if `modified_database` is already set. If a different database tries to write, it throws.
**How to avoid:** Only write to the MSSQL attached database within the transaction. The current code only writes to `mssql_conn.dbo.tblVorlauf` and `mssql_conn.dbo.tblPrimanota`, which are both in the same attached database. This is fine.
**Warning signs:** "Attempting to write to database X in a transaction that has already modified database Y" error.

### Pitfall 5: Connection object lifetime
**What goes wrong:** The `Connection` object is destroyed before `Commit()` or `Rollback()` is called. The `TransactionContext` destructor calls `Rollback()` if a transaction is active, but errors during destructor rollback are swallowed.
**Why it happens:** If the function returns early (via error path) without explicitly rolling back, the Connection destructor handles cleanup. But this is implicit and harder to debug.
**How to avoid:** Ensure every exit path either commits or explicitly rolls back. The current refactored code should have a clear try/catch structure.
**Warning signs:** Silent data loss or incomplete rollback messages.

## Code Examples

### Current Code (Broken Pattern)
```cpp
// Source: into_wz_function.cpp lines 907-951 (current code)
// Problem: Uses raw SQL strings for transaction control

Connection txn_conn(txn_db);

// This works but error handling is fragile:
if (!ExecuteMssqlStatementWithConn(txn_conn, "BEGIN TRANSACTION", error_msg)) {
    // error...
}

// Insert Vorlauf...
if (!InsertVorlauf(...)) {
    string rollback_err;
    ExecuteMssqlStatementWithConn(txn_conn, "ROLLBACK", rollback_err);  // MAY FAIL
    // ...
}

// Insert Primanota...
if (!InsertPrimanota(...)) {
    string rollback_err;
    ExecuteMssqlStatementWithConn(txn_conn, "ROLLBACK", rollback_err);  // MAY FAIL
    // ...
}

if (!ExecuteMssqlStatementWithConn(txn_conn, "COMMIT", error_msg)) {
    // error...
}
```

### Recommended Code (Fixed Pattern)
```cpp
// Source: DuckDB connection.hpp (lines 173-178)
// Pattern: Use C++ API with proper error handling

auto &db = DatabaseInstance::GetDatabase(context);
Connection txn_conn(db);

try {
    txn_conn.BeginTransaction();

    // Insert Vorlauf
    string vorlauf_sql = BuildVorlaufInsertSQL(...);
    auto vorlauf_result = txn_conn.Query(vorlauf_sql);
    if (vorlauf_result->HasError()) {
        throw std::runtime_error("Failed to insert into tblVorlauf: " +
                                 vorlauf_result->GetError());
    }

    // Insert Primanota in batches
    for (size_t batch_start = 0; batch_start < rows.size(); batch_start += BATCH_SIZE) {
        size_t batch_end = std::min(batch_start + BATCH_SIZE, rows.size());
        string primanota_sql = BuildPrimanotaInsertSQL(...);
        auto primanota_result = txn_conn.Query(primanota_sql);
        if (primanota_result->HasError()) {
            throw std::runtime_error(
                "Failed to insert into tblPrimanota (rows " +
                std::to_string(batch_start) + "-" +
                std::to_string(batch_end - 1) + "): " +
                primanota_result->GetError());
        }
    }

    txn_conn.Commit();

} catch (const std::exception &e) {
    // Safe rollback: only if transaction is still active
    if (txn_conn.HasActiveTransaction()) {
        try {
            txn_conn.Rollback();
        } catch (...) {
            // Rollback itself failed -- transaction was already cleaned up
        }
    }
    // Report error with full context
    AddErrorResult(bind_data, "ERROR", e.what(), vorlauf_id);
}
```

### Error Message Format
```cpp
// TXN-02: Actionable error messages with table name, row number, MSSQL error text
// Format: "Failed to insert into {table} (rows {start}-{end}): {mssql_error}"

// For tblVorlauf (single row):
"Failed to insert into tblVorlauf: [MSSQL error text]"

// For tblPrimanota (batched):
"Failed to insert into tblPrimanota (rows 0-99): [MSSQL error text]"
"Failed to insert into tblPrimanota (rows 100-199): [MSSQL error text]"
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Raw SQL `"BEGIN TRANSACTION"` / `"COMMIT"` / `"ROLLBACK"` strings | `Connection::BeginTransaction()` / `Commit()` / `Rollback()` C++ API | Always available in DuckDB | Cleaner exception flow, matches how DuckDB's own code handles transactions |
| Per-statement error checking with manual rollback | try/catch block wrapping entire transaction | C++ best practice | Ensures rollback happens on ANY error, not just anticipated ones |
| No transaction-active check before rollback | `HasActiveTransaction()` guard | Available since DuckDB MetaTransaction introduction | Prevents "cannot rollback" errors |

## Implementation Analysis

### What Needs to Change

1. **Replace raw SQL transaction strings** in `IntoWzExecute()` (lines 907-951 of `into_wz_function.cpp`):
   - `ExecuteMssqlStatementWithConn(txn_conn, "BEGIN TRANSACTION", ...)` -> `txn_conn.BeginTransaction()`
   - `ExecuteMssqlStatementWithConn(txn_conn, "COMMIT", ...)` -> `txn_conn.Commit()`
   - `ExecuteMssqlStatementWithConn(txn_conn, "ROLLBACK", ...)` -> `txn_conn.Rollback()` guarded by `HasActiveTransaction()`

2. **Restructure error flow** from if/else chains to try/catch:
   - Current code has multiple error exit paths, each manually calling ROLLBACK
   - Refactor to a single try/catch block around the entire insert sequence
   - Catch block handles rollback + error reporting

3. **Improve error messages** (TXN-02):
   - Current `InsertVorlauf()` already prefixes "Failed to insert Vorlauf: "
   - Current `InsertPrimanota()` already includes row range in error
   - Both already pass through the underlying MSSQL error text
   - Need to ensure table name is consistently `tblVorlauf` / `tblPrimanota` (not just "Vorlauf")
   - The error messages from the current code are already mostly compliant; verify and standardize

4. **Refactor helper functions** to throw exceptions instead of returning bool:
   - `InsertVorlauf()` currently returns `false` with error in out-param
   - `InsertPrimanota()` currently returns `false` with error in out-param
   - Change to throw `std::runtime_error` with enriched message
   - Or keep bool-return pattern but wrap in try/catch at call site

### What Should NOT Change

- `LoadSourceData()` and `ValidateDuplicates()` use separate connections -- this is correct
- `CheckDuplicates()` creates its own connection -- this is correct (avoids deadlock)
- The batch size of 100 for Primanota inserts -- no change needed
- The `BuildVorlaufInsertSQL()` and `BuildPrimanotaInsertSQL()` functions -- no change needed
- The output format (`AddSuccessResult` / `AddErrorResult`) -- no change needed

### Scope of Change

This is a **focused refactoring** of the `IntoWzExecute()` function (lines 815-958) and minor adjustments to `InsertVorlauf()` and `InsertPrimanota()`. No changes needed to:
- Bind function
- Column mappings
- SQL generation functions
- Helper utilities
- Extension registration

Estimated: **1 plan, ~20-30 lines changed** in `into_wz_function.cpp`.

## Open Questions

1. **Exception behavior of `Connection::BeginTransaction()`**
   - What we know: `BeginTransaction()` calls `Query("BEGIN TRANSACTION")` and throws if result has error (connection.cpp line 331-336). This is functionally identical to the current raw SQL approach but with exception semantics.
   - What's unclear: Whether there are edge cases where `BeginTransaction()` behaves differently from `Query("BEGIN TRANSACTION")` for attached databases.
   - Recommendation: Use `BeginTransaction()` -- it's the standard API and the implementation is literally `Query("BEGIN TRANSACTION")` with error-throwing.

2. **MSSQL extension's transaction descriptor handling**
   - What we know: The mssql-extension tracks an 8-byte "transaction descriptor" from SQL Server, used for distributed transaction coordination. The `SetTransactionDescriptor()` method validates and logs this.
   - What's unclear: Whether this affects wz-extension behavior in any way.
   - Recommendation: Not relevant. The wz-extension does not directly interact with the mssql-extension's transaction internals. DuckDB's MetaTransaction handles the propagation transparently.

## Sources

### Primary (HIGH confidence)
- DuckDB source code (`duckdb/src/main/connection.cpp`) - `BeginTransaction()`, `Commit()`, `Rollback()` implementations verified: they call `Query("BEGIN TRANSACTION")` etc. (lines 331-350)
- DuckDB source code (`duckdb/src/transaction/transaction_context.cpp`) - Transaction lifecycle, auto-commit behavior, "no transaction active" error conditions (lines 25-97)
- DuckDB source code (`duckdb/src/transaction/meta_transaction.cpp`) - MetaTransaction commit/rollback propagation to attached databases (lines 108-175)
- DuckDB source code (`duckdb/src/execution/operator/helper/physical_transaction.cpp`) - How `BEGIN TRANSACTION` SQL is executed, auto-commit toggling, invalidated transaction handling (lines 14-77)
- DuckDB source code (`duckdb/src/include/duckdb/main/connection.hpp`) - `BeginTransaction()`, `Commit()`, `Rollback()`, `HasActiveTransaction()`, `SetAutoCommit()`, `IsAutoCommit()` declarations (lines 173-178)
- WZ extension source code (`src/into_wz_function.cpp`) - Current transaction handling implementation (lines 907-951)

### Secondary (MEDIUM confidence)
- [hugr-lab/mssql-extension GitHub](https://github.com/hugr-lab/mssql-extension) - MSSQLTransactionManager implementation with connection pinning, `CommitTransaction()` sends "COMMIT TRANSACTION", `RollbackTransaction()` sends "ROLLBACK TRANSACTION"
- [DuckDB Multi-Database Support blog post](https://duckdb.org/2024/01/26/multi-database-support-in-duckdb) - Confirms single-writer constraint per transaction, lazy transaction start for attached databases
- [DuckDB Transaction Management docs](https://duckdb.org/docs/stable/sql/statements/transactions) - Official transaction SQL documentation

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - Verified directly from DuckDB source code in the repository
- Architecture: HIGH - Transaction flow traced through DuckDB source: `Connection::BeginTransaction()` -> `PhysicalTransaction` -> `TransactionContext::BeginTransaction()` -> `MetaTransaction` -> `MSSQLTransactionManager::StartTransaction()`
- Pitfalls: HIGH - The "cannot rollback" error traced to exact source line (`TransactionContext::Rollback()` line 71: `throw TransactionException("failed to rollback: no transaction active")`)

**Research date:** 2026-02-06
**Valid until:** 2026-03-06 (stable -- DuckDB transaction API is well-established)
