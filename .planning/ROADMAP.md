# Roadmap: WZ Extension

## Overview

This roadmap delivers reliable, transactional accounting data imports by first cleaning up the codebase (removing dead code, consolidating helpers, eliminating gotos), then fixing the broken transaction handling that is the project's core value problem, and finally adding FK constraint validation so bad data fails early instead of mid-insert.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Code Cleanup** - Remove dead code, consolidate helpers, eliminate gotos
- [x] **Phase 2: Transaction Fix** - All-or-nothing insert guarantee with clear error messages
- [ ] **Phase 3: Constraint Validation** - Pre-insert FK validation against MSSQL reference tables

## Phase Details

### Phase 1: Code Cleanup
**Goal**: The codebase contains only the code that matters -- no dead files, no duplicated helpers, no goto spaghetti
**Depends on**: Nothing (first phase)
**Requirements**: CLN-01, CLN-02, CLN-03
**Success Criteria** (what must be TRUE):
  1. primanota_mapper.cpp and vorlauf_builder.cpp no longer exist in the project -- the build succeeds without them
  2. FindColumnIndex and timestamp generation each exist in exactly one place, used by all callers
  3. into_wz_function.cpp contains zero goto statements -- error flow uses structured control (early returns or result pattern)
**Plans**: 2 plans

Plans:
- [x] 01-01-PLAN.md -- Remove dead files and consolidate helpers into wz_utils.hpp
- [x] 01-02-PLAN.md -- Replace goto-based error flow with early returns and extracted sub-functions, fix guiVorlaufID bug

### Phase 2: Transaction Fix
**Goal**: When `into_wz` runs, either all inserts (tblVorlauf + tblPrimanota) commit or none do -- and failures produce actionable error messages
**Depends on**: Phase 1
**Requirements**: TXN-01, TXN-02
**Success Criteria** (what must be TRUE):
  1. A failing insert at row N of tblPrimanota leaves zero new rows in both tblVorlauf and tblPrimanota -- nothing partially committed
  2. When an insert fails, the error message includes the target table name, the row number, and the underlying MSSQL error text
  3. The "cannot rollback - no transaction is active" error never appears -- transaction lifecycle works with attached MSSQL databases
**Plans**: 1 plan

Plans:
- [x] 02-01-PLAN.md -- Replace raw SQL transaction strings with C++ API and standardize error messages

### Phase 3: Constraint Validation
**Goal**: `into_wz` validates foreign key references against MSSQL before attempting any inserts, failing early with clear violation details
**Depends on**: Phase 2
**Requirements**: CST-01
**Success Criteria** (what must be TRUE):
  1. Source data with an invalid FK value (e.g., nonexistent Konto) is rejected before any INSERT executes -- zero rows written
  2. The error message lists which FK column(s) failed and which specific values do not exist in the referenced MSSQL table
**Plans**: TBD

Plans:
- [ ] 03-01: Implement FK constraint validation against MSSQL reference tables

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Code Cleanup | 2/2 | Complete | 2026-02-05 |
| 2. Transaction Fix | 1/1 | Complete | 2026-02-06 |
| 3. Constraint Validation | 0/1 | Not started | - |
