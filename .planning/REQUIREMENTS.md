# Requirements: WZ Extension

**Defined:** 2026-02-05
**Core Value:** Reliable, transactional import of accounting data from DuckDB into MSSQL WZ tables

## v1 Requirements

### Transaction Handling

- [ ] **TXN-01**: All inserts (tblVorlauf + tblPrimanota) succeed or nothing is committed — all-or-nothing guarantee with attached MSSQL databases
- [ ] **TXN-02**: When an insert fails, error message clearly states which table, which row, and the actual MSSQL error — no cryptic DuckDB internal errors like "cannot rollback - no transaction is active"

### Code Cleanup

- [ ] **CLN-01**: Delete primanota_mapper.cpp and vorlauf_builder.cpp — unused earlier implementations superseded by into_wz_function.cpp
- [ ] **CLN-02**: Consolidate duplicated helper functions (FindColumnIndex, timestamp generation) into wz_extension.hpp or a shared utils file
- [ ] **CLN-03**: Replace goto-based error flow in IntoWzExecute with structured control flow (early returns or result pattern)

### Constraint Validation

- [ ] **CST-01**: Before inserting, validate that foreign key column values in source data exist in their referenced MSSQL tables — fail early with clear message listing violating values

## v2 Requirements

### Constraint Validation Enhancements

- **CST-02**: Clear violation messages listing which specific values violate which FK constraints
- **CST-03**: Suggest closest valid values when FK violations are found

## Out of Scope

| Feature | Reason |
|---------|--------|
| New table functions | This milestone is about fixing existing code, not adding features |
| Schema DDL operations | Extension only inserts data into existing tables |
| Non-MSSQL targets | Extension is specifically for WZ MSSQL databases |
| Refactoring into separate files | User chose to delete dead files, keep logic consolidated in into_wz_function.cpp |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| TXN-01 | Phase 2 | Pending |
| TXN-02 | Phase 2 | Pending |
| CLN-01 | Phase 1 | Pending |
| CLN-02 | Phase 1 | Pending |
| CLN-03 | Phase 1 | Pending |
| CST-01 | Phase 3 | Pending |

**Coverage:**
- v1 requirements: 6 total
- Mapped to phases: 6
- Unmapped: 0

---
*Requirements defined: 2026-02-05*
*Last updated: 2026-02-05 after roadmap creation*
