# Phase 1: Code Cleanup - Context

**Gathered:** 2026-02-05
**Status:** Ready for planning

<domain>
## Phase Boundary

Remove dead code, consolidate duplicated helpers, and eliminate goto-based control flow in the WZ extension codebase. The build must succeed after dead files are removed, shared helpers exist in exactly one place, and into_wz_function.cpp contains zero goto statements.

</domain>

<decisions>
## Implementation Decisions

### Dead code criteria
- Remove the two named dead files (primanota_mapper.cpp, vorlauf_builder.cpp) AND any clearly unused functions/variables in remaining files
- Do NOT remove commented-out code blocks or unused includes — scope is files and functions only
- When removing dead files breaks includes or references, clean up the calling code too — don't leave broken references

### Helper consolidation
- Consolidate FindColumnIndex and timestamp generation into a separate utility header: `wz_utils.hpp`
- All callers reference the single shared location

### Goto replacement strategy
- Replace gotos with early returns — check-and-return-error at each step
- Resource cleanup uses RAII / smart pointers so cleanup is automatic on scope exit
- While restructuring, also improve error messages to be more specific and actionable (not a pure refactor)
- Extract logical sub-functions from the long function (e.g., InsertVorlauf, InsertPrimanota) — break by concern

### Code style
- PascalCase for new/extracted function names (matches existing FindColumnIndex convention)
- Extracted functions stay in into_wz_function.cpp — one file, not split by concern
- Error strings use plain descriptive format: "Failed to insert Vorlauf row 5: <MSSQL error>" — no prefix convention
- Brief doc comment on each extracted function (purpose, params) — no inline comment cleanup

### Claude's Discretion
- Exact RAII wrapper design for resources
- Which specific functions to extract (beyond the obvious InsertVorlauf/InsertPrimanota)
- Order of operations within the cleanup

</decisions>

<specifics>
## Specific Ideas

- Utility header should be named `wz_utils.hpp` specifically
- Error messages should include table name and row number for insert failures
- Sub-functions should be named by concern (InsertVorlauf, InsertPrimanota pattern)

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 01-code-cleanup*
*Context gathered: 2026-02-05*
