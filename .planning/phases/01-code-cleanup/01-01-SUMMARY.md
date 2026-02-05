---
phase: 01-code-cleanup
plan: 01
subsystem: core-cleanup
tags: [dead-code, deduplication, header-cleanup, cmake]

dependency_graph:
  requires: []
  provides:
    - "wz_utils.hpp with consolidated FindColumnIndex, GetCurrentTimestamp, DeriveVorlaufBezeichnung"
    - "Clean wz_extension.hpp with only live declarations"
    - "CMakeLists.txt with 3 source files"
  affects:
    - "01-02 (goto refactoring) - cleaner baseline to work from"
    - "Phase 2 (transaction fix) - fewer files, clearer structure"

tech_stack:
  added: []
  patterns:
    - "Header-only inline utility functions in wz_utils.hpp"

file_tracking:
  created:
    - src/include/wz_utils.hpp
  modified:
    - src/into_wz_function.cpp
    - src/include/wz_extension.hpp
    - CMakeLists.txt
  deleted:
    - src/primanota_mapper.cpp
    - src/vorlauf_builder.cpp

decisions:
  - id: "01-01-D1"
    decision: "Inline ExtractMonthYear logic directly into DeriveVorlaufBezeichnung"
    reason: "ExtractMonthYear had only one caller; separate function added indirection without value"

metrics:
  duration: "~9 minutes"
  completed: "2026-02-05"
---

# Phase 01 Plan 01: Dead Code Removal and Helper Consolidation Summary

Removed 2 dead source files (primanota_mapper.cpp, vorlauf_builder.cpp), consolidated 3 duplicated helper functions into a new wz_utils.hpp header, and trimmed 3 dead structs plus 3 dead function declarations from wz_extension.hpp. Net deletion: ~460 lines.

## Tasks Completed

| Task | Name | Commit | Key Changes |
|------|------|--------|-------------|
| 1 | Create wz_utils.hpp and update into_wz_function.cpp | a89b9cf | Created wz_utils.hpp with FindColumnIndex, GetCurrentTimestamp, DeriveVorlaufBezeichnung; removed local duplicates from into_wz_function.cpp |
| 2 | Delete dead files, clean header, update CMakeLists.txt | fb57c47 | Deleted primanota_mapper.cpp and vorlauf_builder.cpp; removed WzConfig, VorlaufRecord, PrimanotaRecord from header; updated CMakeLists.txt |

## What Changed

### Created: src/include/wz_utils.hpp
Header-only utility file containing three inline functions in namespace duckdb:
- `FindColumnIndex` - Case-insensitive column name lookup returning idx_t
- `GetCurrentTimestamp` - MSSQL-formatted timestamp with centisecond precision
- `DeriveVorlaufBezeichnung` - Derives Vorlauf description from date range (inlined ExtractMonthYear logic)

### Modified: src/into_wz_function.cpp
- Added `#include "wz_utils.hpp"`
- Removed local `static string GetCurrentTimestamp()` (was lines 145-157)
- Removed local `static idx_t FindColumnIndex()` (was lines 205-217)
- All existing calls to FindColumnIndex, GetCurrentTimestamp, and DeriveVorlaufBezeichnung now resolve from wz_utils.hpp

### Modified: src/include/wz_extension.hpp
Removed dead declarations:
- `WzConfig` struct (only used by deleted vorlauf_builder.cpp)
- `VorlaufRecord` struct (only used by deleted vorlauf_builder.cpp)
- `PrimanotaRecord` struct (only used by deleted primanota_mapper.cpp)
- `BuildVorlaufFromData` function declaration
- `DeriveVorlaufBezeichnung` function declaration (now inline in wz_utils.hpp)
- `MapDataToPrimanota` function declaration

Retained: WzExtension class, ForeignKeyConstraint, ConstraintViolation, InsertResult, all constraint checker declarations, RegisterIntoWzFunction.

### Modified: CMakeLists.txt
Reduced EXTENSION_SOURCES from 5 to 3 files: wz_extension.cpp, into_wz_function.cpp, constraint_checker.cpp.

### Deleted: src/primanota_mapper.cpp
All functions were dead (FindColumnIndex duplicate, GetTimestampNow duplicate, MapDataToPrimanota with no callers, GeneratePrimanotaInsertSQL with no callers).

### Deleted: src/vorlauf_builder.cpp
DeriveVorlaufBezeichnung moved to wz_utils.hpp. Remaining functions were dead (GetCurrentTimestampString duplicate, ExtractMonthYear inlined, FindDateRangeInData unused, BuildVorlaufFromData unused).

## Deviations from Plan

None - plan executed exactly as written.

## Decisions Made

1. **Inline ExtractMonthYear into DeriveVorlaufBezeichnung** (01-01-D1): The plan specified inlining ExtractMonthYear logic directly rather than keeping it as a separate helper. This was the right call since it had exactly one caller.

## Verification Results

All success criteria met:
- primanota_mapper.cpp and vorlauf_builder.cpp deleted from filesystem
- FindColumnIndex exists in exactly one place (wz_utils.hpp)
- GetCurrentTimestamp exists in exactly one place (wz_utils.hpp)
- DeriveVorlaufBezeichnung exists in exactly one place (wz_utils.hpp)
- into_wz_function.cpp uses wz_utils.hpp versions of all three helpers
- wz_extension.hpp contains no declarations for deleted code
- CMakeLists.txt lists exactly 3 source files
- No broken references (grep confirms no mention of deleted files in remaining sources)
- Gotos still present in into_wz_function.cpp (Plan 01-02 scope)

## Next Phase Readiness

Plan 01-02 (goto refactoring) can proceed immediately. The codebase now has:
- A clean utility header (wz_utils.hpp) for shared helpers
- A trimmed extension header (wz_extension.hpp) with only live declarations
- Only 3 source files to maintain
- into_wz_function.cpp ready for goto-to-structured-flow refactoring
