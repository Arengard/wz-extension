#define DUCKDB_EXTENSION_MAIN

#include "wz_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

static void TryAutoLoadMssql(DatabaseInstance &db) {
    try {
        ExtensionHelper::AutoLoadExtension(db, "mssql");
    } catch (...) {
        // MSSQL extension is a third-party dependency that may not be installed.
        // It will be needed at runtime when into_wz() or move_to_mssql() is called.
    }
}

void WzExtension::Load(ExtensionLoader &loader) {
    auto &db = loader.GetDatabaseInstance();
    TryAutoLoadMssql(db);
    RegisterIntoWzFunction(db);
    RegisterMoveToMssqlFunction(db);
}

std::string WzExtension::Name() {
    return "wz";
}

std::string WzExtension::Version() const {
    return "0.1.0";
}

} // namespace duckdb

extern "C" {

// New C++ extension entry point (required for C++ ABI)
DUCKDB_EXTENSION_API void wz_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
    auto &db = loader.GetDatabaseInstance();
    duckdb::TryAutoLoadMssql(db);
    duckdb::RegisterIntoWzFunction(db);
    duckdb::RegisterMoveToMssqlFunction(db);
}

// Legacy entry point (kept for compatibility)
DUCKDB_EXTENSION_API void wz_init(duckdb::DatabaseInstance &db) {
    duckdb::TryAutoLoadMssql(db);
    duckdb::RegisterIntoWzFunction(db);
    duckdb::RegisterMoveToMssqlFunction(db);
}

DUCKDB_EXTENSION_API const char *wz_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
