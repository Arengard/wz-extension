#define DUCKDB_EXTENSION_MAIN

#include "wz_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

void WzExtension::Load(ExtensionLoader &loader) {
    // Register the into_wz table function
    RegisterIntoWzFunction(loader.GetDatabaseInstance());
}

std::string WzExtension::Name() {
    return "wz";
}

std::string WzExtension::Version() const {
    return "0.1.0";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void wz_init(duckdb::DatabaseInstance &db) {
    // Register the function directly
    duckdb::RegisterIntoWzFunction(db);
}

DUCKDB_EXTENSION_API const char *wz_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
