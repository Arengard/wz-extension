#define DUCKDB_EXTENSION_MAIN

#include "wz_extension.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

void WzExtension::Load(DuckDB &db) {
    // Register the into_wz table function
    RegisterIntoWzFunction(*db.instance);
}

std::string WzExtension::Name() {
    return "wz";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void wz_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::WzExtension>();
}

DUCKDB_EXTENSION_API const char *wz_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
