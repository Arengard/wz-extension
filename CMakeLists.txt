cmake_minimum_required(VERSION 3.15...3.28)

# Set extension name here
set(TARGET_NAME wz)

project(${TARGET_NAME})

# Require C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)
# Include yyjson for JSON parsing
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/yyjson/include)

set(EXTENSION_SOURCES
    src/wz_extension.cpp
    src/into_wz_function.cpp
    src/constraint_checker.cpp
    src/vorlauf_builder.cpp
    src/primanota_mapper.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link yyjson for JSON parsing
target_link_libraries(${TARGET_NAME}_loadable_extension duckdb_yyjson)
target_link_libraries(${TARGET_NAME}_extension duckdb_yyjson)

# Add the static extension to DuckDB's export set to resolve linking
if(TARGET ${TARGET_NAME}_extension)
    install(TARGETS ${TARGET_NAME}_extension
            EXPORT DuckDBExports
            ARCHIVE DESTINATION lib
            LIBRARY DESTINATION lib)
endif()

install(
  TARGETS ${TARGET_NAME}_loadable_extension
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)
