#pragma once

#include "duckdb.hpp"
#include <chrono>
#include <algorithm>

namespace duckdb {

// Find column index by name using case-insensitive comparison.
// @param columns  Vector of column names to search
// @param name     Target column name to find
// @return         Index of the matching column, or DConstants::INVALID_INDEX if not found
inline idx_t FindColumnIndex(const vector<string> &columns, const string &name) {
    string name_lower = name;
    std::transform(name_lower.begin(), name_lower.end(), name_lower.begin(), ::tolower);

    for (idx_t i = 0; i < columns.size(); i++) {
        string col_lower = columns[i];
        std::transform(col_lower.begin(), col_lower.end(), col_lower.begin(), ::tolower);
        if (col_lower == name_lower) {
            return i;
        }
    }
    return DConstants::INVALID_INDEX;
}

// Get the current timestamp formatted for MSSQL with centisecond precision.
// Format: "YYYY-MM-DD HH:MM:SS.cc"
// @return  Formatted timestamp string
inline string GetCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;

    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&time));

    char result[64];
    snprintf(result, sizeof(result), "%s.%02d", buffer, static_cast<int>(ms.count() / 10));
    return string(result);
}

// Derive Vorlauf description from a date range by extracting month/year from each date.
// Produces a string like "Vorlauf 01/2024-03/2024".
// @param date_from  Start date string in "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS" format
// @param date_to    End date string in the same format
// @return           Formatted Vorlauf description
inline string DeriveVorlaufBezeichnung(const string &date_from, const string &date_to) {
    int year_from = 0, month_from = 0;
    if (date_from.length() >= 7) {
        year_from = std::stoi(date_from.substr(0, 4));
        month_from = std::stoi(date_from.substr(5, 2));
    }

    int year_to = 0, month_to = 0;
    if (date_to.length() >= 7) {
        year_to = std::stoi(date_to.substr(0, 4));
        month_to = std::stoi(date_to.substr(5, 2));
    }

    char buffer[128];
    snprintf(buffer, sizeof(buffer), "Vorlauf %02d/%d-%02d/%d",
             month_from, year_from, month_to, year_to);
    return string(buffer);
}

} // namespace duckdb
