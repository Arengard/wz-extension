#pragma once

#include "duckdb.hpp"
#include <chrono>
#include <algorithm>

namespace duckdb {

// ============================================================================
// SQL helpers
// ============================================================================

// Escape single quotes for SQL string values.
inline string EscapeSqlString(const string &str) {
    string result;
    result.reserve(str.size() * 2);
    for (char c : str) {
        if (c == '\'') {
            result += "''";
        } else {
            result += c;
        }
    }
    return result;
}

// Validate that a string is a safe SQL identifier (alphanumeric, underscores, dots only).
inline bool IsValidSqlIdentifier(const string &name) {
    if (name.empty()) return false;
    for (char c : name) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_' && c != '.') {
            return false;
        }
    }
    return true;
}

// ============================================================================
// Column lookup
// ============================================================================

// Find column index by name using case-insensitive comparison.
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

// Find a source column by MSSQL target column name, trying all known aliases.
// Single source of truth for column name mappings used by both INSERT and FK validation.
inline idx_t FindColumnWithAliases(const vector<string> &source_columns, const string &target_name) {
    string target_lower = target_name;
    std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);

    // Each group: target_lower is the canonical MSSQL column name (lowercase for matching),
    // aliases is a null-terminated list of recognized source column names (priority order).
    struct AliasGroup {
        const char *target_lower;
        const char *aliases[12]; // null-terminated
    };

    static const AliasGroup ALIAS_GROUPS[] = {
        {"deckontonr",       {"decKontoNr", "konto", "kontonr", "konto_nr", "account", nullptr}},
        {"decgegenkontonr",  {"decGegenkontoNr", "gegenkonto", "gegenkontonr", "gegenkonto_nr", "counter_account", nullptr}},
        {"deceakontonr",     {"decEaKontoNr", "eakonto", "ea_konto", "eakontonr", "ea_konto_nr", nullptr}},
        {"ysnsoll",          {"ysnSoll", "ysnsoll", "soll", "sh", "soll_haben", "sollhaben", nullptr}},
        {"cureingabebetrag", {"curEingabeBetrag", "umsatz", "betrag", "amount", "eingabebetrag", nullptr}},
        {"curbasisbetrag",   {"curBasisBetrag", "umsatz_mit_vorzeichen", "basisbetrag", "basis_betrag", nullptr}},
        {"strbeleg1",        {"strBeleg1", "beleg1", "beleg_1", "belegfeld1", "belegfeld_1", "belegnr", "belegnummer", "beleg_nr", "beleg_nummer", nullptr}},
        {"strbeleg2",        {"strBeleg2", "beleg2", "beleg_2", "belegfeld2", "belegfeld_2", "trans_nr", "transnr", "trans_nummer", "transaktionsnr", nullptr}},
        {"strbuchtext",      {"strBuchText", "strbuchtext", "buchungstext", "buchtext", "text", "description", nullptr}},
        {"dtmbelegdatum",    {"dtmBelegDatum", "belegdatum", "datum", "date", nullptr}},
    };

    for (const auto &group : ALIAS_GROUPS) {
        if (target_lower == group.target_lower) {
            for (int j = 0; group.aliases[j] != nullptr; j++) {
                idx_t idx = FindColumnIndex(source_columns, group.aliases[j]);
                if (idx != DConstants::INVALID_INDEX) {
                    return idx;
                }
            }
            return DConstants::INVALID_INDEX;
        }
    }

    // No alias group matched; fall back to direct name lookup
    return FindColumnIndex(source_columns, target_name);
}

// ============================================================================
// Timestamp / date helpers (thread-safe)
// ============================================================================

// Get the current timestamp formatted for MSSQL with centisecond precision.
// Format: "YYYY-MM-DD HH:MM:SS.cc"
inline string GetCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;

    struct tm time_info;
#ifdef _WIN32
    localtime_s(&time_info, &time);
#else
    localtime_r(&time, &time_info);
#endif

    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &time_info);

    char result[64];
    snprintf(result, sizeof(result), "%s.%02d", buffer, static_cast<int>(ms.count() / 10));
    return string(result);
}

// Get the current date formatted as "YYYY-MM-DD".
inline string GetCurrentDate() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);

    struct tm time_info;
#ifdef _WIN32
    localtime_s(&time_info, &time);
#else
    localtime_r(&time, &time_info);
#endif

    char buffer[16];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &time_info);
    return string(buffer);
}

// Get the first day of the current month as "YYYY-MM-01".
inline string GetCurrentMonthStart() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);

    struct tm time_info;
#ifdef _WIN32
    localtime_s(&time_info, &time);
#else
    localtime_r(&time, &time_info);
#endif

    char buffer[16];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-01", &time_info);
    return string(buffer);
}

// ============================================================================
// Vorlauf helpers
// ============================================================================

// Derive Vorlauf description from a date range.
// Produces a string like "Vorlauf 01/2024-03/2024".
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

// Extract "YYYY-MM" from a date string (first 7 chars). Returns "" if too short.
inline string ExtractYearMonth(const string &date_str) {
    if (date_str.length() >= 7) {
        return date_str.substr(0, 7);
    }
    return "";
}

// Compute last day of a month given "YYYY-MM". Handles leap years.
inline string GetLastDayOfMonth(const string &year_month) {
    if (year_month.length() < 7) {
        return year_month + "-28";
    }
    int year = std::stoi(year_month.substr(0, 4));
    int month = std::stoi(year_month.substr(5, 2));

    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int day = days_in_month[month];
    if (month == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
        day = 29;
    }

    char buffer[16];
    snprintf(buffer, sizeof(buffer), "%s-%02d", year_month.c_str(), day);
    return string(buffer);
}

// Derive single-month Vorlauf description: "Vorlauf MM/YYYY".
inline string DeriveMonthVorlaufBezeichnung(const string &year_month) {
    if (year_month.length() < 7) {
        return "Vorlauf";
    }
    int year = std::stoi(year_month.substr(0, 4));
    int month = std::stoi(year_month.substr(5, 2));

    char buffer[64];
    snprintf(buffer, sizeof(buffer), "Vorlauf %02d/%d", month, year);
    return string(buffer);
}

} // namespace duckdb
