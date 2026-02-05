#include "wz_extension.hpp"
#include "duckdb/common/types/uuid.hpp"
#include <chrono>
#include <ctime>
#include <algorithm>

namespace duckdb {

// ============================================================================
// Helper: Get current timestamp as string
// ============================================================================

static string GetCurrentTimestampString() {
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

// ============================================================================
// Helper: Extract month/year from date string
// ============================================================================

static pair<int, int> ExtractMonthYear(const string &date_str) {
    // Expected format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
    int year = 0, month = 0;

    if (date_str.length() >= 7) {
        year = std::stoi(date_str.substr(0, 4));
        month = std::stoi(date_str.substr(5, 2));
    }

    return {month, year};
}

// ============================================================================
// Derive Vorlauf Bezeichnung from date range
// ============================================================================

string DeriveVorlaufBezeichnung(const string &date_from, const string &date_to) {
    pair<int, int> from_result = ExtractMonthYear(date_from);
    int month_from = from_result.first;
    int year_from = from_result.second;

    pair<int, int> to_result = ExtractMonthYear(date_to);
    int month_to = to_result.first;
    int year_to = to_result.second;

    char buffer[128];
    snprintf(buffer, sizeof(buffer), "Vorlauf %02d/%d-%02d/%d",
             month_from, year_from, month_to, year_to);

    return string(buffer);
}

// ============================================================================
// Find date range in data chunk
// ============================================================================

static pair<string, string> FindDateRangeInData(DataChunk &data, const string &date_column_name) {
    string min_date, max_date;

    // Find the date column index
    // This is a simplified implementation - real code would need column name mapping
    // For now, look for "dtmBelegDatum" column

    // Iterate through all columns looking for date-like data
    for (idx_t col_idx = 0; col_idx < data.ColumnCount(); col_idx++) {
        auto &col = data.data[col_idx];
        auto col_type = col.GetType().id();

        if (col_type == LogicalTypeId::DATE || col_type == LogicalTypeId::TIMESTAMP ||
            col_type == LogicalTypeId::VARCHAR) {

            for (idx_t row_idx = 0; row_idx < data.size(); row_idx++) {
                auto val = col.GetValue(row_idx);
                if (!val.IsNull()) {
                    string date_str = val.ToString();

                    // Basic check if it looks like a date (starts with year)
                    if (date_str.length() >= 10 && date_str[4] == '-') {
                        if (min_date.empty() || date_str < min_date) {
                            min_date = date_str;
                        }
                        if (max_date.empty() || date_str > max_date) {
                            max_date = date_str;
                        }
                    }
                }
            }
        }
    }

    return {min_date, max_date};
}

// ============================================================================
// Build Vorlauf record from data
// ============================================================================

VorlaufRecord BuildVorlaufFromData(DataChunk &data,
                                   const WzConfig &config,
                                   const string &generated_vorlauf_id) {
    VorlaufRecord vorlauf;

    // Set fixed values
    vorlauf.lng_timestamp = 0;
    vorlauf.str_angelegt = config.str_angelegt;
    vorlauf.dtm_angelegt = GetCurrentTimestampString();
    vorlauf.str_geaendert = "";  // NULL
    vorlauf.dtm_geaendert = "";  // NULL
    vorlauf.gui_vorlauf_id = generated_vorlauf_id;
    vorlauf.gui_verfahren_id = config.gui_verfahren_id;
    vorlauf.lng_kanzlei_konten_rahmen_id = config.lng_kanzlei_konten_rahmen_id;
    vorlauf.lng_status = 1;
    vorlauf.lng_vorlauf_nr = 0;  // NULL - will be auto-assigned
    vorlauf.dtm_datev_export = "";  // NULL
    vorlauf.ysn_auto_bu_schluessel_4stellig = false;

    // Derive date range from data
    auto date_range = FindDateRangeInData(data, "dtmBelegDatum");
    std::string date_from = date_range.first;
    std::string date_to = date_range.second;

    if (!date_from.empty()) {
        // Convert date to datetime format (add 00:00:00 if needed)
        if (date_from.length() == 10) {
            date_from += " 00:00:00";
        }
        vorlauf.dtm_vorlauf_datum_von = date_from;
    } else {
        // Default to first day of current month
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        char buffer[32];
        std::strftime(buffer, sizeof(buffer), "%Y-%m-01 00:00:00", std::localtime(&time));
        vorlauf.dtm_vorlauf_datum_von = string(buffer);
    }

    if (!date_to.empty()) {
        if (date_to.length() == 10) {
            date_to += " 00:00:00";
        }
        vorlauf.dtm_vorlauf_datum_bis = date_to;
    } else {
        // Default to last day of current month
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        auto tm = *std::localtime(&time);

        // Move to next month, then back one day
        tm.tm_mon += 1;
        tm.tm_mday = 0;  // Day 0 of next month = last day of current month
        std::mktime(&tm);

        char buffer[32];
        std::strftime(buffer, sizeof(buffer), "%Y-%m-%d 00:00:00", &tm);
        vorlauf.dtm_vorlauf_datum_bis = string(buffer);
    }

    // Derive Bezeichnung
    vorlauf.str_bezeichnung = DeriveVorlaufBezeichnung(vorlauf.dtm_vorlauf_datum_von,
                                                        vorlauf.dtm_vorlauf_datum_bis);

    return vorlauf;
}

} // namespace duckdb
