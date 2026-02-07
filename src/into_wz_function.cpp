#include "wz_extension.hpp"
#include "wz_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "mbedtls_wrapper.hpp"
#include <chrono>
#include <iterator>
#include <sstream>

namespace duckdb {

// ============================================================================
// Constants
// ============================================================================

static constexpr size_t PRIMANOTA_BATCH_SIZE = 100;
static constexpr size_t DUPLICATE_CHECK_BATCH_SIZE = 100;
static constexpr size_t MAX_DUPLICATES_TO_DISPLAY = 5;

// Column alias mappings are defined in wz_utils.hpp (FindColumnWithAliases)
// as a single source of truth shared with constraint_checker.cpp.

// ============================================================================
// Helper: Parse Soll/Haben string to boolean
// Returns true for Soll (S), false for Haben (H)
// ============================================================================

static bool ParseSollHaben(const Value &val) {
    if (val.IsNull()) {
        return false;  // Default to Haben (credit)
    }

    // If it's already a boolean, return it directly
    if (val.type().id() == LogicalTypeId::BOOLEAN) {
        return val.GetValue<bool>();
    }

    // If it's a number (0 or 1)
    if (val.type().id() == LogicalTypeId::INTEGER ||
        val.type().id() == LogicalTypeId::BIGINT ||
        val.type().id() == LogicalTypeId::TINYINT ||
        val.type().id() == LogicalTypeId::SMALLINT) {
        return val.GetValue<int64_t>() != 0;
    }

    // Parse string values
    string str = val.ToString();
    // Convert to uppercase for comparison
    std::transform(str.begin(), str.end(), str.begin(), ::toupper);

    // Soll (Debit) = true
    if (str == "S" || str == "SOLL" || str == "1" || str == "TRUE" || str == "DEBIT" || str == "D") {
        return true;
    }

    // Haben (Credit) = false
    // "H", "HABEN", "0", "FALSE", "CREDIT", "C", or anything else
    return false;
}

// ============================================================================
// Bind data for into_wz function
// ============================================================================

struct IntoWzBindData : public TableFunctionData {
    string secret_name;           // MSSQL secret name
    string source_table;          // Source table/query name
    string gui_verfahren_id;
    int64_t lng_kanzlei_konten_rahmen_id;
    string str_angelegt;
    bool generate_vorlauf_id;

    // Source data column info
    vector<string> source_columns;

    // Collected source data
    vector<vector<Value>> source_rows;

    // Results to return
    vector<InsertResult> results;
    bool executed;
};

// ============================================================================
// Global state for into_wz function
// ============================================================================

struct IntoWzGlobalState : public GlobalTableFunctionState {
    idx_t current_idx;
    IntoWzGlobalState() : current_idx(0) {}
};

// ============================================================================
// Helper: Generate deterministic UUID v5 from a row key string
// Uses SHA-1 hash with a fixed namespace UUID (DNS namespace)
// ============================================================================

// Fixed namespace UUID for UUID v5 generation (using DNS namespace as base)
// 6ba7b810-9dad-11d1-80b4-00c04fd430c8
static const uint8_t NAMESPACE_UUID_BYTES[16] = {
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
    0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8
};

static string GenerateUUIDv5(const string &row_key) {
    // Create SHA-1 hash of namespace + row_key
    duckdb_mbedtls::MbedTlsWrapper::SHA1State sha1;

    // Add namespace UUID bytes
    sha1.AddString(string(reinterpret_cast<const char*>(NAMESPACE_UUID_BYTES), 16));
    // Add the row key
    sha1.AddString(row_key);

    // Get the SHA-1 hash (20 raw bytes)
    string hash = sha1.Finalize();

    // Copy first 16 bytes of hash to uuid_bytes
    uint8_t uuid_bytes[16];
    for (int i = 0; i < 16; i++) {
        uuid_bytes[i] = static_cast<uint8_t>(hash[i]);
    }

    // Set version to 5 (bits 4-7 of byte 6)
    uuid_bytes[6] = (uuid_bytes[6] & 0x0F) | 0x50;

    // Set variant to RFC 4122 (bits 6-7 of byte 8)
    uuid_bytes[8] = (uuid_bytes[8] & 0x3F) | 0x80;

    // Format as UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    char uuid_str[37];
    snprintf(uuid_str, sizeof(uuid_str),
             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             uuid_bytes[0], uuid_bytes[1], uuid_bytes[2], uuid_bytes[3],
             uuid_bytes[4], uuid_bytes[5], uuid_bytes[6], uuid_bytes[7],
             uuid_bytes[8], uuid_bytes[9], uuid_bytes[10], uuid_bytes[11],
             uuid_bytes[12], uuid_bytes[13], uuid_bytes[14], uuid_bytes[15]);

    return string(uuid_str);
}

// Build a deterministic row key from row values for UUID generation
static string BuildRowKey(const vector<Value> &row) {
    std::ostringstream key;
    for (size_t i = 0; i < row.size(); i++) {
        if (i > 0) key << "|";
        if (!row[i].IsNull()) {
            key << row[i].ToString();
        }
    }
    return key.str();
}

// Generate deterministic UUID v5 for a specific row
static string GenerateRowUUID(const vector<Value> &row) {
    string row_key = BuildRowKey(row);
    return GenerateUUIDv5(row_key);
}

// Generate deterministic UUID v5 for Vorlauf based on all source rows
static string GenerateVorlaufUUID(const vector<vector<Value>> &rows,
                                   const string &gui_verfahren_id) {
    std::ostringstream combined_key;
    combined_key << "vorlauf:" << gui_verfahren_id << ":";
    for (size_t r = 0; r < rows.size(); r++) {
        if (r > 0) combined_key << "||";
        combined_key << BuildRowKey(rows[r]);
    }
    return GenerateUUIDv5(combined_key.str());
}

// ============================================================================
// Helper: Validate UUID format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
// ============================================================================

static bool IsValidUuidFormat(const string &str) {
    if (str.length() != 36) {
        return false;
    }
    for (size_t i = 0; i < str.length(); i++) {
        char c = str[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') return false;
        } else {
            if (!isxdigit(static_cast<unsigned char>(c))) return false;
        }
    }
    return true;
}

// ============================================================================
// Helper: Find guiVorlaufID column index with common aliases
// ============================================================================

static idx_t FindVorlaufIdColumn(const vector<string> &columns) {
    static const vector<string> candidates = {
        "guiVorlaufID", "gui_vorlauf_id", "guivorlaufid", "guivorlauf_id"
    };
    for (const auto &candidate : candidates) {
        idx_t idx = FindColumnIndex(columns, candidate);
        if (idx != DConstants::INVALID_INDEX) {
            return idx;
        }
    }
    return DConstants::INVALID_INDEX;;
}

// EscapeSqlString is provided by wz_utils.hpp

// ============================================================================
// Helper: Format value for SQL INSERT
// ============================================================================

static string FormatSqlValue(const Value &val) {
    if (val.IsNull()) {
        return "NULL";
    }

    auto type = val.type().id();
    switch (type) {
        case LogicalTypeId::VARCHAR:
            return "'" + EscapeSqlString(val.ToString()) + "'";
        case LogicalTypeId::BOOLEAN:
            return val.GetValue<bool>() ? "1" : "0";
        case LogicalTypeId::DATE:
        case LogicalTypeId::TIMESTAMP:
        case LogicalTypeId::UUID:
            return "'" + EscapeSqlString(val.ToString()) + "'";
        default:
            // Numeric types - validate they don't contain SQL injection
            {
                string str = val.ToString();
                // For numeric types, ensure it's actually numeric (defense in depth)
                bool is_numeric = !str.empty() && (str[0] == '-' || str[0] == '.' || isdigit(str[0]));
                for (size_t i = 1; i < str.size() && is_numeric; i++) {
                    char c = str[i];
                    is_numeric = isdigit(c) || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-';
                }
                if (is_numeric) {
                    return str;
                }
                // Fallback to escaped string if not purely numeric
                return "'" + EscapeSqlString(str) + "'";
            }
    }
}

// ============================================================================
// Helper: Find date range in source data
// ============================================================================

static pair<string, string> FindDateRange(const vector<vector<Value>> &rows,
                                           const vector<string> &columns) {
    string min_date, max_date;

    // Look for dtmBelegDatum column (tries aliases: belegdatum, datum, date)
    idx_t date_col = FindColumnWithAliases(columns, "dtmBelegDatum");

    if (date_col == DConstants::INVALID_INDEX) {
        return {"", ""};
    }

    for (const auto &row : rows) {
        if (date_col < row.size() && !row[date_col].IsNull()) {
            string date_str = row[date_col].ToString();
            // Normalize date string (take first 10 chars for YYYY-MM-DD)
            if (date_str.length() >= 10) {
                date_str = date_str.substr(0, 10);
            }
            if (min_date.empty() || date_str < min_date) {
                min_date = date_str;
            }
            if (max_date.empty() || date_str > max_date) {
                max_date = date_str;
            }
        }
    }

    return {min_date, max_date};
}

// ============================================================================
// Helper: Check for duplicate Primanota IDs
// Uses attached database syntax: db_name.dbo.tblPrimanota
// Uses a separate connection to avoid deadlock when called from table function
// ============================================================================

static vector<string> CheckDuplicates(ClientContext &context,
                                       const string &db_name,
                                       const vector<string> &primanota_ids) {
    vector<string> duplicates;

    if (primanota_ids.empty()) {
        return duplicates;
    }

    // Use a separate connection for queries
    auto &db = DatabaseInstance::GetDatabase(context);
    Connection conn(db);

    // Build IN clause (batch in groups to avoid query size limits)
    for (size_t batch_start = 0; batch_start < primanota_ids.size(); batch_start += DUPLICATE_CHECK_BATCH_SIZE) {
        size_t batch_end = std::min(batch_start + DUPLICATE_CHECK_BATCH_SIZE, primanota_ids.size());

        string in_clause;
        for (size_t i = batch_start; i < batch_end; i++) {
            if (i > batch_start) {
                in_clause += ", ";
            }
            in_clause += "'" + EscapeSqlString(primanota_ids[i]) + "'";
        }

        // Query attached database directly
        string query = "SELECT CAST(guiPrimanotaID AS VARCHAR) AS id FROM " + db_name + ".dbo.tblPrimanota WHERE guiPrimanotaID IN (" + in_clause + ")";

        auto result = conn.Query(query);
        if (!result->HasError()) {
            auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
            auto &collection = materialized->Collection();
            for (auto &chunk : collection.Chunks()) {
                for (idx_t i = 0; i < chunk.size(); i++) {
                    duplicates.push_back(chunk.data[0].GetValue(i).ToString());
                }
            }
        }
    }

    return duplicates;
}

// ============================================================================
// Helper: Build Vorlauf INSERT SQL
// ============================================================================

static string BuildVorlaufInsertSQL(const string &db_name,
                                     const string &vorlauf_id,
                                     const string &verfahren_id,
                                     int64_t konten_rahmen_id,
                                     const string &str_angelegt,
                                     const string &date_from,
                                     const string &date_to,
                                     const string &bezeichnung) {
    string timestamp = GetCurrentTimestamp();

    std::ostringstream sql;
    sql << "INSERT INTO " << db_name << ".dbo.tblVorlauf (";
    sql << "lngTimestamp, strAngelegt, dtmAngelegt, strGeaendert, dtmGeaendert, ";
    sql << "guiVorlaufID, guiVerfahrenID, lngKanzleiKontenRahmenID, lngStatus, ";
    sql << "dtmVorlaufDatumBis, dtmVorlaufDatumVon, lngVorlaufNr, strBezeichnung, ";
    sql << "dtmDatevExport, ysnAutoBuSchluessel4stellig";
    sql << ") VALUES (";
    sql << "0, ";  // lngTimestamp
    sql << "'" << EscapeSqlString(str_angelegt) << "', ";  // strAngelegt
    sql << "'" << timestamp << "', ";  // dtmAngelegt
    sql << "NULL, ";  // strGeaendert
    sql << "NULL, ";  // dtmGeaendert
    sql << "'" << EscapeSqlString(vorlauf_id) << "', ";  // guiVorlaufID
    sql << "'" << EscapeSqlString(verfahren_id) << "', ";  // guiVerfahrenID
    sql << konten_rahmen_id << ", ";  // lngKanzleiKontenRahmenID
    sql << "1, ";  // lngStatus
    sql << "'" << EscapeSqlString(date_to) << " 00:00:00', ";  // dtmVorlaufDatumBis
    sql << "'" << EscapeSqlString(date_from) << " 00:00:00', ";  // dtmVorlaufDatumVon
    sql << "NULL, ";  // lngVorlaufNr
    sql << "'" << EscapeSqlString(bezeichnung) << "', ";  // strBezeichnung
    sql << "NULL, ";  // dtmDatevExport
    sql << "0";  // ysnAutoBuSchluessel4stellig
    sql << ")";

    return sql.str();
}

// ============================================================================
// Helper: Build Primanota INSERT SQL for a batch of rows (using iterators to avoid copying)
// ============================================================================

static string BuildPrimanotaInsertSQL(const string &db_name,
                                       vector<vector<Value>>::const_iterator rows_begin,
                                       vector<vector<Value>>::const_iterator rows_end,
                                       const vector<string> &columns,
                                       const string &vorlauf_id,
                                       const string &verfahren_id,
                                       const string &str_angelegt,
                                       const string &vorlauf_datum_bis) {
    if (rows_begin == rows_end) {
        return "";
    }

    string timestamp = GetCurrentTimestamp();

    // Find column indices using shared alias definitions (single source of truth in wz_utils.hpp)
    idx_t col_primanota_id = FindColumnIndex(columns, "guiPrimanotaID");
    idx_t col_beleg_datum = FindColumnWithAliases(columns, "dtmBelegDatum");
    idx_t col_konto_nr = FindColumnWithAliases(columns, "decKontoNr");
    idx_t col_gegenkonto_nr = FindColumnWithAliases(columns, "decGegenkontoNr");
    idx_t col_ea_konto_nr = FindColumnWithAliases(columns, "decEaKontoNr");
    idx_t col_ysn_soll = FindColumnWithAliases(columns, "ysnSoll");
    idx_t col_eingabe_betrag = FindColumnWithAliases(columns, "curEingabeBetrag");
    idx_t col_basis_betrag = FindColumnWithAliases(columns, "curBasisBetrag");
    idx_t col_beleg1 = FindColumnWithAliases(columns, "strBeleg1");
    idx_t col_beleg2 = FindColumnWithAliases(columns, "strBeleg2");
    idx_t col_buch_text = FindColumnWithAliases(columns, "strBuchText");

    std::ostringstream sql;
    sql << "INSERT INTO " << db_name << ".dbo.tblPrimanota (";
    sql << "lngTimestamp, strAngelegt, dtmAngelegt, strGeaendert, dtmGeaendert, ";
    sql << "guiPrimanotaID, guiVorlaufID, lngStatus, lngZeilenNr, lngEingabeWaehrungID, ";
    sql << "lngBu, decGegenkontoNr, decKontoNr, decEaKontoNr, dtmVorlaufDatumBis, ";
    sql << "dtmBelegDatum, ysnSoll, curEingabeBetrag, curBasisBetrag, curSkontoBetrag, ";
    sql << "curSkontoBasisBetrag, decKostMenge, decWaehrungskurs, strBeleg1, strBeleg2, ";
    sql << "strBuchText, strKost1, strKost2, strEuLand, strUstId, ";
    sql << "decEuSteuersatz, dtmZusatzDatum, guiVerfahrenID, decEaSteuersatz, ";
    sql << "ysnEaTransaktionenManuell, decEaNummer, lngSachverhalt13b, dtmLeistung, ";
    sql << "ysnIstversteuerungInSollversteuerung, lngSkontoSachverhaltWarenRHB, ";
    sql << "ysnVStBeiZahlung, guiParentPrimanota, ysnGeneralUmkehr, decSteuersatzManuell, ";
    sql << "ysnMitUrsprungsland, strUrsprungsland, strUrsprungslandUstId, decUrsprungslandSteuersatz";
    sql << ") VALUES\n";

    bool first = true;
    for (auto it = rows_begin; it != rows_end; ++it) {
        const auto &row = *it;

        if (!first) {
            sql << ",\n";
        }
        first = false;

        // Get values from row with safe access
        auto getValue = [&row](idx_t idx) -> Value {
            if (idx == DConstants::INVALID_INDEX || idx >= row.size()) {
                return Value();
            }
            return row[idx];
        };

        string primanota_id = getValue(col_primanota_id).IsNull() ? GenerateRowUUID(row) : getValue(col_primanota_id).ToString();
        string beleg_datum = getValue(col_beleg_datum).IsNull() ? timestamp.substr(0, 10) : getValue(col_beleg_datum).ToString();
        if (beleg_datum.length() > 10) {
            beleg_datum = beleg_datum.substr(0, 10);
        }
        beleg_datum += " 00:00:00";

        Value konto_val = getValue(col_konto_nr);
        Value gegenkonto_val = getValue(col_gegenkonto_nr);
        Value ea_konto_val = getValue(col_ea_konto_nr);
        Value ysn_soll_val = getValue(col_ysn_soll);
        bool is_soll = ParseSollHaben(ysn_soll_val);  // Parse Soll/Haben string
        Value eingabe_betrag_val = getValue(col_eingabe_betrag);
        Value basis_betrag_val = getValue(col_basis_betrag);
        Value beleg1_val = getValue(col_beleg1);
        Value beleg2_val = getValue(col_beleg2);
        Value buch_text_val = getValue(col_buch_text);

        // Use eingabe_betrag for basis_betrag if not provided
        if (basis_betrag_val.IsNull() && !eingabe_betrag_val.IsNull()) {
            basis_betrag_val = eingabe_betrag_val;
        }

        sql << "(";
        sql << "0, ";  // lngTimestamp
        sql << "'" << EscapeSqlString(str_angelegt) << "', ";  // strAngelegt
        sql << "'" << timestamp << "', ";  // dtmAngelegt
        sql << "NULL, ";  // strGeaendert
        sql << "NULL, ";  // dtmGeaendert
        sql << "'" << EscapeSqlString(primanota_id) << "', ";  // guiPrimanotaID
        sql << "'" << EscapeSqlString(vorlauf_id) << "', ";  // guiVorlaufID
        sql << "1, ";  // lngStatus
        sql << "NULL, ";  // lngZeilenNr
        sql << "1, ";  // lngEingabeWaehrungID (EUR)
        sql << "NULL, ";  // lngBu
        sql << FormatSqlValue(gegenkonto_val) << ", ";  // decGegenkontoNr
        sql << FormatSqlValue(konto_val) << ", ";  // decKontoNr
        sql << FormatSqlValue(ea_konto_val) << ", ";  // decEaKontoNr
        sql << "'" << EscapeSqlString(vorlauf_datum_bis) << " 00:00:00', ";  // dtmVorlaufDatumBis
        sql << "'" << EscapeSqlString(beleg_datum) << "', ";  // dtmBelegDatum
        sql << (is_soll ? "1" : "0") << ", ";  // ysnSoll (parsed from sh/Soll/Haben)
        sql << FormatSqlValue(eingabe_betrag_val) << ", ";  // curEingabeBetrag
        sql << FormatSqlValue(basis_betrag_val) << ", ";  // curBasisBetrag
        sql << "NULL, ";  // curSkontoBetrag
        sql << "NULL, ";  // curSkontoBasisBetrag
        sql << "NULL, ";  // decKostMenge
        sql << "NULL, ";  // decWaehrungskurs
        sql << (beleg1_val.IsNull() ? "NULL" : "'" + EscapeSqlString(beleg1_val.ToString()) + "'") << ", ";  // strBeleg1
        sql << (beleg2_val.IsNull() ? "NULL" : "'" + EscapeSqlString(beleg2_val.ToString()) + "'") << ", ";  // strBeleg2
        sql << (buch_text_val.IsNull() ? "NULL" : "'" + EscapeSqlString(buch_text_val.ToString()) + "'") << ", ";  // strBuchText
        sql << "NULL, NULL, ";  // strKost1, strKost2
        sql << "NULL, NULL, ";  // strEuLand, strUstId
        sql << "NULL, NULL, ";  // decEuSteuersatz, dtmZusatzDatum
        sql << "'" << EscapeSqlString(verfahren_id) << "', ";  // guiVerfahrenID
        sql << "NULL, ";  // decEaSteuersatz
        sql << "0, ";  // ysnEaTransaktionenManuell
        sql << "NULL, NULL, NULL, ";  // decEaNummer, lngSachverhalt13b, dtmLeistung
        sql << "0, NULL, 0, ";  // ysnIstversteuerung..., lngSkontoSachverhalt..., ysnVStBeiZahlung
        sql << "NULL, 0, NULL, ";  // guiParentPrimanota, ysnGeneralUmkehr, decSteuersatzManuell
        sql << "0, NULL, NULL, NULL";  // ysnMitUrsprungsland, strUrsprungsland, strUrsprungslandUstId, decUrsprungslandSteuersatz
        sql << ")";
    }

    return sql.str();
}


// ============================================================================
// Helper: Execute SQL statement via a Connection (for transaction continuity)
// ============================================================================

static bool ExecuteMssqlStatementWithConn(Connection &conn,
                                           const string &sql,
                                           string &error_message) {
    auto result = conn.Query(sql);
    if (result->HasError()) {
        error_message = result->GetError();
        return false;
    }
    return true;
}

// ============================================================================
// Bind function
// ============================================================================

static unique_ptr<FunctionData> IntoWzBind(ClientContext &context,
                                            TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string> &names) {
    auto bind_data = make_uniq<IntoWzBindData>();

    // Set defaults (overridden by parsed parameters below)
    bind_data->generate_vorlauf_id = true;
    bind_data->lng_kanzlei_konten_rahmen_id = 0;

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "secret") {
            bind_data->secret_name = StringValue::Get(kv.second);
        } else if (kv.first == "source_table") {
            bind_data->source_table = StringValue::Get(kv.second);
        } else if (kv.first == "gui_verfahren_id") {
            bind_data->gui_verfahren_id = StringValue::Get(kv.second);
        } else if (kv.first == "lng_kanzlei_konten_rahmen_id") {
            bind_data->lng_kanzlei_konten_rahmen_id = kv.second.GetValue<int64_t>();
        } else if (kv.first == "str_angelegt") {
            bind_data->str_angelegt = StringValue::Get(kv.second);
        } else if (kv.first == "generate_vorlauf_id") {
            bind_data->generate_vorlauf_id = kv.second.GetValue<bool>();
        }
    }

    // Set defaults
    if (bind_data->secret_name.empty()) {
        bind_data->secret_name = "mssql_conn";
    }
    if (bind_data->str_angelegt.empty()) {
        bind_data->str_angelegt = "wz_extension";
    }
    bind_data->executed = false;

    // Define return columns
    names = {"table_name", "rows_inserted", "gui_vorlauf_id", "duration_seconds", "success", "error_message"};
    return_types = {
        LogicalType::VARCHAR,   // table_name
        LogicalType::BIGINT,    // rows_inserted
        LogicalType::VARCHAR,   // gui_vorlauf_id
        LogicalType::DOUBLE,    // duration_seconds
        LogicalType::BOOLEAN,   // success
        LogicalType::VARCHAR    // error_message
    };

    return bind_data;
}

// ============================================================================
// Init global state
// ============================================================================

static unique_ptr<GlobalTableFunctionState> IntoWzInitGlobal(ClientContext &context,
                                                              TableFunctionInitInput &input) {
    return make_uniq<IntoWzGlobalState>();
}

// ============================================================================
// Helper: Add error result
// ============================================================================

static void AddErrorResult(IntoWzBindData &bind_data, const string &table_name,
                           const string &error_message, const string &vorlauf_id = "") {
    InsertResult result;
    result.table_name = table_name;
    result.rows_inserted = 0;
    result.gui_vorlauf_id = vorlauf_id;
    result.duration_seconds = 0;
    result.success = false;
    result.error_message = error_message;
    bind_data.results.push_back(result);
}

// ============================================================================
// Helper: Add success result
// ============================================================================

static void AddSuccessResult(IntoWzBindData &bind_data, const string &table_name,
                              int64_t rows, const string &vorlauf_id, double duration) {
    InsertResult result;
    result.table_name = table_name;
    result.rows_inserted = rows;
    result.gui_vorlauf_id = vorlauf_id;
    result.duration_seconds = duration;
    result.success = true;
    result.error_message = "";
    bind_data.results.push_back(result);
}


// ============================================================================
// Output accumulated results from bind_data to the DataChunk.
// Handles pagination: picks up from global_state.current_idx.
// ============================================================================

static void OutputResults(IntoWzBindData &bind_data,
                          IntoWzGlobalState &global_state,
                          DataChunk &output) {
    if (global_state.current_idx >= bind_data.results.size()) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    while (global_state.current_idx < bind_data.results.size() && count < STANDARD_VECTOR_SIZE) {
        auto &result = bind_data.results[global_state.current_idx];
        output.SetValue(0, count, Value(result.table_name));
        output.SetValue(1, count, Value(result.rows_inserted));
        output.SetValue(2, count, Value(result.gui_vorlauf_id));
        output.SetValue(3, count, Value(result.duration_seconds));
        output.SetValue(4, count, Value(result.success));
        output.SetValue(5, count, Value(result.error_message));
        global_state.current_idx++;
        count++;
    }
    output.SetCardinality(count);
}

// ============================================================================
// Helper: Rollback transaction and add error result (reduces repetitive pattern)
// ============================================================================

static void RollbackAndError(Connection &txn_conn, IntoWzBindData &bind_data,
                              IntoWzGlobalState &global_state, DataChunk &output,
                              const string &table_name, const string &error_message,
                              const string &vorlauf_id = "") {
    string rollback_err;
    ExecuteMssqlStatementWithConn(txn_conn, "ROLLBACK", rollback_err);
    AddErrorResult(bind_data, table_name, error_message, vorlauf_id);
    OutputResults(bind_data, global_state, output);
}

// ============================================================================
// Read source table into bind_data.source_columns and source_rows.
// Returns true on success, sets error_message on failure.
// ============================================================================

static bool LoadSourceData(ClientContext &context,
                           IntoWzBindData &bind_data,
                           string &error_message) {
    auto &db = DatabaseInstance::GetDatabase(context);
    Connection conn(db);

    string source_query = "SELECT * FROM " + bind_data.source_table;
    auto source_result = conn.Query(source_query);

    if (source_result->HasError()) {
        error_message = "Failed to read source table: " + source_result->GetError();
        return false;
    }

    auto source_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(source_result));

    // Get column names
    bind_data.source_columns.reserve(source_materialized->ColumnCount());
    for (idx_t i = 0; i < source_materialized->ColumnCount(); i++) {
        bind_data.source_columns.push_back(source_materialized->ColumnName(i));
    }

    // Collect all rows - pre-allocate for efficiency
    auto &collection = source_materialized->Collection();
    bind_data.source_rows.reserve(collection.Count());
    for (auto &chunk : collection.Chunks()) {
        for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
            vector<Value> row;
            row.reserve(chunk.ColumnCount());
            for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
                row.push_back(chunk.data[col_idx].GetValue(row_idx));
            }
            bind_data.source_rows.push_back(std::move(row));
        }
    }

    if (bind_data.source_rows.empty()) {
        error_message = "Source table is empty";
        return false;
    }

    return true;
}

// ============================================================================
// Check if any guiPrimanotaID values already exist in tblPrimanota.
// Returns true if no duplicates (safe to proceed), false if duplicates found.
// ============================================================================

static bool ValidateDuplicates(ClientContext &context,
                               IntoWzBindData &bind_data,
                               string &error_message) {
    idx_t primanota_id_col = FindColumnIndex(bind_data.source_columns, "guiPrimanotaID");

    if (primanota_id_col == DConstants::INVALID_INDEX) {
        return true;  // No IDs to check
    }

    vector<string> primanota_ids;
    for (const auto &row : bind_data.source_rows) {
        if (primanota_id_col < row.size() && !row[primanota_id_col].IsNull()) {
            primanota_ids.push_back(row[primanota_id_col].ToString());
        }
    }

    auto duplicates = CheckDuplicates(context, bind_data.secret_name, primanota_ids);
    if (!duplicates.empty()) {
        string dup_list;
        size_t display_count = std::min(duplicates.size(), MAX_DUPLICATES_TO_DISPLAY);
        for (size_t i = 0; i < display_count; i++) {
            if (i > 0) dup_list += ", ";
            dup_list += duplicates[i];
        }
        if (duplicates.size() > MAX_DUPLICATES_TO_DISPLAY) {
            dup_list += " (and " + std::to_string(duplicates.size() - MAX_DUPLICATES_TO_DISPLAY) + " more)";
        }
        error_message = "Duplicate guiPrimanotaID found: " + dup_list +
            ". " + std::to_string(duplicates.size()) + " records already exist in tblPrimanota.";
        return false;
    }

    return true;
}

// ============================================================================
// Check if a guiVorlaufID already exists in tblVorlauf.
// Returns true if the check succeeded. `exists` is set accordingly.
// ============================================================================

static bool VorlaufExists(Connection &conn,
                          const string &db_name,
                          const string &vorlauf_id,
                          bool &exists,
                          string &error_message) {
    exists = false;
    string sql = "SELECT 1 FROM " + db_name + ".dbo.tblVorlauf WHERE guiVorlaufID = '" +
                 EscapeSqlString(vorlauf_id) + "'";

    auto result = conn.Query(sql);
    if (result->HasError()) {
        error_message = "Failed to check existing tblVorlauf: " + result->GetError();
        return false;
    }

    auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
    exists = materialized->Collection().Count() > 0;
    return true;
}

// ============================================================================
// Insert a single tblVorlauf record via the transaction connection.
// Returns true on success, sets error_message on failure.
// ============================================================================

static bool InsertVorlauf(Connection &txn_conn,
                          IntoWzBindData &bind_data,
                          const string &vorlauf_id,
                          const string &date_from,
                          const string &date_to,
                          const string &bezeichnung,
                          string &error_message) {
    string vorlauf_sql = BuildVorlaufInsertSQL(
        bind_data.secret_name,
        vorlauf_id,
        bind_data.gui_verfahren_id,
        bind_data.lng_kanzlei_konten_rahmen_id,
        bind_data.str_angelegt,
        date_from,
        date_to,
        bezeichnung
    );

    if (!ExecuteMssqlStatementWithConn(txn_conn, vorlauf_sql, error_message)) {
        error_message = "Failed to insert into tblVorlauf: " + error_message;
        return false;
    }

    return true;
}

// ============================================================================
// Update an existing tblVorlauf record (strBezeichnung and dtmVorlaufDatumBis).
// Returns true on success, sets error_message on failure.
// ============================================================================

static bool UpdateVorlauf(Connection &txn_conn,
                          const string &db_name,
                          const string &vorlauf_id,
                          const string &date_to,
                          const string &bezeichnung,
                          const string &str_angelegt,
                          string &error_message) {
    string timestamp = GetCurrentTimestamp();

    std::ostringstream sql;
    sql << "UPDATE " << db_name << ".dbo.tblVorlauf SET ";
    sql << "strBezeichnung = '" << EscapeSqlString(bezeichnung) << "', ";
    sql << "dtmVorlaufDatumBis = '" << EscapeSqlString(date_to) << " 00:00:00', ";
    sql << "strGeaendert = '" << EscapeSqlString(str_angelegt) << "', ";
    sql << "dtmGeaendert = '" << timestamp << "' ";
    sql << "WHERE guiVorlaufID = '" << EscapeSqlString(vorlauf_id) << "'";

    if (!ExecuteMssqlStatementWithConn(txn_conn, sql.str(), error_message)) {
        error_message = "Failed to update tblVorlauf: " + error_message;
        return false;
    }

    return true;
}

// ============================================================================
// Insert all source rows into tblPrimanota in batches of 100.
// Returns true on success, sets total_rows and error_message on failure.
// ============================================================================

static bool InsertPrimanota(Connection &txn_conn,
                            IntoWzBindData &bind_data,
                            const string &vorlauf_id,
                            const string &date_to,
                            int64_t &total_rows,
                            string &error_message) {
    total_rows = 0;

    for (size_t batch_start = 0; batch_start < bind_data.source_rows.size(); batch_start += PRIMANOTA_BATCH_SIZE) {
        size_t batch_end = std::min(batch_start + PRIMANOTA_BATCH_SIZE, bind_data.source_rows.size());

        auto rows_begin = bind_data.source_rows.begin() + batch_start;
        auto rows_end = bind_data.source_rows.begin() + batch_end;

        string primanota_sql = BuildPrimanotaInsertSQL(
            bind_data.secret_name,
            rows_begin,
            rows_end,
            bind_data.source_columns,
            vorlauf_id,
            bind_data.gui_verfahren_id,
            bind_data.str_angelegt,
            date_to
        );

        if (!ExecuteMssqlStatementWithConn(txn_conn, primanota_sql, error_message)) {
            error_message = "Failed to insert into tblPrimanota (rows " +
                std::to_string(batch_start) + "-" + std::to_string(batch_end - 1) +
                "): " + error_message;
            return false;
        }

        total_rows += std::distance(rows_begin, rows_end);
    }

    return true;
}

// ============================================================================
// Helper: Derive strBezeichnung from MSSQL data
// Returns true on success; leaves `bezeichnung` empty if no data; sets error_message on failure.
// ============================================================================

static bool DeriveBezeichnungFromMssql(Connection &conn,
                                       const string &db_name,
                                       const string &gui_verfahren_id,
                                       string &bezeichnung,
                                       string &error_message) {
    bezeichnung.clear();
    string sql =
        "WITH data AS ("
        " SELECT MIN(dtmBelegDatum) AS minBelegdatum, MAX(dtmBelegDatum) AS maxBelegdatum, tblVerfahren.strAZGericht"
        " FROM " + db_name + ".dbo.tblPrimanota"
        " INNER JOIN " + db_name + ".dbo.tblVerfahren ON tblVerfahren.guiVerfahrenID = tblPrimanota.guiVerfahrenID"
        " WHERE tblPrimanota.guiVerfahrenID = '" + EscapeSqlString(gui_verfahren_id) + "'"
        " GROUP BY tblVerfahren.strAZGericht"
        ")"
        " SELECT 'az - ' || LEFT(RIGHT(strAZGericht, 6), 3) AS x, strAZGericht, minBelegdatum, maxBelegdatum FROM data";

    auto result = conn.Query(sql);
    if (result->HasError()) {
        error_message = "Failed to derive strBezeichnung: " + result->GetError();
        return false;
    }

    auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
    auto &collection = materialized->Collection();
    if (collection.Count() == 0) {
        return true; // Nothing to derive; caller may fallback
    }

    // Use Fetch() to get a chunk safely
    auto chunk = materialized->Fetch();
    if (!chunk || chunk->size() == 0) {
        return true;
    }

    // Verify we have enough columns
    if (chunk->ColumnCount() < 4) {
        return true;
    }

    // Check for null values before accessing
    Value az_val = chunk->data[0].GetValue(0);
    Value min_val = chunk->data[2].GetValue(0);
    Value max_val = chunk->data[3].GetValue(0);

    if (az_val.IsNull()) {
        return true;
    }

    string az_prefix = az_val.ToString();
    string min_date = min_val.IsNull() ? "" : min_val.ToString();
    string max_date = max_val.IsNull() ? "" : max_val.ToString();

    if (min_date.size() > 10) min_date = min_date.substr(0, 10);
    if (max_date.size() > 10) max_date = max_date.substr(0, 10);

    bezeichnung = az_prefix + " Vorlauf " + min_date + " - " + max_date;
    return true;
}

// ============================================================================
// Main execution function
// ============================================================================

static void IntoWzExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<IntoWzBindData>();
    auto &global_state = data_p.global_state->Cast<IntoWzGlobalState>();

    // Re-entry: return remaining results
    if (bind_data.executed) {
        OutputResults(bind_data, global_state, output);
        return;
    }
    bind_data.executed = true;

    // =========================================================================
    // Step 1: Validate required parameters
    // =========================================================================

    if (bind_data.gui_verfahren_id.empty()) {
        AddErrorResult(bind_data, "ERROR", "gui_verfahren_id is required");
        OutputResults(bind_data, global_state, output);
        return;
    }

    if (bind_data.source_table.empty()) {
        AddErrorResult(bind_data, "ERROR", "source_table is required");
        OutputResults(bind_data, global_state, output);
        return;
    }

    if (bind_data.lng_kanzlei_konten_rahmen_id <= 0) {
        AddErrorResult(bind_data, "ERROR", "lng_kanzlei_konten_rahmen_id is required and must be positive");
        OutputResults(bind_data, global_state, output);
        return;
    }

    if (!IsValidSqlIdentifier(bind_data.secret_name)) {
        AddErrorResult(bind_data, "ERROR", "Invalid secret name: must contain only alphanumeric characters, underscores, and dots");
        OutputResults(bind_data, global_state, output);
        return;
    }

    // Validate gui_verfahren_id looks like a UUID (defense in depth)
    if (!IsValidUuidFormat(bind_data.gui_verfahren_id)) {
        AddErrorResult(bind_data, "ERROR", "Invalid gui_verfahren_id format: must be a valid UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)");
        OutputResults(bind_data, global_state, output);
        return;
    }

    // =========================================================================
    // Step 2: Load source data from DuckDB table
    // =========================================================================

    string error_msg;
    if (!LoadSourceData(context, bind_data, error_msg)) {
        AddErrorResult(bind_data, "ERROR", error_msg);
        OutputResults(bind_data, global_state, output);
        return;
    }

    // =========================================================================
    // Step 3: Check for duplicate Primanota IDs
    // =========================================================================

    if (!ValidateDuplicates(context, bind_data, error_msg)) {
        AddErrorResult(bind_data, "ERROR", error_msg);
        OutputResults(bind_data, global_state, output);
        return;
    }

    // =========================================================================
    // Step 3.5: Validate foreign key constraints against MSSQL reference tables
    // =========================================================================

    if (!ValidateForeignKeys(context, bind_data.secret_name,
                             bind_data.source_rows, bind_data.source_columns, error_msg)) {
        AddErrorResult(bind_data, "ERROR", error_msg);
        OutputResults(bind_data, global_state, output);
        return;
    }
    // Propagate FK validation warning (e.g., could not query constraints) as informational row
    if (!error_msg.empty()) {
        InsertResult warning;
        warning.table_name = "FK_VALIDATION";
        warning.rows_inserted = 0;
        warning.gui_vorlauf_id = "";
        warning.duration_seconds = 0;
        warning.success = true;
        warning.error_message = error_msg;
        bind_data.results.push_back(warning);
        error_msg.clear();
    }

    // =========================================================================
    // Step 4: Prepare Vorlauf data
    // Use source guiVorlaufID if provided (and generate_vorlauf_id is false),
    // otherwise generate UUID
    // =========================================================================

    string vorlauf_id;
    bool vorlauf_id_from_source = false;

    // If generate_vorlauf_id is true, always generate a new UUID
    // Otherwise, use source guiVorlaufID if available
    if (!bind_data.generate_vorlauf_id) {
        idx_t vorlauf_id_col = FindVorlaufIdColumn(bind_data.source_columns);
        if (vorlauf_id_col != DConstants::INVALID_INDEX
            && !bind_data.source_rows.empty()
            && vorlauf_id_col < bind_data.source_rows[0].size()
            && !bind_data.source_rows[0][vorlauf_id_col].IsNull()) {
            vorlauf_id = bind_data.source_rows[0][vorlauf_id_col].ToString();
            vorlauf_id_from_source = true;
        }
    }

    if (vorlauf_id.empty()) {
        // Generate deterministic UUID v5 based on all source rows and verfahren_id
        vorlauf_id = GenerateVorlaufUUID(bind_data.source_rows,
                                          bind_data.gui_verfahren_id);
    }

    auto date_range = FindDateRange(bind_data.source_rows, bind_data.source_columns);
    string date_from = date_range.first;
    string date_to = date_range.second;

    if (date_from.empty()) {
        date_from = GetCurrentMonthStart();
    }
    if (date_to.empty()) {
        date_to = GetCurrentDate();
    }

    string bezeichnung = DeriveVorlaufBezeichnung(date_from, date_to);

    // =========================================================================
    // Steps 5-7: Transaction block using separate Connection + SQL-level control
    // Uses conn.Query("BEGIN TRANSACTION"/"COMMIT"/"ROLLBACK") — NOT C++ API
    // BeginTransaction()/Commit() which acquires database-level locks that deadlock
    // =========================================================================

    auto &txn_db = DatabaseInstance::GetDatabase(context);
    Connection txn_conn(txn_db);

    // Begin transaction via SQL (not C++ API — avoids database-level lock conflict)
    if (!ExecuteMssqlStatementWithConn(txn_conn, "BEGIN TRANSACTION", error_msg)) {
        AddErrorResult(bind_data, "ERROR", "Failed to begin transaction: " + error_msg);
        OutputResults(bind_data, global_state, output);
        return;
    }

    string derived_bezeichnung;
    if (!DeriveBezeichnungFromMssql(txn_conn, bind_data.secret_name, bind_data.gui_verfahren_id, derived_bezeichnung, error_msg)) {
        RollbackAndError(txn_conn, bind_data, global_state, output, "tblVorlauf", error_msg, vorlauf_id);
        return;
    }
    if (!derived_bezeichnung.empty()) {
        bezeichnung = std::move(derived_bezeichnung);
    }
    // Step 5: Insert Vorlauf (or update if already exists)
    bool skip_vorlauf_insert = false;
    if (vorlauf_id_from_source) {
        bool exists = false;
        if (!VorlaufExists(txn_conn, bind_data.secret_name, vorlauf_id, exists, error_msg)) {
            RollbackAndError(txn_conn, bind_data, global_state, output, "tblVorlauf", error_msg, vorlauf_id);
            return;
        }
        skip_vorlauf_insert = exists;
    }

    double vorlauf_duration = 0.0;
    if (!skip_vorlauf_insert) {
        auto vorlauf_start = std::chrono::high_resolution_clock::now();
        if (!InsertVorlauf(txn_conn, bind_data, vorlauf_id, date_from, date_to, bezeichnung, error_msg)) {
            RollbackAndError(txn_conn, bind_data, global_state, output, "tblVorlauf", error_msg, vorlauf_id);
            return;
        }
        auto vorlauf_end = std::chrono::high_resolution_clock::now();
        vorlauf_duration = std::chrono::duration<double>(vorlauf_end - vorlauf_start).count();
        AddSuccessResult(bind_data, "tblVorlauf", 1, vorlauf_id, vorlauf_duration);
    } else {
        // Vorlauf exists: update strBezeichnung and dtmVorlaufDatumBis
        auto vorlauf_start = std::chrono::high_resolution_clock::now();
        if (!UpdateVorlauf(txn_conn, bind_data.secret_name, vorlauf_id, date_to, bezeichnung, bind_data.str_angelegt, error_msg)) {
            RollbackAndError(txn_conn, bind_data, global_state, output, "tblVorlauf", error_msg, vorlauf_id);
            return;
        }
        auto vorlauf_end = std::chrono::high_resolution_clock::now();
        vorlauf_duration = std::chrono::duration<double>(vorlauf_end - vorlauf_start).count();
        AddSuccessResult(bind_data, "tblVorlauf (updated)", 0, vorlauf_id, vorlauf_duration);
    }

    // Step 6: Insert Primanota rows
    auto primanota_start = std::chrono::high_resolution_clock::now();
    int64_t total_rows = 0;
    if (!InsertPrimanota(txn_conn, bind_data, vorlauf_id, date_to, total_rows, error_msg)) {
        RollbackAndError(txn_conn, bind_data, global_state, output, "tblPrimanota", error_msg, vorlauf_id);
        return;
    }

    // Step 7: Commit transaction
    if (!ExecuteMssqlStatementWithConn(txn_conn, "COMMIT", error_msg)) {
        AddErrorResult(bind_data, "ERROR", "Failed to commit: " + error_msg, vorlauf_id);
        OutputResults(bind_data, global_state, output);
        return;
    }

    auto primanota_end = std::chrono::high_resolution_clock::now();
    double primanota_duration = std::chrono::duration<double>(primanota_end - primanota_start).count();
    AddSuccessResult(bind_data, "tblPrimanota", total_rows, vorlauf_id, primanota_duration);

    OutputResults(bind_data, global_state, output);
}

// ============================================================================
// Register the function
// ============================================================================

void RegisterIntoWzFunction(DatabaseInstance &db) {
    TableFunction into_wz_func("into_wz", {}, IntoWzExecute, IntoWzBind, IntoWzInitGlobal);

    // Add named parameters
    into_wz_func.named_parameters["secret"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["source_table"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["gui_verfahren_id"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["lng_kanzlei_konten_rahmen_id"] = LogicalType::BIGINT;
    into_wz_func.named_parameters["str_angelegt"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["generate_vorlauf_id"] = LogicalType::BOOLEAN;

    // Register using connection and catalog
    Connection con(db);
    con.BeginTransaction();
    auto &catalog = Catalog::GetSystemCatalog(db);
    CreateTableFunctionInfo info(into_wz_func);
    catalog.CreateFunction(*con.context, info);
    con.Commit();
}

} // namespace duckdb
