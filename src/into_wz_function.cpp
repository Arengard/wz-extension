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
#include "duckdb/main/appender.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "mbedtls_wrapper.hpp"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

namespace duckdb {

// ============================================================================
// Constants
// ============================================================================

static constexpr size_t DUPLICATE_CHECK_BATCH_SIZE = 1000;
static constexpr size_t MAX_DUPLICATES_TO_DISPLAY = 5;
static constexpr size_t MSSQL_INSERT_BATCH_SIZE = 1000;   // rows per INSERT VALUES (MSSQL max)
static constexpr size_t MSSQL_STMTS_PER_ROUNDTRIP = 5;   // INSERT statements per conn.Query()

// Staging table name for bulk Primanota transfer
static const char *STAGING_TABLE_NAME = "__wz_primanota_staging";

// Column list shared between staging table creation and INSERT INTO SELECT
static const char *PRIMANOTA_COLUMN_LIST =
    "lngTimestamp, strAngelegt, dtmAngelegt, strGeaendert, dtmGeaendert, "
    "guiPrimanotaID, guiVorlaufID, lngStatus, lngZeilenNr, lngEingabeWaehrungID, "
    "lngBu, decGegenkontoNr, decKontoNr, decEaKontoNr, dtmVorlaufDatumBis, "
    "dtmBelegDatum, ysnSoll, curEingabeBetrag, curBasisBetrag, curSkontoBetrag, "
    "curSkontoBasisBetrag, decKostMenge, decWaehrungskurs, strBeleg1, strBeleg2, "
    "strBuchText, strKost1, strKost2, strEuLand, strUstId, "
    "decEuSteuersatz, dtmZusatzDatum, guiVerfahrenID, decEaSteuersatz, "
    "ysnEaTransaktionenManuell, decEaNummer, lngSachverhalt13b, dtmLeistung, "
    "ysnIstversteuerungInSollversteuerung, lngSkontoSachverhaltWarenRHB, "
    "ysnVStBeiZahlung, guiParentPrimanota, ysnGeneralUmkehr, decSteuersatzManuell, "
    "ysnMitUrsprungsland, strUrsprungsland, strUrsprungslandUstId, decUrsprungslandSteuersatz";

// Staging table DDL (all VARCHAR except INTEGER for int/bool columns)
static const char *STAGING_TABLE_DDL =
    "lngTimestamp INTEGER, strAngelegt VARCHAR, dtmAngelegt VARCHAR, "
    "strGeaendert VARCHAR, dtmGeaendert VARCHAR, "
    "guiPrimanotaID VARCHAR, guiVorlaufID VARCHAR, "
    "lngStatus INTEGER, lngZeilenNr INTEGER, lngEingabeWaehrungID INTEGER, "
    "lngBu INTEGER, decGegenkontoNr VARCHAR, decKontoNr VARCHAR, "
    "decEaKontoNr VARCHAR, dtmVorlaufDatumBis VARCHAR, "
    "dtmBelegDatum VARCHAR, ysnSoll INTEGER, "
    "curEingabeBetrag VARCHAR, curBasisBetrag VARCHAR, "
    "curSkontoBetrag VARCHAR, curSkontoBasisBetrag VARCHAR, "
    "decKostMenge VARCHAR, decWaehrungskurs VARCHAR, "
    "strBeleg1 VARCHAR, strBeleg2 VARCHAR, strBuchText VARCHAR, "
    "strKost1 VARCHAR, strKost2 VARCHAR, strEuLand VARCHAR, strUstId VARCHAR, "
    "decEuSteuersatz VARCHAR, dtmZusatzDatum VARCHAR, "
    "guiVerfahrenID VARCHAR, decEaSteuersatz VARCHAR, "
    "ysnEaTransaktionenManuell INTEGER, decEaNummer VARCHAR, "
    "lngSachverhalt13b INTEGER, dtmLeistung VARCHAR, "
    "ysnIstversteuerungInSollversteuerung INTEGER, "
    "lngSkontoSachverhaltWarenRHB INTEGER, "
    "ysnVStBeiZahlung INTEGER, guiParentPrimanota VARCHAR, "
    "ysnGeneralUmkehr INTEGER, decSteuersatzManuell VARCHAR, "
    "ysnMitUrsprungsland INTEGER, strUrsprungsland VARCHAR, "
    "strUrsprungslandUstId VARCHAR, decUrsprungslandSteuersatz VARCHAR, "
    "year_month VARCHAR";

// Column alias mappings are defined in wz_utils.hpp (FindColumnWithAliases)
// as a single source of truth shared with constraint_checker.cpp.

// Execution phases for the state-machine Execute loop.
// Each phase does one unit of work, emits result rows, and yields to DuckDB
// so that the progress bar can update between phases.
enum class ExecutionPhase {
    VALIDATE_PARAMS,      // 0→5%
    LOAD_DATA,            // 5→20%   (includes row key computation)
    VALIDATE_DATA,        // 20→35%  (duplicate check + FK validation)
    PRE_COMPUTE,          // 35→50%  (UUIDs, month grouping, AZ prefix)
    POPULATE_STAGING,     // 50→65%
    VORLAUF_RECORDS,      // 65→80%
    TRANSFER_PRIMANOTA,   // 80→95%
    FINALIZE,             // 95→100% (TIMING:total)
    OUTPUT_RESULTS,       // drain remaining result rows
    DONE                  // return empty → pipeline ends
};

// Pre-computed column indices to avoid redundant lookups per batch
struct PrimanotaColumnIndices {
    idx_t col_primanota_id;
    idx_t col_beleg_datum;
    idx_t col_konto_nr;
    idx_t col_gegenkonto_nr;
    idx_t col_ea_konto_nr;
    idx_t col_ysn_soll;
    idx_t col_eingabe_betrag;
    idx_t col_basis_betrag;
    idx_t col_beleg1;
    idx_t col_beleg2;
    idx_t col_buch_text;

    static PrimanotaColumnIndices Build(const vector<string> &columns) {
        PrimanotaColumnIndices idx;
        idx.col_primanota_id = FindColumnIndex(columns, "guiPrimanotaID");
        idx.col_beleg_datum = FindColumnWithAliases(columns, "dtmBelegDatum");
        idx.col_konto_nr = FindColumnWithAliases(columns, "decKontoNr");
        idx.col_gegenkonto_nr = FindColumnWithAliases(columns, "decGegenkontoNr");
        idx.col_ea_konto_nr = FindColumnWithAliases(columns, "decEaKontoNr");
        idx.col_ysn_soll = FindColumnWithAliases(columns, "ysnSoll");
        idx.col_eingabe_betrag = FindColumnWithAliases(columns, "curEingabeBetrag");
        idx.col_basis_betrag = FindColumnWithAliases(columns, "curBasisBetrag");
        idx.col_beleg1 = FindColumnWithAliases(columns, "strBeleg1");
        idx.col_beleg2 = FindColumnWithAliases(columns, "strBeleg2");
        idx.col_buch_text = FindColumnWithAliases(columns, "strBuchText");
        return idx;
    }
};

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
    bool monatsvorlauf;
    bool skip_duplicate_check;
    bool skip_fk_check;

    // Source data column info
    vector<string> source_columns;

    // Collected source data
    vector<vector<Value>> source_rows;

    // Pre-computed row keys for UUID generation (avoids repeated ToString per cell)
    vector<string> row_keys;

    // Results to return
    vector<InsertResult> results;
    bool executed;
};

// ============================================================================
// Global state for into_wz function
// ============================================================================

// Per-month data used by monatsvorlauf (batch Vorlauf + parallel Primanota)
struct MonthInfo {
    string year_month;
    vector<idx_t> row_indices;
    string vorlauf_id;
    string bezeichnung;
    string date_from;
    string date_to;
    vector<string> primanota_ids;
};

struct IntoWzGlobalState : public GlobalTableFunctionState {
    idx_t current_idx;
    ExecutionPhase phase;
    double progress;  // 0.0–100.0, read by IntoWzProgress callback

    // Timers
    std::chrono::high_resolution_clock::time_point total_start;

    // Persistent state across phases
    PrimanotaColumnIndices col_idx;
    unique_ptr<Connection> staging_conn;
    unique_ptr<Connection> txn_conn;

    // Single-vorlauf state
    string vorlauf_id;
    string date_from, date_to, bezeichnung;
    vector<string> primanota_ids;
    bool vorlauf_id_from_source;
    bool skip_vorlauf_insert;

    // Monatsvorlauf state
    vector<MonthInfo> months;
    string cached_az_prefix;

    IntoWzGlobalState() : current_idx(0), phase(ExecutionPhase::VALIDATE_PARAMS),
                           progress(0.0), vorlauf_id_from_source(false),
                           skip_vorlauf_insert(false) {}
};

// Progress callback: DuckDB polls this between Execute calls.
static double IntoWzProgress(ClientContext &context, const FunctionData *bind_data_p,
                              const GlobalTableFunctionState *global_state_p) {
    auto &state = global_state_p->Cast<IntoWzGlobalState>();
    return state.progress;
}

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
    string key;
    key.reserve(row.size() * 22);  // ~20 chars per value estimate
    for (size_t i = 0; i < row.size(); i++) {
        if (i > 0) key += '|';
        if (!row[i].IsNull()) {
            key += row[i].ToString();
        }
    }
    return key;
}

// Generate deterministic UUID v5 for Vorlauf based on pre-computed row keys
static string GenerateVorlaufUUID(const vector<string> &row_keys,
                                   const string &gui_verfahren_id) {
    size_t total_size = 9 + gui_verfahren_id.size() + 1;
    for (const auto &key : row_keys) {
        total_size += key.size() + 2;
    }
    string combined_key;
    combined_key.reserve(total_size);
    combined_key += "vorlauf:";
    combined_key += gui_verfahren_id;
    combined_key += ":";
    for (size_t r = 0; r < row_keys.size(); r++) {
        if (r > 0) combined_key += "||";
        combined_key += row_keys[r];
    }
    return GenerateUUIDv5(combined_key);
}

// Generate deterministic UUID v5 for a monthly Vorlauf based on pre-computed row keys
static string GenerateMonthVorlaufUUID(const vector<string> &row_keys,
                                        const vector<idx_t> &row_indices,
                                        const string &gui_verfahren_id,
                                        const string &year_month) {
    size_t total_size = 9 + gui_verfahren_id.size() + 7 + year_month.size() + 1;
    for (const auto &idx : row_indices) {
        total_size += row_keys[idx].size() + 2;
    }
    string combined_key;
    combined_key.reserve(total_size);
    combined_key += "vorlauf:";
    combined_key += gui_verfahren_id;
    combined_key += ":month:";
    combined_key += year_month;
    combined_key += ":";
    for (size_t i = 0; i < row_indices.size(); i++) {
        if (i > 0) combined_key += "||";
        combined_key += row_keys[row_indices[i]];
    }
    return GenerateUUIDv5(combined_key);
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
// Helper: Pre-compute all primanota IDs for a set of rows.
// If guiPrimanotaID column exists and is non-null, use it; otherwise generate UUID v5.
// ============================================================================

static vector<string> PreComputePrimanotaIds(const vector<vector<Value>> &rows,
                                              const vector<string> &row_keys,
                                              size_t count,
                                              const idx_t *row_indices,
                                              idx_t primanota_id_col) {
    vector<string> ids;
    ids.reserve(count);
    for (size_t i = 0; i < count; i++) {
        size_t row_idx = row_indices ? row_indices[i] : i;
        const auto &row = rows[row_idx];
        if (primanota_id_col != DConstants::INVALID_INDEX && primanota_id_col < row.size()
            && !row[primanota_id_col].IsNull()) {
            ids.push_back(row[primanota_id_col].ToString());
        } else {
            ids.push_back(GenerateUUIDv5(row_keys[row_idx]));
        }
    }
    return ids;
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
// Helper: Group source rows by YYYY-MM month key (for monatsvorlauf)
// Returns std::map for automatic chronological ordering.
// ============================================================================

static std::map<string, vector<idx_t>> GroupRowsByMonth(const vector<vector<Value>> &rows,
                                                         const vector<string> &columns) {
    std::map<string, vector<idx_t>> month_groups;

    idx_t date_col = FindColumnWithAliases(columns, "dtmBelegDatum");

    // Compute current month fallback once
    string current_ym = ExtractYearMonth(GetCurrentDate());

    for (idx_t i = 0; i < rows.size(); i++) {
        string ym;
        if (date_col != DConstants::INVALID_INDEX && date_col < rows[i].size() && !rows[i][date_col].IsNull()) {
            string date_str = rows[i][date_col].ToString();
            ym = ExtractYearMonth(date_str);
        }
        if (ym.empty()) {
            ym = current_ym;
        }
        month_groups[ym].push_back(i);
    }

    return month_groups;
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
// Helper: Convert a Value to VARCHAR Value for staging table (explicit conversion)
// ============================================================================

static inline Value ToVarchar(const Value &val) {
    if (val.IsNull()) return Value();
    return Value(val.ToString());
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
    bind_data->monatsvorlauf = false;
    bind_data->skip_duplicate_check = false;
    bind_data->skip_fk_check = false;
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
        } else if (kv.first == "monatsvorlauf") {
            bind_data->monatsvorlauf = kv.second.GetValue<bool>();
        } else if (kv.first == "skip_duplicate_check") {
            bind_data->skip_duplicate_check = kv.second.GetValue<bool>();
        } else if (kv.first == "skip_fk_check") {
            bind_data->skip_fk_check = kv.second.GetValue<bool>();
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
    names = {"table_name", "rows_inserted", "gui_vorlauf_id", "duration", "success", "error_message"};
    return_types = {
        LogicalType::VARCHAR,   // table_name
        LogicalType::BIGINT,    // rows_inserted
        LogicalType::VARCHAR,   // gui_vorlauf_id
        LogicalType::VARCHAR,   // duration (hh:mm:ss)
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
    result.duration = "00:00:00";
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
    result.duration = FormatDuration(duration);
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
        output.SetValue(3, count, Value(result.duration));
        output.SetValue(4, count, Value(result.success));
        output.SetValue(5, count, Value(result.error_message));
        global_state.current_idx++;
        count++;
    }
    output.SetCardinality(count);
}

// Forward declaration (defined after staging table helpers)
static void DropStagingTable(Connection &staging_conn);

// ============================================================================
// Helper: Clean up open connections, add error, and jump to OUTPUT_RESULTS.
// Handles both staging table drop and transaction rollback.
// ============================================================================

static void CleanupAndError(IntoWzGlobalState &state, IntoWzBindData &bind_data,
                             DataChunk &output, const string &table_name,
                             const string &error_message, const string &vorlauf_id = "") {
    if (state.txn_conn) {
        string rb_err;
        ExecuteMssqlStatementWithConn(*state.txn_conn, "ROLLBACK", rb_err);
        state.txn_conn.reset();
    }
    if (state.staging_conn) {
        DropStagingTable(*state.staging_conn);
        state.staging_conn.reset();
    }
    AddErrorResult(bind_data, table_name, error_message, vorlauf_id);
    state.phase = ExecutionPhase::OUTPUT_RESULTS;
    OutputResults(bind_data, state, output);
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
// Populate local staging table with Primanota rows using DuckDB Appender.
// Handles both all-rows (row_indices=nullptr) and subset modes.
// The staging table must already exist. Caller manages create/drop.
// ============================================================================

static bool PopulateStagingTable(Connection &staging_conn,
                                  IntoWzBindData &bind_data,
                                  const vector<idx_t> *row_indices,
                                  const vector<string> &primanota_ids,
                                  const string &vorlauf_id,
                                  const string &verfahren_id,
                                  const string &str_angelegt,
                                  const string &vorlauf_datum_bis,
                                  const string &timestamp,
                                  const string &date_fallback,
                                  const PrimanotaColumnIndices &col_idx,
                                  string &error_message,
                                  const string &year_month = "") {
    size_t count = row_indices ? row_indices->size() : bind_data.source_rows.size();
    if (count == 0) return true;

    static const Value null_value;

    try {
        Appender appender(staging_conn, string(STAGING_TABLE_NAME));

        for (size_t i = 0; i < count; i++) {
            size_t row_idx = row_indices ? (*row_indices)[i] : i;
            const auto &row = bind_data.source_rows[row_idx];

            auto getValue = [&row](idx_t idx) -> const Value& {
                static const Value nv;
                if (idx == DConstants::INVALID_INDEX || idx >= row.size()) return nv;
                return row[idx];
            };

            const string &primanota_id = primanota_ids[i];

            const Value &beleg_datum_val = getValue(col_idx.col_beleg_datum);
            string beleg_datum = beleg_datum_val.IsNull() ? date_fallback : beleg_datum_val.ToString();
            if (beleg_datum.length() > 10) beleg_datum = beleg_datum.substr(0, 10);
            beleg_datum += " 00:00:00";

            bool is_soll = ParseSollHaben(getValue(col_idx.col_ysn_soll));
            const Value &eingabe_betrag_val = getValue(col_idx.col_eingabe_betrag);
            const Value &basis_betrag_ref = getValue(col_idx.col_basis_betrag);
            const Value &basis_betrag_val = (basis_betrag_ref.IsNull() && !eingabe_betrag_val.IsNull())
                ? eingabe_betrag_val : basis_betrag_ref;

            appender.BeginRow();
            appender.Append<int32_t>(0);                              // lngTimestamp
            appender.Append(Value(str_angelegt));                     // strAngelegt
            appender.Append(Value(timestamp));                        // dtmAngelegt
            appender.Append(Value());                                 // strGeaendert (NULL)
            appender.Append(Value());                                 // dtmGeaendert (NULL)
            appender.Append(Value(primanota_id));                     // guiPrimanotaID
            appender.Append(Value(vorlauf_id));                       // guiVorlaufID
            appender.Append<int32_t>(1);                              // lngStatus
            appender.Append(Value());                                 // lngZeilenNr (NULL)
            appender.Append<int32_t>(1);                              // lngEingabeWaehrungID
            appender.Append(Value());                                 // lngBu (NULL)
            appender.Append(ToVarchar(getValue(col_idx.col_gegenkonto_nr)));  // decGegenkontoNr
            appender.Append(ToVarchar(getValue(col_idx.col_konto_nr)));       // decKontoNr
            appender.Append(ToVarchar(getValue(col_idx.col_ea_konto_nr)));    // decEaKontoNr
            appender.Append(Value(vorlauf_datum_bis));                // dtmVorlaufDatumBis
            appender.Append(Value(beleg_datum));                      // dtmBelegDatum
            appender.Append<int32_t>(is_soll ? 1 : 0);               // ysnSoll
            appender.Append(ToVarchar(eingabe_betrag_val));           // curEingabeBetrag
            appender.Append(ToVarchar(basis_betrag_val));             // curBasisBetrag
            appender.Append(Value());                                 // curSkontoBetrag (NULL)
            appender.Append(Value());                                 // curSkontoBasisBetrag (NULL)
            appender.Append(Value());                                 // decKostMenge (NULL)
            appender.Append(Value());                                 // decWaehrungskurs (NULL)
            appender.Append(ToVarchar(getValue(col_idx.col_beleg1))); // strBeleg1
            appender.Append(ToVarchar(getValue(col_idx.col_beleg2))); // strBeleg2
            appender.Append(ToVarchar(getValue(col_idx.col_buch_text))); // strBuchText
            appender.Append(Value());                                 // strKost1 (NULL)
            appender.Append(Value());                                 // strKost2 (NULL)
            appender.Append(Value());                                 // strEuLand (NULL)
            appender.Append(Value());                                 // strUstId (NULL)
            appender.Append(Value());                                 // decEuSteuersatz (NULL)
            appender.Append(Value());                                 // dtmZusatzDatum (NULL)
            appender.Append(Value(verfahren_id));                     // guiVerfahrenID
            appender.Append(Value());                                 // decEaSteuersatz (NULL)
            appender.Append<int32_t>(0);                              // ysnEaTransaktionenManuell
            appender.Append(Value());                                 // decEaNummer (NULL)
            appender.Append(Value());                                 // lngSachverhalt13b (NULL)
            appender.Append(Value());                                 // dtmLeistung (NULL)
            appender.Append<int32_t>(0);                              // ysnIstversteuerungInSollversteuerung
            appender.Append(Value());                                 // lngSkontoSachverhaltWarenRHB (NULL)
            appender.Append<int32_t>(0);                              // ysnVStBeiZahlung
            appender.Append(Value());                                 // guiParentPrimanota (NULL)
            appender.Append<int32_t>(0);                              // ysnGeneralUmkehr
            appender.Append(Value());                                 // decSteuersatzManuell (NULL)
            appender.Append<int32_t>(0);                              // ysnMitUrsprungsland
            appender.Append(Value());                                 // strUrsprungsland (NULL)
            appender.Append(Value());                                 // strUrsprungslandUstId (NULL)
            appender.Append(Value());                                 // decUrsprungslandSteuersatz (NULL)
            appender.Append(year_month.empty() ? Value() : Value(year_month));  // year_month (filtering only)
            appender.EndRow();
        }

        appender.Close();
    } catch (const std::exception &e) {
        error_message = "Failed to populate staging table: " + string(e.what());
        return false;
    }

    return true;
}

// ============================================================================
// BCP-based bulk transfer: export staging to CSV, invoke bcp.exe
// ============================================================================

// Parse MSSQL connection string into components.
// Supports both ODBC-style "Server=x;Database=y;Uid=z;Pwd=w" and
// key variations (UID/User ID, PWD/Password, etc.)
struct MssqlConnInfo {
    string server;
    string database;
    string user;
    string password;
    bool trusted;  // Windows authentication
};

static bool ParseConnectionString(const string &conn_str, MssqlConnInfo &info) {
    info.trusted = false;
    // Split by semicolons, parse key=value pairs
    std::istringstream ss(conn_str);
    string token;
    while (std::getline(ss, token, ';')) {
        auto eq_pos = token.find('=');
        if (eq_pos == string::npos) continue;
        string key = token.substr(0, eq_pos);
        string val = token.substr(eq_pos + 1);
        // Trim whitespace
        while (!key.empty() && key.front() == ' ') key.erase(key.begin());
        while (!key.empty() && key.back() == ' ') key.pop_back();
        // Case-insensitive key matching
        string lower_key;
        lower_key.resize(key.size());
        std::transform(key.begin(), key.end(), lower_key.begin(), ::tolower);

        if (lower_key == "server" || lower_key == "data source") {
            info.server = val;
        } else if (lower_key == "database" || lower_key == "initial catalog") {
            info.database = val;
        } else if (lower_key == "uid" || lower_key == "user id" || lower_key == "user") {
            info.user = val;
        } else if (lower_key == "pwd" || lower_key == "password") {
            info.password = val;
        } else if (lower_key == "trusted_connection" || lower_key == "integrated security") {
            string lower_val;
            lower_val.resize(val.size());
            std::transform(val.begin(), val.end(), lower_val.begin(), ::tolower);
            if (lower_val == "yes" || lower_val == "true" || lower_val == "sspi") {
                info.trusted = true;
            }
        }
    }
    return !info.server.empty() && !info.database.empty() &&
           (info.trusted || (!info.user.empty() && !info.password.empty()));
}

// Try to extract MSSQL connection info from a DuckDB secret using the C++ SecretManager API.
// This bypasses the duckdb_secrets(redact=false) SQL function which is blocked by default in v1.4+.
static bool GetMssqlConnInfo(ClientContext &context, const string &secret_name, MssqlConnInfo &info) {
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &secret_manager = SecretManager::Get(db);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

    // Helper: extract host/database/user/password from a KeyValueSecret
    auto extract_info = [&](const KeyValueSecret *kv) -> bool {
        info = MssqlConnInfo();  // reset
        Value val;
        if (kv->TryGetValue("host", val) && !val.IsNull()) info.server = val.ToString();
        if (kv->TryGetValue("database", val) && !val.IsNull()) info.database = val.ToString();
        if (kv->TryGetValue("user", val) && !val.IsNull()) info.user = val.ToString();
        if (kv->TryGetValue("password", val) && !val.IsNull()) info.password = val.ToString();
        // Append non-default port to server (bcp uses "server,port" syntax)
        if (kv->TryGetValue("port", val) && !val.IsNull()) {
            string port_str = val.ToString();
            if (!port_str.empty() && port_str != "1433") {
                info.server += "," + port_str;
            }
        }
        return !info.server.empty() && !info.database.empty() &&
               (info.trusted || (!info.user.empty() && !info.password.empty()));
    };

    // Try 1: Look up secret by exact name (works when user passes the actual secret name)
    auto entry = secret_manager.GetSecretByName(transaction, secret_name);
    if (entry) {
        auto *kv = dynamic_cast<const KeyValueSecret *>(entry->secret.get());
        if (kv && extract_info(kv)) return true;
    }

    // Try 2: Scan all secrets for the first MSSQL-type secret
    // (handles the common case where secret param is the DB alias, not the secret name)
    auto all = secret_manager.AllSecrets(transaction);
    for (auto &e : all) {
        string type = e.secret->GetType();
        std::transform(type.begin(), type.end(), type.begin(), ::tolower);
        if (type == "mssql") {
            auto *kv = dynamic_cast<const KeyValueSecret *>(e.secret.get());
            if (kv && extract_info(kv)) return true;
        }
    }

    return false;
}

// Export staging table to a tab-separated file for bcp.
static bool ExportStagingToCsv(Connection &staging_conn, const string &csv_path,
                                 string &error_message,
                                 const string &year_month_filter = "") {
    string select_cols = string(PRIMANOTA_COLUMN_LIST);
    string sql = "COPY (SELECT " + select_cols + " FROM " + STAGING_TABLE_NAME;
    if (!year_month_filter.empty()) {
        sql += " WHERE year_month = '" + EscapeSqlString(year_month_filter) + "'";
    }
    sql += ") TO '" + csv_path + "' (DELIMITER '\t', HEADER false, NULL '', QUOTE '')";

    auto result = staging_conn.Query(sql);
    if (result->HasError()) {
        error_message = "Failed to export staging to CSV: " + result->GetError();
        return false;
    }
    return true;
}

// Generate a BCP format file for character-mode import.
// Derives column names from PRIMANOTA_COLUMN_LIST (single source of truth).
// Uses \n (LF) as the row terminator to match DuckDB COPY TO output.
// Without this, BCP defaults to \r\n which misparses the last column.
static bool GenerateBcpFormatFile(const string &fmt_path, string &error_message) {
    // Parse column names from PRIMANOTA_COLUMN_LIST
    vector<string> col_names;
    std::istringstream cols(PRIMANOTA_COLUMN_LIST);
    string col;
    while (std::getline(cols, col, ',')) {
        // Trim whitespace
        while (!col.empty() && col.front() == ' ') col.erase(col.begin());
        while (!col.empty() && col.back() == ' ') col.pop_back();
        if (!col.empty()) col_names.push_back(col);
    }

    std::ofstream f(fmt_path);
    if (!f.is_open()) {
        error_message = "Failed to create BCP format file: " + fmt_path;
        return false;
    }

    int num_cols = static_cast<int>(col_names.size());
    f << "14.0\n";
    f << num_cols << "\n";
    for (int i = 0; i < num_cols; i++) {
        const char *terminator = (i < num_cols - 1) ? "\\t" : "\\n";
        f << (i + 1) << "       SQLCHAR       0       8000      \""
          << terminator << "\"     " << (i + 1) << "     " << col_names[i] << "       \"\"\n";
    }

    f.close();
    return true;
}

// Invoke bcp.exe to bulk-load a data file into a table using a format file.
// Returns true on success, sets error_message on failure.
static bool InvokeBcp(const MssqlConnInfo &info, const string &table_name,
                       const string &csv_path, const string &fmt_path,
                       string &error_message, int64_t &rows_loaded) {
    rows_loaded = 0;

    // Build bcp command using format file for correct \n row terminator
    // bcp <db>.dbo.<table> in <file> -S <server> -f <fmt> -k -b 5000
    string cmd = "bcp " + info.database + ".dbo." + table_name + " in \"" + csv_path + "\"" +
                 " -S " + info.server +
                 " -f \"" + fmt_path + "\" -k -b 5000";

    if (info.trusted) {
        cmd += " -T";
    } else {
        cmd += " -U " + info.user + " -P " + info.password;
    }

    // Redirect stderr to stdout to capture all output
    cmd += " 2>&1";

    // Execute and capture output
    string output;
#ifdef _WIN32
    FILE *pipe = _popen(cmd.c_str(), "r");
#else
    FILE *pipe = popen(cmd.c_str(), "r");
#endif
    if (!pipe) {
        error_message = "Failed to execute bcp command";
        return false;
    }

    char buffer[256];
    while (fgets(buffer, sizeof(buffer), pipe)) {
        output += buffer;
    }

#ifdef _WIN32
    int exit_code = _pclose(pipe);
#else
    int exit_code = pclose(pipe);
#endif

    if (exit_code != 0) {
        // Truncate output for error message
        if (output.size() > 500) output = output.substr(0, 500) + "...";
        error_message = "bcp failed (exit " + std::to_string(exit_code) + "): " + output;
        return false;
    }

    // Parse "N rows copied" from bcp output
    auto pos = output.find("rows copied");
    if (pos != string::npos) {
        // Walk backwards from "rows copied" to find the number
        auto num_end = pos;
        while (num_end > 0 && output[num_end - 1] == ' ') num_end--;
        auto num_start = num_end;
        while (num_start > 0 && std::isdigit(output[num_start - 1])) num_start--;
        if (num_start < num_end) {
            rows_loaded = std::stoll(output.substr(num_start, num_end - num_start));
        }
    }

    return true;
}

// Full bcp transfer: export staging → bcp → cleanup.
// Returns false if bcp is not available or fails (caller should fall back).
static bool BcpTransferPrimanota(Connection &staging_conn, ClientContext &context,
                                   const string &secret_name,
                                   string &error_message,
                                   int64_t &rows_transferred,
                                   const string &year_month_filter = "") {
    rows_transferred = 0;

    // 1. Get connection info from secret via C++ SecretManager API
    MssqlConnInfo conn_info;
    if (!GetMssqlConnInfo(context, secret_name, conn_info)) {
        error_message = "Could not extract MSSQL connection info from secret";
        return false;
    }

    // 2. Export staging table to temp TSV
    auto temp_dir = std::filesystem::temp_directory_path();
    string csv_path = (temp_dir / "wz_primanota_staging.csv").string();
    string fmt_path = (temp_dir / "wz_primanota_staging.fmt").string();

    if (!ExportStagingToCsv(staging_conn, csv_path, error_message, year_month_filter)) {
        return false;
    }

    // 3. Generate format file (defines \n row terminator to match DuckDB export)
    if (!GenerateBcpFormatFile(fmt_path, error_message)) {
        std::filesystem::remove(csv_path);
        return false;
    }

    // 4. Invoke bcp with format file
    bool success = InvokeBcp(conn_info, "tblPrimanota", csv_path, fmt_path, error_message, rows_transferred);

    // 5. Cleanup temp files
    std::filesystem::remove(csv_path);
    std::filesystem::remove(fmt_path);

    return success;
}

// ============================================================================
// Fallback bulk transfer: batched multi-row INSERT VALUES
// ============================================================================

// Read rows from the staging table and insert into MSSQL via batched
// multi-row INSERT VALUES statements.  DuckDB's MSSQL extension sends
// INSERT INTO SELECT as row-by-row TDS inserts.  Building VALUES tuples
// with 1000 rows/statement and 5 statements/round-trip gives ~19 round-trips
// for 95k rows instead of 95k.
static bool BulkTransferPrimanota(Connection &txn_conn,
                                   Connection &staging_conn,
                                   const string &db_name,
                                   string &error_message,
                                   const string &year_month_filter = "") {
    // Read rows from staging
    string select_sql = string("SELECT ") + PRIMANOTA_COLUMN_LIST +
                        " FROM " + STAGING_TABLE_NAME;
    if (!year_month_filter.empty()) {
        select_sql += " WHERE year_month = '" + EscapeSqlString(year_month_filter) + "'";
    }
    auto staging_result = staging_conn.Query(select_sql);
    if (staging_result->HasError()) {
        error_message = "Failed to read staging table: " + staging_result->GetError();
        return false;
    }

    idx_t col_count = staging_result->types.size();
    string insert_prefix = string("INSERT INTO ") + db_name + ".dbo.tblPrimanota (" +
                           PRIMANOTA_COLUMN_LIST + ") VALUES ";

    // Accumulate complete INSERT statements, send multiple per round-trip
    vector<string> pending_stmts;
    idx_t rows_in_batch = 0;
    // ~200 bytes per row × 1000 rows = ~200KB per statement
    string values_sql;
    values_sql.reserve(256 * 1024);

    auto flush_stmt = [&]() {
        if (rows_in_batch == 0) return;
        pending_stmts.push_back(insert_prefix + values_sql);
        rows_in_batch = 0;
        values_sql.clear();
    };

    auto flush_roundtrip = [&]() -> bool {
        if (pending_stmts.empty()) return true;
        // Concatenate statements with "; " for multi-statement batch
        string batch;
        size_t total_len = 0;
        for (auto &s : pending_stmts) total_len += s.size() + 2;
        batch.reserve(total_len);
        for (size_t i = 0; i < pending_stmts.size(); i++) {
            if (i > 0) batch += "; ";
            batch += pending_stmts[i];
        }
        pending_stmts.clear();
        if (!ExecuteMssqlStatementWithConn(txn_conn, batch, error_message)) {
            error_message = "Failed to insert batch into tblPrimanota: " + error_message;
            return false;
        }
        return true;
    };

    for (auto &chunk : staging_result->Collection().Chunks()) {
        idx_t row_count = chunk.size();
        for (idx_t row = 0; row < row_count; row++) {
            if (rows_in_batch > 0) {
                values_sql += ',';
            }
            values_sql += '(';
            for (idx_t col = 0; col < col_count; col++) {
                if (col > 0) values_sql += ',';
                Value val = chunk.data[col].GetValue(row);
                if (val.IsNull()) {
                    values_sql += "NULL";
                } else {
                    auto &type = staging_result->types[col];
                    if (type == LogicalType::INTEGER || type == LogicalType::BIGINT ||
                        type == LogicalType::SMALLINT || type == LogicalType::TINYINT) {
                        values_sql += val.ToString();
                    } else {
                        values_sql += '\'';
                        values_sql += EscapeSqlString(val.ToString());
                        values_sql += '\'';
                    }
                }
            }
            values_sql += ')';
            rows_in_batch++;

            if (rows_in_batch >= MSSQL_INSERT_BATCH_SIZE) {
                flush_stmt();
                if (pending_stmts.size() >= MSSQL_STMTS_PER_ROUNDTRIP) {
                    if (!flush_roundtrip()) return false;
                }
            }
        }
    }

    // Flush remaining rows
    flush_stmt();
    return flush_roundtrip();
}

// ============================================================================
// Parallel per-month Primanota transfer: one thread per month-batch, up to
// max_threads concurrent workers. Each thread gets its own Connection + TX.
// Returns per-month success/failure via out_results (mutex-protected).
// ============================================================================

struct MonthTransferResult {
    string year_month;
    string vorlauf_id;
    int64_t rows_transferred;
    double duration;
    bool success;
    string error_message;
};

static void ParallelTransferPrimanota(DatabaseInstance &db,
                                       const string &db_name,
                                       const vector<MonthInfo> &months,
                                       size_t max_threads,
                                       vector<MonthTransferResult> &out_results) {
    out_results.resize(months.size());
    std::mutex result_mutex;

    // Worker function: transfers one month's primanota rows
    auto worker = [&](size_t month_idx) {
        auto &mi = months[month_idx];
        MonthTransferResult res;
        res.year_month = mi.year_month;
        res.vorlauf_id = mi.vorlauf_id;
        res.rows_transferred = 0;
        res.success = false;

        auto start = std::chrono::high_resolution_clock::now();

        try {
            Connection conn(db);
            string err;

            // Begin per-month transaction
            if (!ExecuteMssqlStatementWithConn(conn, "BEGIN TRANSACTION", err)) {
                res.error_message = "Failed to begin transaction for month " + mi.year_month + ": " + err;
                goto done;
            }

            {
                // INSERT INTO mssql...tblPrimanota SELECT columns FROM staging WHERE year_month = '...'
                string sql = string("INSERT INTO ") + db_name + ".dbo.tblPrimanota (" +
                             PRIMANOTA_COLUMN_LIST + ") SELECT " + PRIMANOTA_COLUMN_LIST +
                             " FROM " + STAGING_TABLE_NAME +
                             " WHERE year_month = '" + EscapeSqlString(mi.year_month) + "'";

                if (!ExecuteMssqlStatementWithConn(conn, sql, err)) {
                    // Rollback this month's transaction
                    string rb_err;
                    ExecuteMssqlStatementWithConn(conn, "ROLLBACK", rb_err);
                    res.error_message = "Failed to transfer primanota for month " + mi.year_month + ": " + err;
                    goto done;
                }
            }

            // Commit this month
            if (!ExecuteMssqlStatementWithConn(conn, "COMMIT", err)) {
                res.error_message = "Failed to commit month " + mi.year_month + ": " + err;
                goto done;
            }

            res.rows_transferred = static_cast<int64_t>(mi.row_indices.size());
            res.success = true;
        } catch (const std::exception &e) {
            res.error_message = "Exception transferring month " + mi.year_month + ": " + string(e.what());
        }

    done:
        auto end = std::chrono::high_resolution_clock::now();
        res.duration = std::chrono::duration<double>(end - start).count();

        std::lock_guard<std::mutex> lock(result_mutex);
        out_results[month_idx] = std::move(res);
    };

    // Launch threads in waves of max_threads
    for (size_t wave_start = 0; wave_start < months.size(); wave_start += max_threads) {
        size_t wave_end = std::min(wave_start + max_threads, months.size());
        vector<std::thread> threads;
        threads.reserve(wave_end - wave_start);

        for (size_t i = wave_start; i < wave_end; i++) {
            threads.emplace_back(worker, i);
        }
        for (auto &t : threads) {
            t.join();
        }
    }
}

// ============================================================================
// Create and drop staging table helpers
// ============================================================================

static bool CreateStagingTable(Connection &staging_conn, string &error_message) {
    staging_conn.Query(string("DROP TABLE IF EXISTS ") + STAGING_TABLE_NAME);
    string sql = string("CREATE TABLE ") + STAGING_TABLE_NAME + " (" + STAGING_TABLE_DDL + ")";
    auto result = staging_conn.Query(sql);
    if (result->HasError()) {
        error_message = "Failed to create staging table: " + result->GetError();
        return false;
    }
    return true;
}

static void DropStagingTable(Connection &staging_conn) {
    staging_conn.Query(string("DROP TABLE IF EXISTS ") + STAGING_TABLE_NAME);
}

// ============================================================================
// Helper: Fetch AZ prefix from tblVerfahren (cacheable — same for all months)
// Returns the "az - XXX" prefix string; empty if not found.
// ============================================================================

static bool FetchAZPrefix(Connection &conn,
                          const string &db_name,
                          const string &gui_verfahren_id,
                          string &az_prefix,
                          string &error_message) {
    az_prefix.clear();

    string sql =
        "SELECT strAZGericht"
        " FROM " + db_name + ".dbo.tblVerfahren"
        " WHERE guiVerfahrenID = '" + EscapeSqlString(gui_verfahren_id) + "'";

    auto result = conn.Query(sql);
    if (result->HasError()) {
        error_message = "Failed to fetch strAZGericht: " + result->GetError();
        return false;
    }

    auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
    auto &collection = materialized->Collection();
    if (collection.Count() == 0) return true;

    auto chunk = materialized->Fetch();
    if (!chunk || chunk->size() == 0 || chunk->ColumnCount() < 1) return true;

    Value az_gericht_val = chunk->data[0].GetValue(0);
    if (az_gericht_val.IsNull()) return true;

    string str_az_gericht = az_gericht_val.ToString();
    az_prefix = "az - ";
    if (str_az_gericht.length() >= 6) {
        az_prefix += str_az_gericht.substr(str_az_gericht.length() - 6, 3);
    } else if (str_az_gericht.length() >= 3) {
        az_prefix += str_az_gericht.substr(0, 3);
    } else {
        az_prefix += str_az_gericht;
    }
    return true;
}

// Build bezeichnung from cached AZ prefix + date range
static string BuildBezeichnungFromPrefix(const string &az_prefix,
                                          const string &date_from,
                                          const string &date_to) {
    string min_date = date_from;
    string max_date = date_to;
    if (min_date.size() > 10) min_date = min_date.substr(0, 10);
    if (max_date.size() > 10) max_date = max_date.substr(0, 10);
    return az_prefix + " Vorlauf " + min_date + " - " + max_date;
}

// ============================================================================
// Batch Vorlauf creation: ~3 MSSQL round-trips instead of 2*N sequential ones.
// 1) SELECT IN (...) to find which IDs already exist
// 2) Multi-value INSERT for all new records
// 3) Batched UPDATE statements for existing ones
// Runs inside a caller-provided transaction on txn_conn.
// Returns per-month insert/update results via out_results.
// ============================================================================

struct VorlaufResult {
    string vorlauf_id;
    bool inserted;   // true=inserted, false=updated
    double duration;
};

static bool BatchCreateVorlaufRecords(Connection &txn_conn,
                                       const string &db_name,
                                       const string &gui_verfahren_id,
                                       int64_t konten_rahmen_id,
                                       const string &str_angelegt,
                                       const vector<MonthInfo> &months,
                                       vector<VorlaufResult> &out_results,
                                       string &error_message) {
    if (months.empty()) return true;

    auto batch_start = std::chrono::high_resolution_clock::now();
    string timestamp = GetCurrentTimestamp();

    // Step 1: Batch-check which vorlauf IDs already exist
    string in_clause;
    for (size_t i = 0; i < months.size(); i++) {
        if (i > 0) in_clause += ", ";
        in_clause += "'" + EscapeSqlString(months[i].vorlauf_id) + "'";
    }

    string check_sql = "SELECT CAST(guiVorlaufID AS VARCHAR) AS id FROM " +
                        db_name + ".dbo.tblVorlauf WHERE guiVorlaufID IN (" + in_clause + ")";

    std::set<string> existing_ids;
    {
        auto result = txn_conn.Query(check_sql);
        if (result->HasError()) {
            error_message = "Failed to check existing Vorlauf IDs: " + result->GetError();
            return false;
        }
        auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
        for (auto &chunk : materialized->Collection().Chunks()) {
            for (idx_t i = 0; i < chunk.size(); i++) {
                existing_ids.insert(chunk.data[0].GetValue(i).ToString());
            }
        }
    }

    // Step 2: Multi-value INSERT for all new vorlauf records
    vector<size_t> new_indices;
    vector<size_t> update_indices;
    for (size_t i = 0; i < months.size(); i++) {
        if (existing_ids.count(months[i].vorlauf_id)) {
            update_indices.push_back(i);
        } else {
            new_indices.push_back(i);
        }
    }

    if (!new_indices.empty()) {
        std::ostringstream sql;
        sql << "INSERT INTO " << db_name << ".dbo.tblVorlauf ("
            << "lngTimestamp, strAngelegt, dtmAngelegt, strGeaendert, dtmGeaendert, "
            << "guiVorlaufID, guiVerfahrenID, lngKanzleiKontenRahmenID, lngStatus, "
            << "dtmVorlaufDatumBis, dtmVorlaufDatumVon, lngVorlaufNr, strBezeichnung, "
            << "dtmDatevExport, ysnAutoBuSchluessel4stellig"
            << ") VALUES ";

        for (size_t j = 0; j < new_indices.size(); j++) {
            auto &mi = months[new_indices[j]];
            if (j > 0) sql << ", ";
            sql << "(0, "
                << "'" << EscapeSqlString(str_angelegt) << "', "
                << "'" << timestamp << "', "
                << "NULL, NULL, "
                << "'" << EscapeSqlString(mi.vorlauf_id) << "', "
                << "'" << EscapeSqlString(gui_verfahren_id) << "', "
                << konten_rahmen_id << ", 1, "
                << "'" << EscapeSqlString(mi.date_to) << " 00:00:00', "
                << "'" << EscapeSqlString(mi.date_from) << " 00:00:00', "
                << "NULL, "
                << "'" << EscapeSqlString(mi.bezeichnung) << "', "
                << "NULL, 0)";
        }

        if (!ExecuteMssqlStatementWithConn(txn_conn, sql.str(), error_message)) {
            error_message = "Failed to batch-insert Vorlauf records: " + error_message;
            return false;
        }
    }

    // Step 3: Batched UPDATE statements for existing ones
    if (!update_indices.empty()) {
        std::ostringstream sql;
        for (size_t j = 0; j < update_indices.size(); j++) {
            auto &mi = months[update_indices[j]];
            if (j > 0) sql << "; ";
            sql << "UPDATE " << db_name << ".dbo.tblVorlauf SET "
                << "strBezeichnung = '" << EscapeSqlString(mi.bezeichnung) << "', "
                << "dtmVorlaufDatumBis = '" << EscapeSqlString(mi.date_to) << " 00:00:00', "
                << "strGeaendert = '" << EscapeSqlString(str_angelegt) << "', "
                << "dtmGeaendert = '" << timestamp << "' "
                << "WHERE guiVorlaufID = '" << EscapeSqlString(mi.vorlauf_id) << "'";
        }

        if (!ExecuteMssqlStatementWithConn(txn_conn, sql.str(), error_message)) {
            error_message = "Failed to batch-update Vorlauf records: " + error_message;
            return false;
        }
    }

    auto batch_end = std::chrono::high_resolution_clock::now();
    double total_dur = std::chrono::duration<double>(batch_end - batch_start).count();

    // Build per-month results
    out_results.reserve(months.size());
    double per_month_dur = total_dur / static_cast<double>(months.size());
    for (size_t i = 0; i < months.size(); i++) {
        bool was_inserted = (existing_ids.count(months[i].vorlauf_id) == 0);
        out_results.push_back({months[i].vorlauf_id, was_inserted, per_month_dur});
    }

    return true;
}

// ============================================================================
// Helper: Derive strBezeichnung from MSSQL tblVerfahren data
// Uses the provided date range (from source data) and fetches strAZGericht from tblVerfahren.
// Returns true on success; leaves `bezeichnung` empty if no data; sets error_message on failure.
// ============================================================================

static bool DeriveBezeichnungFromMssql(Connection &conn,
                                       const string &db_name,
                                       const string &gui_verfahren_id,
                                       const string &date_from,
                                       const string &date_to,
                                       string &bezeichnung,
                                       string &error_message) {
    bezeichnung.clear();

    // Query tblVerfahren directly for strAZGericht - no need to check existing Primanota
    // Just fetch the raw value and build the prefix in C++ to avoid SQL dialect issues
    string sql =
        "SELECT strAZGericht"
        " FROM " + db_name + ".dbo.tblVerfahren"
        " WHERE guiVerfahrenID = '" + EscapeSqlString(gui_verfahren_id) + "'";

    auto result = conn.Query(sql);
    if (result->HasError()) {
        error_message = "Failed to derive strBezeichnung: " + result->GetError();
        return false;
    }

    auto materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
    auto &collection = materialized->Collection();
    if (collection.Count() == 0) {
        return true; // No Verfahren found; caller may fallback
    }

    // Use Fetch() to get a chunk safely
    auto chunk = materialized->Fetch();
    if (!chunk || chunk->size() == 0) {
        return true;
    }

    // Verify we have the column
    if (chunk->ColumnCount() < 1) {
        return true;
    }

    // Check for null values before accessing
    Value az_gericht_val = chunk->data[0].GetValue(0);

    if (az_gericht_val.IsNull()) {
        return true;
    }

    // Build az_prefix: equivalent to 'az - ' + LEFT(RIGHT(strAZGericht, 6), 3)
    // This extracts 3 characters starting 6 characters from the end
    string str_az_gericht = az_gericht_val.ToString();
    string az_prefix = "az - ";
    if (str_az_gericht.length() >= 6) {
        // RIGHT(strAZGericht, 6) = last 6 chars, then LEFT(..., 3) = first 3 of those
        az_prefix += str_az_gericht.substr(str_az_gericht.length() - 6, 3);
    } else if (str_az_gericht.length() >= 3) {
        // If less than 6 chars, just take first 3
        az_prefix += str_az_gericht.substr(0, 3);
    } else {
        // Use whatever is available
        az_prefix += str_az_gericht;
    }

    // Use the provided date range from source data
    string min_date = date_from;
    string max_date = date_to;

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
    auto &state = data_p.global_state->Cast<IntoWzGlobalState>();

    // Pipeline end: return empty chunk
    if (state.phase == ExecutionPhase::DONE) {
        output.SetCardinality(0);
        return;
    }

    // Drain accumulated result rows
    if (state.phase == ExecutionPhase::OUTPUT_RESULTS) {
        OutputResults(bind_data, state, output);
        if (output.size() == 0) {
            state.phase = ExecutionPhase::DONE;
        }
        state.progress = 100.0;
        return;
    }

    string error_msg;

    switch (state.phase) {

    // -----------------------------------------------------------------
    // VALIDATE_PARAMS  (0 → 5%)
    // -----------------------------------------------------------------
    case ExecutionPhase::VALIDATE_PARAMS: {
        state.total_start = std::chrono::high_resolution_clock::now();

        if (bind_data.gui_verfahren_id.empty()) {
            AddErrorResult(bind_data, "ERROR", "gui_verfahren_id is required");
            state.phase = ExecutionPhase::OUTPUT_RESULTS;
            OutputResults(bind_data, state, output);
            return;
        }
        if (bind_data.source_table.empty()) {
            AddErrorResult(bind_data, "ERROR", "source_table is required");
            state.phase = ExecutionPhase::OUTPUT_RESULTS;
            OutputResults(bind_data, state, output);
            return;
        }
        if (bind_data.lng_kanzlei_konten_rahmen_id <= 0) {
            AddErrorResult(bind_data, "ERROR", "lng_kanzlei_konten_rahmen_id is required and must be positive");
            state.phase = ExecutionPhase::OUTPUT_RESULTS;
            OutputResults(bind_data, state, output);
            return;
        }
        if (!IsValidSqlIdentifier(bind_data.secret_name)) {
            AddErrorResult(bind_data, "ERROR", "Invalid secret name: must contain only alphanumeric characters, underscores, and dots");
            state.phase = ExecutionPhase::OUTPUT_RESULTS;
            OutputResults(bind_data, state, output);
            return;
        }
        if (!IsValidUuidFormat(bind_data.gui_verfahren_id)) {
            AddErrorResult(bind_data, "ERROR", "Invalid gui_verfahren_id format: must be a valid UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)");
            state.phase = ExecutionPhase::OUTPUT_RESULTS;
            OutputResults(bind_data, state, output);
            return;
        }

        state.phase = ExecutionPhase::LOAD_DATA;
        state.progress = 5.0;
        // Emit a progress row to keep the pipeline alive
        AddSuccessResult(bind_data, "PROGRESS:validating", 0, "", 0.0);
        OutputResults(bind_data, state, output);
        return;
    }

    // -----------------------------------------------------------------
    // LOAD_DATA  (5 → 20%)  —  load source + compute row keys
    // -----------------------------------------------------------------
    case ExecutionPhase::LOAD_DATA: {
        auto load_start = std::chrono::high_resolution_clock::now();
        if (!LoadSourceData(context, bind_data, error_msg)) {
            AddErrorResult(bind_data, "ERROR", error_msg);
            state.phase = ExecutionPhase::OUTPUT_RESULTS;
            OutputResults(bind_data, state, output);
            return;
        }
        auto load_end = std::chrono::high_resolution_clock::now();
        double load_dur = std::chrono::duration<double>(load_end - load_start).count();
        AddSuccessResult(bind_data, "TIMING:load", static_cast<int64_t>(bind_data.source_rows.size()), "", load_dur);

        // Pre-compute row keys
        auto key_start = std::chrono::high_resolution_clock::now();
        bind_data.row_keys.reserve(bind_data.source_rows.size());
        for (const auto &row : bind_data.source_rows) {
            bind_data.row_keys.push_back(BuildRowKey(row));
        }
        auto key_end = std::chrono::high_resolution_clock::now();
        double key_dur = std::chrono::duration<double>(key_end - key_start).count();
        AddSuccessResult(bind_data, "TIMING:row_keys", static_cast<int64_t>(bind_data.row_keys.size()), "", key_dur);

        state.phase = ExecutionPhase::VALIDATE_DATA;
        state.progress = 20.0;
        OutputResults(bind_data, state, output);
        return;
    }

    // -----------------------------------------------------------------
    // VALIDATE_DATA  (20 → 35%)  —  duplicate check + FK validation
    // -----------------------------------------------------------------
    case ExecutionPhase::VALIDATE_DATA: {
        if (!bind_data.skip_duplicate_check) {
            if (!ValidateDuplicates(context, bind_data, error_msg)) {
                AddErrorResult(bind_data, "ERROR", error_msg);
                state.phase = ExecutionPhase::OUTPUT_RESULTS;
                OutputResults(bind_data, state, output);
                return;
            }
        }

        if (!bind_data.skip_fk_check) {
            if (!ValidateForeignKeys(context, bind_data.secret_name,
                                     bind_data.source_rows, bind_data.source_columns, error_msg)) {
                AddErrorResult(bind_data, "ERROR", error_msg);
                state.phase = ExecutionPhase::OUTPUT_RESULTS;
                OutputResults(bind_data, state, output);
                return;
            }
        }
        // Propagate FK validation warning
        if (!error_msg.empty()) {
            InsertResult warning;
            warning.table_name = "FK_VALIDATION";
            warning.rows_inserted = 0;
            warning.gui_vorlauf_id = "";
            warning.duration = "00:00:00";
            warning.success = true;
            warning.error_message = error_msg;
            bind_data.results.push_back(warning);
            error_msg.clear();
        }

        state.col_idx = PrimanotaColumnIndices::Build(bind_data.source_columns);

        state.phase = ExecutionPhase::PRE_COMPUTE;
        state.progress = 35.0;
        // Emit progress row if no FK warning was produced
        if (bind_data.results.size() <= state.current_idx) {
            AddSuccessResult(bind_data, "PROGRESS:validation", 0, "", 0.0);
        }
        OutputResults(bind_data, state, output);
        return;
    }

    // -----------------------------------------------------------------
    // PRE_COMPUTE  (35 → 50%)  —  UUIDs, month groups, AZ prefix
    // -----------------------------------------------------------------
    case ExecutionPhase::PRE_COMPUTE: {
        auto &db = DatabaseInstance::GetDatabase(context);
        auto uuid_start = std::chrono::high_resolution_clock::now();

        if (bind_data.monatsvorlauf) {
            // --- Monatsvorlauf pre-computation ---
            auto month_groups = GroupRowsByMonth(bind_data.source_rows, bind_data.source_columns);

            // Cache AZ prefix once
            {
                Connection fetch_conn(db);
                if (!FetchAZPrefix(fetch_conn, bind_data.secret_name, bind_data.gui_verfahren_id, state.cached_az_prefix, error_msg)) {
                    AddErrorResult(bind_data, "ERROR", error_msg);
                    state.phase = ExecutionPhase::OUTPUT_RESULTS;
                    OutputResults(bind_data, state, output);
                    return;
                }
            }

            state.months.reserve(month_groups.size());
            for (auto &[year_month, row_indices] : month_groups) {
                MonthInfo mi;
                mi.year_month = year_month;
                mi.row_indices = std::move(row_indices);
                mi.date_from = year_month + "-01";
                mi.date_to = GetLastDayOfMonth(year_month);

                // Vorlauf UUID
                if (!bind_data.generate_vorlauf_id) {
                    idx_t vorlauf_id_col = FindVorlaufIdColumn(bind_data.source_columns);
                    if (vorlauf_id_col != DConstants::INVALID_INDEX
                        && vorlauf_id_col < bind_data.source_rows[mi.row_indices[0]].size()
                        && !bind_data.source_rows[mi.row_indices[0]][vorlauf_id_col].IsNull()) {
                        string source_id = bind_data.source_rows[mi.row_indices[0]][vorlauf_id_col].ToString();
                        mi.vorlauf_id = GenerateUUIDv5("vorlauf:" + source_id + ":month:" + year_month);
                    }
                }
                if (mi.vorlauf_id.empty()) {
                    mi.vorlauf_id = GenerateMonthVorlaufUUID(
                        bind_data.row_keys, mi.row_indices,
                        bind_data.gui_verfahren_id, year_month);
                }

                // Bezeichnung
                if (!state.cached_az_prefix.empty()) {
                    mi.bezeichnung = BuildBezeichnungFromPrefix(state.cached_az_prefix, mi.date_from, mi.date_to);
                } else {
                    mi.bezeichnung = DeriveMonthVorlaufBezeichnung(year_month);
                }

                // Primanota IDs
                mi.primanota_ids = PreComputePrimanotaIds(
                    bind_data.source_rows, bind_data.row_keys,
                    mi.row_indices.size(), mi.row_indices.data(),
                    state.col_idx.col_primanota_id);

                state.months.push_back(std::move(mi));
            }
        } else {
            // --- Single-vorlauf pre-computation ---
            if (!bind_data.generate_vorlauf_id) {
                idx_t vorlauf_id_col = FindVorlaufIdColumn(bind_data.source_columns);
                if (vorlauf_id_col != DConstants::INVALID_INDEX
                    && !bind_data.source_rows.empty()
                    && vorlauf_id_col < bind_data.source_rows[0].size()
                    && !bind_data.source_rows[0][vorlauf_id_col].IsNull()) {
                    state.vorlauf_id = bind_data.source_rows[0][vorlauf_id_col].ToString();
                    state.vorlauf_id_from_source = true;
                }
            }
            if (state.vorlauf_id.empty()) {
                state.vorlauf_id = GenerateVorlaufUUID(bind_data.row_keys, bind_data.gui_verfahren_id);
            }

            auto date_range = FindDateRange(bind_data.source_rows, bind_data.source_columns);
            state.date_from = date_range.first;
            state.date_to = date_range.second;
            if (state.date_from.empty()) state.date_from = GetCurrentMonthStart();
            if (state.date_to.empty()) state.date_to = GetCurrentDate();

            state.bezeichnung = DeriveVorlaufBezeichnung(state.date_from, state.date_to);

            // Pre-compute primanota IDs
            state.primanota_ids = PreComputePrimanotaIds(
                bind_data.source_rows, bind_data.row_keys,
                bind_data.source_rows.size(), nullptr, state.col_idx.col_primanota_id);
        }

        auto uuid_end = std::chrono::high_resolution_clock::now();
        double uuid_dur = std::chrono::duration<double>(uuid_end - uuid_start).count();
        AddSuccessResult(bind_data, "TIMING:uuid_gen", static_cast<int64_t>(bind_data.source_rows.size()), "", uuid_dur);

        state.phase = ExecutionPhase::POPULATE_STAGING;
        state.progress = 50.0;
        OutputResults(bind_data, state, output);
        return;
    }

    // -----------------------------------------------------------------
    // POPULATE_STAGING  (50 → 65%)
    // -----------------------------------------------------------------
    case ExecutionPhase::POPULATE_STAGING: {
        auto &db = DatabaseInstance::GetDatabase(context);
        auto staging_start = std::chrono::high_resolution_clock::now();

        state.staging_conn = make_uniq<Connection>(db);
        if (!CreateStagingTable(*state.staging_conn, error_msg)) {
            state.staging_conn.reset();
            AddErrorResult(bind_data, "ERROR", error_msg);
            state.phase = ExecutionPhase::OUTPUT_RESULTS;
            OutputResults(bind_data, state, output);
            return;
        }

        string timestamp = GetCurrentTimestamp();
        string date_fallback = timestamp.substr(0, 10);

        if (bind_data.monatsvorlauf) {
            for (auto &mi : state.months) {
                string vorlauf_datum_bis = mi.date_to + " 00:00:00";
                if (!PopulateStagingTable(*state.staging_conn, bind_data,
                                           &mi.row_indices, mi.primanota_ids,
                                           mi.vorlauf_id, bind_data.gui_verfahren_id,
                                           bind_data.str_angelegt, vorlauf_datum_bis,
                                           timestamp, date_fallback, state.col_idx, error_msg,
                                           mi.year_month)) {
                    CleanupAndError(state, bind_data, output, "ERROR", error_msg);
                    return;
                }
            }
        } else {
            string vorlauf_datum_bis = state.date_to + " 00:00:00";
            if (!PopulateStagingTable(*state.staging_conn, bind_data,
                                       nullptr, state.primanota_ids,
                                       state.vorlauf_id, bind_data.gui_verfahren_id,
                                       bind_data.str_angelegt, vorlauf_datum_bis,
                                       timestamp, date_fallback, state.col_idx, error_msg)) {
                CleanupAndError(state, bind_data, output, "ERROR", error_msg);
                return;
            }
        }

        auto staging_end = std::chrono::high_resolution_clock::now();
        double staging_dur = std::chrono::duration<double>(staging_end - staging_start).count();
        AddSuccessResult(bind_data, "TIMING:staging", static_cast<int64_t>(bind_data.source_rows.size()), "", staging_dur);

        state.phase = ExecutionPhase::VORLAUF_RECORDS;
        state.progress = 65.0;
        OutputResults(bind_data, state, output);
        return;
    }

    // -----------------------------------------------------------------
    // VORLAUF_RECORDS  (65 → 80%)
    // -----------------------------------------------------------------
    case ExecutionPhase::VORLAUF_RECORDS: {
        auto &db = DatabaseInstance::GetDatabase(context);

        if (bind_data.monatsvorlauf) {
            // Batch Vorlauf: BEGIN TX → batch insert/update → COMMIT
            auto vorlauf_batch_start = std::chrono::high_resolution_clock::now();

            Connection txn_conn(db);
            if (!ExecuteMssqlStatementWithConn(txn_conn, "BEGIN TRANSACTION", error_msg)) {
                CleanupAndError(state, bind_data, output, "ERROR", "Failed to begin vorlauf transaction: " + error_msg);
                return;
            }

            vector<VorlaufResult> vorlauf_results;
            if (!BatchCreateVorlaufRecords(txn_conn, bind_data.secret_name,
                                            bind_data.gui_verfahren_id,
                                            bind_data.lng_kanzlei_konten_rahmen_id,
                                            bind_data.str_angelegt, state.months,
                                            vorlauf_results, error_msg)) {
                string rb_err;
                ExecuteMssqlStatementWithConn(txn_conn, "ROLLBACK", rb_err);
                CleanupAndError(state, bind_data, output, "tblVorlauf", error_msg);
                return;
            }

            if (!ExecuteMssqlStatementWithConn(txn_conn, "COMMIT", error_msg)) {
                CleanupAndError(state, bind_data, output, "ERROR", "Failed to commit vorlauf batch: " + error_msg);
                return;
            }

            for (auto &vr : vorlauf_results) {
                if (vr.inserted) {
                    AddSuccessResult(bind_data, "tblVorlauf", 1, vr.vorlauf_id, vr.duration);
                } else {
                    AddSuccessResult(bind_data, "tblVorlauf (updated)", 0, vr.vorlauf_id, vr.duration);
                }
            }

            auto vorlauf_batch_end = std::chrono::high_resolution_clock::now();
            double vorlauf_batch_dur = std::chrono::duration<double>(vorlauf_batch_end - vorlauf_batch_start).count();
            AddSuccessResult(bind_data, "TIMING:vorlauf_batch", static_cast<int64_t>(state.months.size()), "", vorlauf_batch_dur);

        } else {
            // Single-vorlauf: BEGIN TX → insert/update (TX stays open for TRANSFER phase)
            state.txn_conn = make_uniq<Connection>(db);

            if (!ExecuteMssqlStatementWithConn(*state.txn_conn, "BEGIN TRANSACTION", error_msg)) {
                state.txn_conn.reset();
                CleanupAndError(state, bind_data, output, "ERROR", "Failed to begin transaction: " + error_msg);
                return;
            }

            // Derive bezeichnung from MSSQL
            string derived_bezeichnung;
            if (!DeriveBezeichnungFromMssql(*state.txn_conn, bind_data.secret_name, bind_data.gui_verfahren_id,
                                             state.date_from, state.date_to, derived_bezeichnung, error_msg)) {
                CleanupAndError(state, bind_data, output, "tblVorlauf", error_msg, state.vorlauf_id);
                return;
            }
            if (!derived_bezeichnung.empty()) {
                state.bezeichnung = std::move(derived_bezeichnung);
            }

            // Check if vorlauf exists (for source-provided IDs)
            if (state.vorlauf_id_from_source) {
                bool exists = false;
                if (!VorlaufExists(*state.txn_conn, bind_data.secret_name, state.vorlauf_id, exists, error_msg)) {
                    CleanupAndError(state, bind_data, output, "tblVorlauf", error_msg, state.vorlauf_id);
                    return;
                }
                state.skip_vorlauf_insert = exists;
            }

            // Insert or update Vorlauf
            if (!state.skip_vorlauf_insert) {
                auto vorlauf_start = std::chrono::high_resolution_clock::now();
                if (!InsertVorlauf(*state.txn_conn, bind_data, state.vorlauf_id, state.date_from, state.date_to, state.bezeichnung, error_msg)) {
                    CleanupAndError(state, bind_data, output, "tblVorlauf", error_msg, state.vorlauf_id);
                    return;
                }
                auto vorlauf_end = std::chrono::high_resolution_clock::now();
                double vorlauf_dur = std::chrono::duration<double>(vorlauf_end - vorlauf_start).count();
                AddSuccessResult(bind_data, "tblVorlauf", 1, state.vorlauf_id, vorlauf_dur);
            } else {
                auto vorlauf_start = std::chrono::high_resolution_clock::now();
                if (!UpdateVorlauf(*state.txn_conn, bind_data.secret_name, state.vorlauf_id, state.date_to, state.bezeichnung, bind_data.str_angelegt, error_msg)) {
                    CleanupAndError(state, bind_data, output, "tblVorlauf", error_msg, state.vorlauf_id);
                    return;
                }
                auto vorlauf_end = std::chrono::high_resolution_clock::now();
                double vorlauf_dur = std::chrono::duration<double>(vorlauf_end - vorlauf_start).count();
                AddSuccessResult(bind_data, "tblVorlauf (updated)", 0, state.vorlauf_id, vorlauf_dur);
            }
            // txn_conn stays open — TRANSFER_PRIMANOTA will COMMIT
        }

        state.phase = ExecutionPhase::TRANSFER_PRIMANOTA;
        state.progress = 80.0;
        OutputResults(bind_data, state, output);
        return;
    }

    // -----------------------------------------------------------------
    // TRANSFER_PRIMANOTA  (80 → 95%)
    //   Monatsvorlauf: one month per Execute call (sequential MSSQL inserts
    //   avoid deadlocks on the shared attached-database connection).
    //   Single-vorlauf: one bulk transfer.
    // -----------------------------------------------------------------
    case ExecutionPhase::TRANSFER_PRIMANOTA: {
        auto &db = DatabaseInstance::GetDatabase(context);

        if (bind_data.monatsvorlauf) {
            auto transfer_start = std::chrono::high_resolution_clock::now();
            int64_t total_transferred = 0;
            bool used_bcp = false;

            // Try bcp first — orders of magnitude faster than SQL INSERT
            string bcp_err;
            int64_t bcp_rows = 0;

            // bcp transfers all months at once (no year_month filter needed)
            if (BcpTransferPrimanota(*state.staging_conn, context, bind_data.secret_name,
                                      bcp_err, bcp_rows)) {
                used_bcp = true;
                total_transferred = bcp_rows;
                auto bcp_end = std::chrono::high_resolution_clock::now();
                double bcp_dur = std::chrono::duration<double>(bcp_end - transfer_start).count();
                AddSuccessResult(bind_data, "tblPrimanota (bcp)", total_transferred, "", bcp_dur);
                state.progress = 95.0;
            } else {
                // bcp failed — fall back to batched INSERT VALUES
                AddSuccessResult(bind_data, "INFO:bcp_fallback", 0, "", 0);

                Connection conn(db);
                for (size_t mi_idx = 0; mi_idx < state.months.size(); mi_idx++) {
                    auto &mi = state.months[mi_idx];
                    auto month_start = std::chrono::high_resolution_clock::now();

                    if (!ExecuteMssqlStatementWithConn(conn, "BEGIN TRANSACTION", error_msg)) {
                        CleanupAndError(state, bind_data, output, "tblPrimanota",
                            "Failed to begin transaction for month " + mi.year_month + ": " + error_msg, mi.vorlauf_id);
                        return;
                    }

                    if (!BulkTransferPrimanota(conn, *state.staging_conn, bind_data.secret_name,
                                                error_msg, mi.year_month)) {
                        string rb_err;
                        ExecuteMssqlStatementWithConn(conn, "ROLLBACK", rb_err);
                        CleanupAndError(state, bind_data, output, "tblPrimanota",
                            "Failed to transfer primanota for month " + mi.year_month + ": " + error_msg, mi.vorlauf_id);
                        return;
                    }

                    if (!ExecuteMssqlStatementWithConn(conn, "COMMIT", error_msg)) {
                        CleanupAndError(state, bind_data, output, "tblPrimanota",
                            "Failed to commit month " + mi.year_month + ": " + error_msg, mi.vorlauf_id);
                        return;
                    }

                    auto month_end = std::chrono::high_resolution_clock::now();
                    double month_dur = std::chrono::duration<double>(month_end - month_start).count();
                    int64_t month_rows = static_cast<int64_t>(mi.row_indices.size());
                    total_transferred += month_rows;
                    AddSuccessResult(bind_data, "tblPrimanota", month_rows, mi.vorlauf_id, month_dur);

                    double frac = static_cast<double>(mi_idx + 1) / static_cast<double>(state.months.size());
                    state.progress = 80.0 + frac * 15.0;
                }

                auto transfer_end = std::chrono::high_resolution_clock::now();
                double transfer_dur = std::chrono::duration<double>(transfer_end - transfer_start).count();
                AddSuccessResult(bind_data, "tblPrimanota (total)", total_transferred, "", transfer_dur);
            }

            DropStagingTable(*state.staging_conn);
            state.staging_conn.reset();

        } else {
            // Single-vorlauf: try bcp first, fall back to INSERT VALUES within open transaction
            auto transfer_start = std::chrono::high_resolution_clock::now();

            // Try bcp (commits its own transaction via bulk copy protocol)
            string bcp_err;
            int64_t bcp_rows = 0;
            bool used_bcp = false;

            // For single-vorlauf, commit the open transaction first so bcp can insert
            if (ExecuteMssqlStatementWithConn(*state.txn_conn, "COMMIT", error_msg)) {
                if (BcpTransferPrimanota(*state.staging_conn, context, bind_data.secret_name,
                                          bcp_err, bcp_rows)) {
                    used_bcp = true;
                }
            }

            if (used_bcp) {
                DropStagingTable(*state.staging_conn);
                state.staging_conn.reset();
                state.txn_conn.reset();

                auto transfer_end = std::chrono::high_resolution_clock::now();
                double transfer_dur = std::chrono::duration<double>(transfer_end - transfer_start).count();
                AddSuccessResult(bind_data, "tblPrimanota (bcp)", bcp_rows, state.vorlauf_id, transfer_dur);
            } else {
                // Fallback: re-open transaction and use INSERT VALUES
                if (!ExecuteMssqlStatementWithConn(*state.txn_conn, "BEGIN TRANSACTION", error_msg)) {
                    CleanupAndError(state, bind_data, output, "ERROR", "Failed to re-begin transaction: " + error_msg, state.vorlauf_id);
                    return;
                }

                if (!BulkTransferPrimanota(*state.txn_conn, *state.staging_conn, bind_data.secret_name, error_msg)) {
                    CleanupAndError(state, bind_data, output, "tblPrimanota", error_msg, state.vorlauf_id);
                    return;
                }

                if (!ExecuteMssqlStatementWithConn(*state.txn_conn, "COMMIT", error_msg)) {
                    state.txn_conn.reset();
                    CleanupAndError(state, bind_data, output, "ERROR", "Failed to commit: " + error_msg, state.vorlauf_id);
                    return;
                }

                DropStagingTable(*state.staging_conn);
                state.staging_conn.reset();
                state.txn_conn.reset();

                auto transfer_end = std::chrono::high_resolution_clock::now();
                double transfer_dur = std::chrono::duration<double>(transfer_end - transfer_start).count();
                AddSuccessResult(bind_data, "tblPrimanota (bulk)", static_cast<int64_t>(bind_data.source_rows.size()), state.vorlauf_id, transfer_dur);
            }
        }

        state.phase = ExecutionPhase::FINALIZE;
        state.progress = 95.0;
        OutputResults(bind_data, state, output);
        return;
    }

    // -----------------------------------------------------------------
    // FINALIZE  (95 → 100%)  —  total timing
    // -----------------------------------------------------------------
    case ExecutionPhase::FINALIZE: {
        auto total_end = std::chrono::high_resolution_clock::now();
        double total_dur = std::chrono::duration<double>(total_end - state.total_start).count();
        string vid = bind_data.monatsvorlauf ? "" : state.vorlauf_id;
        AddSuccessResult(bind_data, "TIMING:total", static_cast<int64_t>(bind_data.source_rows.size()), vid, total_dur);

        state.phase = ExecutionPhase::OUTPUT_RESULTS;
        state.progress = 100.0;
        OutputResults(bind_data, state, output);
        return;
    }

    default:
        output.SetCardinality(0);
        return;
    }
}

// ============================================================================
// Register the function
// ============================================================================

void RegisterIntoWzFunction(DatabaseInstance &db) {
    TableFunction into_wz_func("into_wz", {}, IntoWzExecute, IntoWzBind, IntoWzInitGlobal);

    // Enable DuckDB's built-in progress bar for this table function
    into_wz_func.table_scan_progress = IntoWzProgress;

    // Add named parameters
    into_wz_func.named_parameters["secret"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["source_table"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["gui_verfahren_id"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["lng_kanzlei_konten_rahmen_id"] = LogicalType::BIGINT;
    into_wz_func.named_parameters["str_angelegt"] = LogicalType::VARCHAR;
    into_wz_func.named_parameters["generate_vorlauf_id"] = LogicalType::BOOLEAN;
    into_wz_func.named_parameters["monatsvorlauf"] = LogicalType::BOOLEAN;
    into_wz_func.named_parameters["skip_duplicate_check"] = LogicalType::BOOLEAN;
    into_wz_func.named_parameters["skip_fk_check"] = LogicalType::BOOLEAN;

    // Register using connection and catalog
    Connection con(db);
    con.BeginTransaction();
    auto &catalog = Catalog::GetSystemCatalog(db);
    CreateTableFunctionInfo info(into_wz_func);
    catalog.CreateFunction(*con.context, info);
    con.Commit();
}

} // namespace duckdb
