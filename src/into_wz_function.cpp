#include "wz_extension.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "yyjson.hpp"
#include <chrono>
#include <sstream>

namespace duckdb {

// ============================================================================
// Column name mappings from source data to tblPrimanota
// ============================================================================

struct ColumnMapping {
    string source_name;
    string target_name;
    bool required;
};

// Note: These mappings define alternative column names
// The actual column lookup happens in BuildPrimanotaInsertSQL with fallback logic
static const vector<ColumnMapping> PRIMANOTA_COLUMN_MAPPINGS = {
    // Primary column names
    {"guiPrimanotaID", "guiPrimanotaID", true},
    {"dtmBelegDatum", "dtmBelegDatum", true},
    {"decKontoNr", "decKontoNr", true},
    {"decGegenkontoNr", "decGegenkontoNr", true},
    {"decEaKontoNr", "decEaKontoNr", false},
    {"ysnSoll", "ysnSoll", true},
    {"curEingabeBetrag", "curEingabeBetrag", true},
    {"curBasisBetrag", "curBasisBetrag", false},
    {"strBeleg1", "strBeleg1", false},
    {"strBeleg2", "strBeleg2", false},
    {"strBuchText", "strBuchText", false},
    // Alternative names (common in source data)
    {"konto", "decKontoNr", false},              // konto -> decKontoNr
    {"gegenkonto", "decGegenkontoNr", false},    // gegenkonto -> decGegenkontoNr
    {"eakonto", "decEaKontoNr", false},          // eakonto -> decEaKontoNr
    {"ea_konto", "decEaKontoNr", false},         // ea_konto -> decEaKontoNr
    {"umsatz", "curEingabeBetrag", false},       // umsatz -> curEingabeBetrag
    {"betrag", "curEingabeBetrag", false},       // betrag -> curEingabeBetrag
    {"umsatz_mit_vorzeichen", "curBasisBetrag", false},
    {"sh", "ysnSoll", false},                    // sh (Soll/Haben) -> ysnSoll
    {"soll_haben", "ysnSoll", false},            // soll_haben -> ysnSoll
    {"strbuchtext", "strBuchText", false},
    {"buchungstext", "strBuchText", false},
    {"text", "strBuchText", false},
    {"beleg1", "strBeleg1", false},
    {"beleg2", "strBeleg2", false},
    {"belegdatum", "dtmBelegDatum", false},
    {"datum", "dtmBelegDatum", false},
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

    // Source data column info
    vector<string> source_columns;
    vector<LogicalType> source_types;

    // Collected source data
    vector<vector<Value>> source_rows;

    // Results to return
    vector<InsertResult> results;
    idx_t current_result_idx;
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
// Helper: Generate UUID v4
// ============================================================================

static string GenerateUUID() {
    return UUID::ToString(UUID::GenerateRandomUUID());
}

// ============================================================================
// Helper: Get current timestamp for MSSQL
// ============================================================================

static string GetCurrentTimestamp() {
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
// Helper: Escape SQL string for MSSQL
// ============================================================================

static string EscapeSqlString(const string &str) {
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
            return "'" + val.ToString() + "'";
        case LogicalTypeId::UUID:
            return "'" + val.ToString() + "'";
        default:
            return val.ToString();
    }
}

// ============================================================================
// Helper: Find column index by name (case-insensitive)
// ============================================================================

static idx_t FindColumnIndex(const vector<string> &columns, const string &name) {
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

// ============================================================================
// Helper: Find date range in source data
// ============================================================================

static pair<string, string> FindDateRange(const vector<vector<Value>> &rows,
                                           const vector<string> &columns) {
    string min_date, max_date;

    // Look for dtmBelegDatum column
    idx_t date_col = FindColumnIndex(columns, "dtmBelegDatum");
    if (date_col == DConstants::INVALID_INDEX) {
        // Try alternative names
        date_col = FindColumnIndex(columns, "belegdatum");
        if (date_col == DConstants::INVALID_INDEX) {
            date_col = FindColumnIndex(columns, "datum");
        }
    }

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

    // Create a new connection to avoid deadlock
    auto &db = DatabaseInstance::GetDatabase(context);
    Connection conn(db);

    // Build IN clause (batch in groups of 100 to avoid query size limits)
    for (size_t batch_start = 0; batch_start < primanota_ids.size(); batch_start += 100) {
        size_t batch_end = std::min(batch_start + 100, primanota_ids.size());

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
    sql << "'" << vorlauf_id << "', ";  // guiVorlaufID
    sql << "'" << verfahren_id << "', ";  // guiVerfahrenID
    sql << konten_rahmen_id << ", ";  // lngKanzleiKontenRahmenID
    sql << "1, ";  // lngStatus
    sql << "'" << date_to << " 00:00:00', ";  // dtmVorlaufDatumBis
    sql << "'" << date_from << " 00:00:00', ";  // dtmVorlaufDatumVon
    sql << "NULL, ";  // lngVorlaufNr
    sql << "'" << EscapeSqlString(bezeichnung) << "', ";  // strBezeichnung
    sql << "NULL, ";  // dtmDatevExport
    sql << "0";  // ysnAutoBuSchluessel4stellig
    sql << ")";

    return sql.str();
}

// ============================================================================
// Helper: Build Primanota INSERT SQL for a batch of rows
// ============================================================================

static string BuildPrimanotaInsertSQL(const string &db_name,
                                       const vector<vector<Value>> &rows,
                                       const vector<string> &columns,
                                       const string &vorlauf_id,
                                       const string &verfahren_id,
                                       const string &str_angelegt,
                                       const string &vorlauf_datum_bis) {
    if (rows.empty()) {
        return "";
    }

    string timestamp = GetCurrentTimestamp();

    // Helper lambda to find column with fallback names
    auto findColumnWithFallbacks = [&columns](std::initializer_list<const char*> names) -> idx_t {
        for (const char* name : names) {
            idx_t idx = FindColumnIndex(columns, name);
            if (idx != DConstants::INVALID_INDEX) {
                return idx;
            }
        }
        return DConstants::INVALID_INDEX;
    };

    // Find column indices with fallback alternative names
    idx_t col_primanota_id = FindColumnIndex(columns, "guiPrimanotaID");

    idx_t col_beleg_datum = findColumnWithFallbacks({
        "dtmBelegDatum", "belegdatum", "datum", "date"
    });

    idx_t col_konto_nr = findColumnWithFallbacks({
        "decKontoNr", "konto", "kontonr", "konto_nr", "account"
    });

    idx_t col_gegenkonto_nr = findColumnWithFallbacks({
        "decGegenkontoNr", "gegenkonto", "gegenkontonr", "gegenkonto_nr", "counter_account"
    });

    idx_t col_ea_konto_nr = findColumnWithFallbacks({
        "decEaKontoNr", "eakonto", "ea_konto", "eakontonr", "ea_konto_nr"
    });

    // ysnSoll can come from ysnSoll (boolean) or sh (Soll/Haben string)
    idx_t col_ysn_soll = findColumnWithFallbacks({
        "ysnSoll", "ysnsoll", "soll", "sh", "soll_haben", "sollhaben"
    });

    idx_t col_eingabe_betrag = findColumnWithFallbacks({
        "curEingabeBetrag", "umsatz", "betrag", "amount", "eingabebetrag"
    });

    idx_t col_basis_betrag = findColumnWithFallbacks({
        "curBasisBetrag", "umsatz_mit_vorzeichen", "basisbetrag", "basis_betrag"
    });

    idx_t col_beleg1 = findColumnWithFallbacks({
        "strBeleg1", "beleg1", "beleg_1", "belegfeld1", "belegfeld_1",
        "belegnr", "belegnummer", "beleg_nr", "beleg_nummer"
    });

    idx_t col_beleg2 = findColumnWithFallbacks({
        "strBeleg2", "beleg2", "beleg_2", "belegfeld2", "belegfeld_2",
        "trans_nr", "transnr", "trans_nummer", "transaktionsnr"
    });

    idx_t col_buch_text = findColumnWithFallbacks({
        "strBuchText", "strbuchtext", "buchungstext", "buchtext", "text", "description"
    });

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

    for (size_t i = 0; i < rows.size(); i++) {
        const auto &row = rows[i];

        if (i > 0) {
            sql << ",\n";
        }

        // Get values from row with safe access
        auto getValue = [&row](idx_t idx) -> Value {
            if (idx == DConstants::INVALID_INDEX || idx >= row.size()) {
                return Value();
            }
            return row[idx];
        };

        string primanota_id = getValue(col_primanota_id).IsNull() ? GenerateUUID() : getValue(col_primanota_id).ToString();
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
        sql << "'" << vorlauf_id << "', ";  // guiVorlaufID
        sql << "1, ";  // lngStatus
        sql << "NULL, ";  // lngZeilenNr
        sql << "1, ";  // lngEingabeWaehrungID (EUR)
        sql << "NULL, ";  // lngBu
        sql << FormatSqlValue(gegenkonto_val) << ", ";  // decGegenkontoNr
        sql << FormatSqlValue(konto_val) << ", ";  // decKontoNr
        sql << FormatSqlValue(ea_konto_val) << ", ";  // decEaKontoNr
        sql << "'" << vorlauf_datum_bis << " 00:00:00', ";  // dtmVorlaufDatumBis
        sql << "'" << beleg_datum << "', ";  // dtmBelegDatum
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
        sql << "'" << verfahren_id << "', ";  // guiVerfahrenID
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
// Helper: Execute SQL via mssql_query (for queries that return data)
// ============================================================================

static unique_ptr<MaterializedQueryResult> ExecuteMssqlQuery(ClientContext &context,
                                                              const string &secret_name,
                                                              const string &sql) {
    string query = "SELECT * FROM mssql_query('" + secret_name + "', $$" + sql + "$$)";
    auto result = context.Query(query, false);
    if (result->HasError()) {
        return nullptr;
    }
    return unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
}

// ============================================================================
// Helper: Execute SQL statement on attached MSSQL database
// The db_name is the attached database name (e.g., 'mssql_conn')
// If conn_ptr is provided, uses that connection (for transaction continuity)
// Otherwise creates a new connection
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

static bool ExecuteMssqlStatement(ClientContext &context,
                                   const string &db_name,
                                   const string &sql,
                                   string &error_message) {
    // Create a new connection - note: this won't maintain transaction state
    auto &db = DatabaseInstance::GetDatabase(context);
    Connection conn(db);
    return ExecuteMssqlStatementWithConn(conn, sql, error_message);
}

// ============================================================================
// Bind function
// ============================================================================

static unique_ptr<FunctionData> IntoWzBind(ClientContext &context,
                                            TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string> &names) {
    auto bind_data = make_uniq<IntoWzBindData>();

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
    bind_data->generate_vorlauf_id = true;
    bind_data->executed = false;
    bind_data->current_result_idx = 0;

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

    return std::move(bind_data);
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
// Main execution function
// ============================================================================

static void IntoWzExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<IntoWzBindData>();
    auto &global_state = data_p.global_state->Cast<IntoWzGlobalState>();

    // If already executed, return remaining results
    if (bind_data.executed) {
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
        return;
    }

    // Mark as executed
    bind_data.executed = true;
    auto start_time = std::chrono::high_resolution_clock::now();

    // =========================================================================
    // Step 1: Validate required parameters
    // =========================================================================

    if (bind_data.gui_verfahren_id.empty()) {
        AddErrorResult(bind_data, "ERROR", "gui_verfahren_id is required");
        goto output_results;
    }

    if (bind_data.source_table.empty()) {
        AddErrorResult(bind_data, "ERROR", "source_table is required");
        goto output_results;
    }

    {
        // =========================================================================
        // Step 2: Read source data from DuckDB table
        // Use a separate connection to avoid deadlock
        // =========================================================================

        auto &db = DatabaseInstance::GetDatabase(context);
        Connection conn(db);

        string source_query = "SELECT * FROM " + bind_data.source_table;
        auto source_result = conn.Query(source_query);

        if (source_result->HasError()) {
            AddErrorResult(bind_data, "ERROR", "Failed to read source table: " + source_result->GetError());
            goto output_results;
        }

        auto source_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(source_result));

        // Get column names and types
        for (idx_t i = 0; i < source_materialized->ColumnCount(); i++) {
            bind_data.source_columns.push_back(source_materialized->ColumnName(i));
            bind_data.source_types.push_back(source_materialized->types[i]);
        }

        // Collect all rows
        auto &collection = source_materialized->Collection();
        for (auto &chunk : collection.Chunks()) {
            for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
                vector<Value> row;
                for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
                    row.push_back(chunk.data[col_idx].GetValue(row_idx));
                }
                bind_data.source_rows.push_back(std::move(row));
            }
        }

        if (bind_data.source_rows.empty()) {
            AddErrorResult(bind_data, "ERROR", "Source table is empty");
            goto output_results;
        }

        // =========================================================================
        // Step 3: Extract Primanota IDs and check for duplicates
        // =========================================================================

        idx_t primanota_id_col = FindColumnIndex(bind_data.source_columns, "guiPrimanotaID");
        vector<string> primanota_ids;

        if (primanota_id_col != DConstants::INVALID_INDEX) {
            for (const auto &row : bind_data.source_rows) {
                if (primanota_id_col < row.size() && !row[primanota_id_col].IsNull()) {
                    primanota_ids.push_back(row[primanota_id_col].ToString());
                }
            }

            auto duplicates = CheckDuplicates(context, bind_data.secret_name, primanota_ids);
            if (!duplicates.empty()) {
                string dup_list;
                for (size_t i = 0; i < std::min(duplicates.size(), size_t(5)); i++) {
                    if (i > 0) dup_list += ", ";
                    dup_list += duplicates[i];
                }
                if (duplicates.size() > 5) {
                    dup_list += " (and " + std::to_string(duplicates.size() - 5) + " more)";
                }
                AddErrorResult(bind_data, "ERROR",
                    "Duplicate guiPrimanotaID found: " + dup_list +
                    ". " + std::to_string(duplicates.size()) + " records already exist in tblPrimanota.");
                goto output_results;
            }
        }

        // =========================================================================
        // Step 4: Generate Vorlauf data
        // =========================================================================

        string vorlauf_id = GenerateUUID();
        pair<string, string> date_range = FindDateRange(bind_data.source_rows, bind_data.source_columns);
        string date_from = date_range.first;
        string date_to = date_range.second;

        if (date_from.empty()) {
            // Default to current month
            auto now = std::chrono::system_clock::now();
            auto time = std::chrono::system_clock::to_time_t(now);
            char buffer[32];
            std::strftime(buffer, sizeof(buffer), "%Y-%m-01", std::localtime(&time));
            date_from = string(buffer);
        }
        if (date_to.empty()) {
            // Default to current date
            auto now = std::chrono::system_clock::now();
            auto time = std::chrono::system_clock::to_time_t(now);
            char buffer[32];
            std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", std::localtime(&time));
            date_to = string(buffer);
        }

        string bezeichnung = DeriveVorlaufBezeichnung(date_from, date_to);

        // =========================================================================
        // Step 5: Create a single connection for transaction and insert data
        // All transaction operations must use the same connection
        // =========================================================================

        string error_msg;
        auto vorlauf_start = std::chrono::high_resolution_clock::now();

        // Create a single connection for the entire transaction
        auto &db = DatabaseInstance::GetDatabase(context);
        Connection txn_conn(db);

        // Begin transaction
        if (!ExecuteMssqlStatementWithConn(txn_conn, "BEGIN TRANSACTION", error_msg)) {
            AddErrorResult(bind_data, "ERROR", "Failed to begin transaction: " + error_msg);
            goto output_results;
        }

        // Insert Vorlauf
        {
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

            if (!ExecuteMssqlStatementWithConn(txn_conn, vorlauf_sql, error_msg)) {
                // Rollback on error
                string rollback_err;
                ExecuteMssqlStatementWithConn(txn_conn, "ROLLBACK", rollback_err);
                AddErrorResult(bind_data, "tblVorlauf", "Insert failed: " + error_msg, vorlauf_id);
                goto output_results;
            }
        }

        auto vorlauf_end = std::chrono::high_resolution_clock::now();
        double vorlauf_duration = std::chrono::duration<double>(vorlauf_end - vorlauf_start).count();
        AddSuccessResult(bind_data, "tblVorlauf", 1, vorlauf_id, vorlauf_duration);

        // =========================================================================
        // Step 6: Insert Primanota records in batches
        // =========================================================================

        {
            auto primanota_start = std::chrono::high_resolution_clock::now();
            int64_t total_primanota_rows = 0;
            const size_t BATCH_SIZE = 100;  // Insert 100 rows at a time

            for (size_t batch_start = 0; batch_start < bind_data.source_rows.size(); batch_start += BATCH_SIZE) {
                size_t batch_end = std::min(batch_start + BATCH_SIZE, bind_data.source_rows.size());

                vector<vector<Value>> batch_rows(
                    bind_data.source_rows.begin() + batch_start,
                    bind_data.source_rows.begin() + batch_end
                );

                string primanota_sql = BuildPrimanotaInsertSQL(
                    bind_data.secret_name,
                    batch_rows,
                    bind_data.source_columns,
                    vorlauf_id,
                    bind_data.gui_verfahren_id,
                    bind_data.str_angelegt,
                    date_to
                );

                if (!ExecuteMssqlStatementWithConn(txn_conn, primanota_sql, error_msg)) {
                    // Rollback on error
                    string rollback_err;
                    ExecuteMssqlStatementWithConn(txn_conn, "ROLLBACK", rollback_err);
                    AddErrorResult(bind_data, "tblPrimanota",
                        "Insert failed at row " + std::to_string(batch_start) + ": " + error_msg, vorlauf_id);
                    goto output_results;
                }

                total_primanota_rows += batch_rows.size();
            }

            // =========================================================================
            // Step 7: Commit transaction
            // =========================================================================

            if (!ExecuteMssqlStatementWithConn(txn_conn, "COMMIT", error_msg)) {
                AddErrorResult(bind_data, "ERROR", "Failed to commit transaction: " + error_msg, vorlauf_id);
                goto output_results;
            }

            auto primanota_end = std::chrono::high_resolution_clock::now();
            double primanota_duration = std::chrono::duration<double>(primanota_end - primanota_start).count();
            AddSuccessResult(bind_data, "tblPrimanota", total_primanota_rows, vorlauf_id, primanota_duration);
        }
    }

output_results:
    // Return results
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
