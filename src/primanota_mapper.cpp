#include "wz_extension.hpp"
#include <chrono>

namespace duckdb {

// ============================================================================
// Helper: Get current timestamp
// ============================================================================

static string GetTimestampNow() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);

    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d 00:00:00", std::localtime(&time));
    return string(buffer);
}

// ============================================================================
// Helper: Find column index by name (case-insensitive)
// ============================================================================

static idx_t FindColumnIndex(const vector<string> &column_names, const string &target) {
    string target_lower = target;
    std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);

    for (idx_t i = 0; i < column_names.size(); i++) {
        string col_lower = column_names[i];
        std::transform(col_lower.begin(), col_lower.end(), col_lower.begin(), ::tolower);

        if (col_lower == target_lower) {
            return i;
        }
    }

    return DConstants::INVALID_INDEX;
}

// ============================================================================
// Map source data to Primanota records
// ============================================================================

vector<PrimanotaRecord> MapDataToPrimanota(DataChunk &data,
                                           const string &vorlauf_id,
                                           const string &verfahren_id) {
    vector<PrimanotaRecord> records;

    // Expected source columns based on user's sample data:
    // guiPrimanotaID, id, umsatz, umsatz_mit_vorzeichen, sh, ysnSoll,
    // dtmBelegDatum, strBeleg1, strBeleg2, decKontoNr, kontobezeichnung, kontoart,
    // decGegenkontoNr, gegenkontobezeichnung, gegenkontoart, strbuchtext

    // For each row in the data chunk, create a PrimanotaRecord
    for (idx_t row_idx = 0; row_idx < data.size(); row_idx++) {
        PrimanotaRecord record;

        // Fixed values
        record.lng_timestamp = 0;
        record.str_angelegt = "wz_extension";
        record.dtm_angelegt = GetTimestampNow();
        record.str_geaendert = "";
        record.dtm_geaendert = "";
        record.gui_vorlauf_id = vorlauf_id;
        record.gui_verfahren_id = verfahren_id;
        record.lng_status = 1;
        record.lng_zeilen_nr = 0;  // NULL
        record.lng_eingabe_waehrung_id = 1;  // Default to EUR
        record.lng_bu = 0;  // NULL

        // Map from source columns - this is a simplified mapping
        // In real implementation, we'd need to match column names

        // guiPrimanotaID - should come from source
        auto primanota_id_val = data.data[0].GetValue(row_idx);
        record.gui_primanota_id = primanota_id_val.IsNull() ? "" : primanota_id_val.ToString();

        // Default NULL values for optional fields
        record.dec_ea_konto_nr = 0;
        record.cur_skonto_betrag = 0;
        record.cur_skonto_basis_betrag = 0;
        record.dec_kost_menge = 0;
        record.dec_waehrungskurs = 0;
        record.str_kost1 = "";
        record.str_kost2 = "";
        record.str_eu_land = "";
        record.str_ust_id = "";
        record.dec_eu_steuersatz = 0;
        record.dtm_zusatz_datum = "";
        record.dec_ea_steuersatz = 0;
        record.ysn_ea_transaktionen_manuell = false;
        record.dec_ea_nummer = 0;
        record.lng_sachverhalt_13b = 0;
        record.dtm_leistung = "";
        record.ysn_istversteuerung_in_sollversteuerung = false;
        record.lng_skonto_sachverhalt_waren_rhb = 0;
        record.ysn_vst_bei_zahlung = false;
        record.gui_parent_primanota = "";
        record.ysn_general_umkehr = false;
        record.dec_steuersatz_manuell = 0;
        record.ysn_mit_ursprungsland = false;
        record.str_ursprungsland = "";
        record.str_ursprungsland_ust_id = "";
        record.dec_ursprungsland_steuersatz = 0;

        records.push_back(record);
    }

    return records;
}

// ============================================================================
// Generate INSERT SQL for Primanota records
// ============================================================================

string GeneratePrimanotaInsertSQL(const vector<PrimanotaRecord> &records,
                                   const string &vorlauf_datum_bis) {
    if (records.empty()) {
        return "";
    }

    string sql = "INSERT INTO tblPrimanota (";
    sql += "lngTimestamp, strAngelegt, dtmAngelegt, strGeaendert, dtmGeaendert, ";
    sql += "guiPrimanotaID, guiVorlaufID, lngStatus, lngZeilenNr, lngEingabeWaehrungID, ";
    sql += "lngBu, decGegenkontoNr, decKontoNr, decEaKontoNr, dtmVorlaufDatumBis, ";
    sql += "dtmBelegDatum, ysnSoll, curEingabeBetrag, curBasisBetrag, curSkontoBetrag, ";
    sql += "curSkontoBasisBetrag, decKostMenge, decWaehrungskurs, strBeleg1, strBeleg2, ";
    sql += "strBuchText, strKost1, strKost2, strEuLand, strUstId, ";
    sql += "decEuSteuersatz, dtmZusatzDatum, guiVerfahrenID, decEaSteuersatz, ";
    sql += "ysnEaTransaktionenManuell, decEaNummer, lngSachverhalt13b, dtmLeistung, ";
    sql += "ysnIstversteuerungInSollversteuerung, lngSkontoSachverhaltWarenRHB, ";
    sql += "ysnVStBeiZahlung, guiParentPrimanota, ysnGeneralUmkehr, decSteuersatzManuell, ";
    sql += "ysnMitUrsprungsland, strUrsprungsland, strUrsprungslandUstId, decUrsprungslandSteuersatz";
    sql += ") VALUES\n";

    for (size_t i = 0; i < records.size(); i++) {
        const auto &r = records[i];

        if (i > 0) {
            sql += ",\n";
        }

        sql += "(";
        sql += std::to_string(r.lng_timestamp) + ", ";
        sql += "'" + r.str_angelegt + "', ";
        sql += "'" + r.dtm_angelegt + "', ";
        sql += r.str_geaendert.empty() ? "NULL, " : "'" + r.str_geaendert + "', ";
        sql += r.dtm_geaendert.empty() ? "NULL, " : "'" + r.dtm_geaendert + "', ";
        sql += "'" + r.gui_primanota_id + "', ";
        sql += "'" + r.gui_vorlauf_id + "', ";
        sql += std::to_string(r.lng_status) + ", ";
        sql += "NULL, ";  // lngZeilenNr
        sql += std::to_string(r.lng_eingabe_waehrung_id) + ", ";
        sql += "NULL, ";  // lngBu
        sql += std::to_string(r.dec_gegenkonto_nr) + ", ";
        sql += std::to_string(r.dec_konto_nr) + ", ";
        sql += "NULL, ";  // decEaKontoNr
        sql += "'" + vorlauf_datum_bis + "', ";
        sql += "'" + r.dtm_beleg_datum + "', ";
        sql += r.ysn_soll ? "1, " : "0, ";
        sql += std::to_string(r.cur_eingabe_betrag) + ", ";
        sql += std::to_string(r.cur_basis_betrag) + ", ";
        sql += "NULL, ";  // curSkontoBetrag
        sql += "NULL, ";  // curSkontoBasisBetrag
        sql += "NULL, ";  // decKostMenge
        sql += "NULL, ";  // decWaehrungskurs
        sql += "'" + r.str_beleg1 + "', ";
        sql += r.str_beleg2.empty() ? "NULL, " : "'" + r.str_beleg2 + "', ";
        sql += "'" + r.str_buch_text + "', ";
        sql += "NULL, NULL, ";  // strKost1, strKost2
        sql += "NULL, NULL, ";  // strEuLand, strUstId
        sql += "NULL, NULL, ";  // decEuSteuersatz, dtmZusatzDatum
        sql += "'" + r.gui_verfahren_id + "', ";
        sql += "NULL, ";  // decEaSteuersatz
        sql += "0, ";  // ysnEaTransaktionenManuell
        sql += "NULL, NULL, NULL, ";  // decEaNummer, lngSachverhalt13b, dtmLeistung
        sql += "0, NULL, 0, ";  // ysnIstversteuerung..., lngSkontoSachverhalt..., ysnVStBeiZahlung
        sql += "NULL, 0, NULL, ";  // guiParentPrimanota, ysnGeneralUmkehr, decSteuersatzManuell
        sql += "0, NULL, NULL, NULL";  // ysnMitUrsprungsland, strUrsprungsland, strUrsprungslandUstId, decUrsprungslandSteuersatz
        sql += ")";
    }

    return sql;
}

} // namespace duckdb
