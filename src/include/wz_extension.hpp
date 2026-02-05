#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/main/extension_entries.hpp"

namespace duckdb {

// Forward declarations
class WzExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override;
    std::string Version() const override;
};

// ============================================================================
// Configuration structures
// ============================================================================

struct WzConfig {
    string database;           // Target MSSQL database
    string gui_verfahren_id;   // Required: VerfahrenID for both tables
    int64_t lng_kanzlei_konten_rahmen_id; // Required: KontenRahmenID
    string str_angelegt;       // User who created the record
    bool generate_vorlauf_id;  // Whether to auto-generate guiVorlaufID
};

// ============================================================================
// Vorlauf record structure
// ============================================================================

struct VorlaufRecord {
    int64_t lng_timestamp;
    string str_angelegt;
    string dtm_angelegt;
    string str_geaendert;
    string dtm_geaendert;
    string gui_vorlauf_id;
    string gui_verfahren_id;
    int64_t lng_kanzlei_konten_rahmen_id;
    int64_t lng_status;
    string dtm_vorlauf_datum_bis;
    string dtm_vorlauf_datum_von;
    int64_t lng_vorlauf_nr;
    string str_bezeichnung;
    string dtm_datev_export;
    bool ysn_auto_bu_schluessel_4stellig;
};

// ============================================================================
// Primanota record structure
// ============================================================================

struct PrimanotaRecord {
    int64_t lng_timestamp;
    string str_angelegt;
    string dtm_angelegt;
    string str_geaendert;
    string dtm_geaendert;
    string gui_primanota_id;
    string gui_vorlauf_id;
    int64_t lng_status;
    int64_t lng_zeilen_nr;
    int64_t lng_eingabe_waehrung_id;
    int64_t lng_bu;
    double dec_gegenkonto_nr;
    double dec_konto_nr;
    double dec_ea_konto_nr;
    string dtm_vorlauf_datum_bis;
    string dtm_beleg_datum;
    bool ysn_soll;
    double cur_eingabe_betrag;
    double cur_basis_betrag;
    double cur_skonto_betrag;
    double cur_skonto_basis_betrag;
    double dec_kost_menge;
    double dec_waehrungskurs;
    string str_beleg1;
    string str_beleg2;
    string str_buch_text;
    string str_kost1;
    string str_kost2;
    string str_eu_land;
    string str_ust_id;
    double dec_eu_steuersatz;
    string dtm_zusatz_datum;
    string gui_verfahren_id;
    double dec_ea_steuersatz;
    bool ysn_ea_transaktionen_manuell;
    double dec_ea_nummer;
    int64_t lng_sachverhalt_13b;
    string dtm_leistung;
    bool ysn_istversteuerung_in_sollversteuerung;
    int64_t lng_skonto_sachverhalt_waren_rhb;
    bool ysn_vst_bei_zahlung;
    string gui_parent_primanota;
    bool ysn_general_umkehr;
    double dec_steuersatz_manuell;
    bool ysn_mit_ursprungsland;
    string str_ursprungsland;
    string str_ursprungsland_ust_id;
    double dec_ursprungsland_steuersatz;
};

// ============================================================================
// Constraint information
// ============================================================================

struct ForeignKeyConstraint {
    string constraint_name;
    string column_name;
    string referenced_table;
    string referenced_column;
};

struct ConstraintViolation {
    string constraint_name;
    string table_name;
    string column_name;
    string referenced_table;
    string referenced_column;
    vector<string> violating_values;
    vector<string> violating_row_ids;
};

// ============================================================================
// Insert result
// ============================================================================

struct InsertResult {
    string table_name;
    int64_t rows_inserted;
    string gui_vorlauf_id;
    double duration_seconds;
    bool success;
    string error_message;
};

// ============================================================================
// Function declarations
// ============================================================================

// Constraint checker functions
vector<ForeignKeyConstraint> GetForeignKeyConstraints(ClientContext &context,
                                                       const string &secret_name,
                                                       const string &table_name);

vector<ConstraintViolation> ValidateConstraints(ClientContext &context,
                                                 const string &secret_name,
                                                 const string &table_name,
                                                 DataChunk &data);

bool CheckDuplicatePrimanotaIds(ClientContext &context,
                                const string &secret_name,
                                const vector<string> &primanota_ids,
                                vector<string> &existing_ids);

// Vorlauf builder functions
VorlaufRecord BuildVorlaufFromData(DataChunk &data,
                                   const WzConfig &config,
                                   const string &generated_vorlauf_id);

string DeriveVorlaufBezeichnung(const string &date_from, const string &date_to);

// Primanota mapper functions
vector<PrimanotaRecord> MapDataToPrimanota(DataChunk &data,
                                           const string &vorlauf_id,
                                           const string &verfahren_id);

// Table function registration
void RegisterIntoWzFunction(DatabaseInstance &db);

} // namespace duckdb
