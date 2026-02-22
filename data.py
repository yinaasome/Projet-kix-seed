
import requests
import pandas as pd
from pandas import json_normalize
import json
import pickle
import time 

# Étape 1 : Connexion

login_url = ""
login_payload = {
    "email": "",         # Remplace par ton email
    "password": ""  # Remplace par ton mot de passe
}

headers_login = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

response_login = requests.post(login_url, json=login_payload, headers=headers_login)
if response_login.status_code != 200:
    raise Exception("Échec de la connexion. Vérifiez vos identifiants.")

token = response_login.json().get("token")
print("Connexion réussie. Token récupéré.")

# Étape 2 : Récupération des données paginées
data_url = "https://stats.seeds.bf/api/get_ia_datas/2024"
headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
}

def recuperer_donnees_api(data_url, headers):
    page = 1
    all_data = []

    while True:
        url = f"{data_url}?page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Erreur page {page} : {response.status_code}")
            break

        data = response.json()
        results = data.get("results", [])
        if not results:
            print("Fin des données.")
            break

        all_data.extend(results)
        print(f"Page {page} récupérée.")
        page += 1

    return pd.DataFrame(all_data)

# Étape 3 : Traitement des données
def traiter_donnees(df):
    df_copy = df.copy()
    df_optmoyen = df.copy()

    # Suppression colonnes inutiles initialement
    df_copy.drop(columns=["optbulletinmoyennes", "retard_absence"], errors='ignore', inplace=True)

    # Convertir optbulletins en dictionnaires
    def convertir_optbulletins(liste):
        if isinstance(liste, list):
            return {b['trimestre']: b for b in liste if 'trimestre' in b}
        return liste

    df_copy["optbulletins"] = df_copy["optbulletins"].apply(convertir_optbulletins)

    # Aplatir les dictionnaires
    colonnes_dict = [col for col in df_copy.columns if df_copy[col].apply(lambda x: isinstance(x, dict)).any()]
    for col in colonnes_dict:
        temp_df = json_normalize(df_copy[col])
        temp_df.columns = [f"{col}_{c}" for c in temp_df.columns]
        df_copy = pd.concat([df_copy.drop(columns=[col]), temp_df], axis=1)

    # Moyennes par matière
    # 1. Explosion des moyennes par élève
    df_exploded = df_optmoyen.explode('optbulletinmoyennes').dropna(subset=['optbulletinmoyennes'])

    # 2. Flatten du champ JSON en colonnes
    opt_flat = json_normalize(df_exploded['optbulletinmoyennes']).add_prefix('optbmoyen_')

# 1. Dictionnaire de renommage
    colonnes_renommees = {
    "optbmoyen_eleve_id": "eleve_id",
    "optbmoyen_nom_prof": "nom_prof",
    "optbmoyen_libelle_matiere": "libelle_matiere",
    "optbmoyen_moyenne_compo": "moyenne_compo",
    "optbmoyen_trimestre": "trimestre"
    }

# 2. Filtrer et renommer les colonnes
    df_performance = opt_flat[list(colonnes_renommees.keys())].rename(columns=colonnes_renommees)

# 3. Créer une variable note_matiere_t : libelle_matiere_trimestre
    df_performance["note_matiere_t"] = df_performance["libelle_matiere"] + "_" + df_performance["trimestre"].astype(str)

# Suppression colonnes inutiles (tronqué pour clarté ici)
    colonnes_supprimer = ["trimestre", "libelle_matiere"]
    df_performance = df_performance.drop(columns=[col for col in colonnes_supprimer if col in df_performance.columns], errors='ignore')
    df_pivot = df_performance.pivot_table(
    index="eleve_id",    # identifiants uniques (tu peux enlever "nom_prof" si inutile)
    columns="note_matiere_t",         # ce que tu veux comme colonnes
    values="moyenne_compo",  # les valeurs à remplir
    aggfunc='first'                     # au cas où il y aurait des doublons
    ).reset_index()
# Séparer les colonnes d'identifiants (index)
    id_vars = ["eleve_id"]
    # Trier les autres colonnes selon le suffixe de trimestre
    other_columns = [col for col in df_pivot.columns if col not in id_vars]
    sorted_columns = sorted(other_columns, key=lambda x: (x[-2:], x[:-3]))  # Trie par T1/T2/T3 puis par matière
    # Réorganiser les colonnes dans le DataFrame
    df_pivot = df_pivot[id_vars + sorted_columns]

    # Fusion avec les autres infos
    df_copy = df_copy.loc[:, ~df_copy.columns.duplicated()]
    df_merged = pd.merge(df_pivot, df_copy, on="eleve_id", how="inner")

    # Retard et absence
    retard = df_optmoyen.explode('retard_absence').dropna(subset=['retard_absence'])
    retard_flat = json_normalize(retard['retard_absence']).add_prefix("retard_absence_")
    retard_flat.rename(columns={"retard_absence_eleve_id": "eleve_id"}, inplace=True)
    df_final = pd.merge(df_merged,retard_flat, on="eleve_id", how="outer")

    # Suppression colonnes inutiles (tronqué pour clarté ici)
    colonnes_supprimer = [
        "code_matiere", "id", "serie",
        "updated_by", "created_by", "created_at", "updated_at", "ecole_updated_at",
        "eleve_updated_by", "eleve_created_by", "eleve_deleted_at",
        "eleve_created_at", "eleve_updated_at", "eleve_annee_id",
        "classe_updated_by", "classe_created_by", "classe_created_at", "classe_updated_at",
        "salle_classe_annee_id", "salle_classe_updated_by", "salle_classe_created_by",
        "salle_classe_created_at", "salle_classe_updated_at", "salle_classe_classe.id",
        "salle_classe_classe.libelle", "salle_classe_classe.code", "salle_classe_classe.category",
        "salle_classe_classe.updated_by", "salle_classe_classe.created_by",
        "salle_classe_classe.created_at", "salle_classe_classe.updated_at",
        "optbulletins_T1.salle_classe_id", "optbulletins_T1.salle_classe",
        "optbulletins_T1.matricule", "optbulletins_T1.date_naissance",
        "optbulletins_T1.lieu_naissance", "optbulletins_T1.conduite", "optbulletins_T1.moyenne_t2",
        "optbulletins_T1.updated_by", "optbulletins_T1.created_by", "optbulletins_T1.created_at",
        "optbulletins_T1.updated_at", "optbulletins_T2.salle_classe_id", "optbulletins_T2.salle_classe",
        "optbulletins_T2.nom_prenom", "optbulletins_T2.date_naissance",
        "optbulletins_T2.lieu_naissance", "optbulletins_T2.conduite", "optbulletins_T2.updated_by",
        "optbulletins_T2.created_by", "optbulletins_T2.created_at", "optbulletins_T2.updated_at",
        "retard_absence_id", "retard_absence_created_at", "retard_absence_updated_at",
        "retard_absence_deleted_at", "optbulletins_T3.updated_by","optbulletins_T3.created_by",
        "optbulletins_T3.created_at","optbulletins_T3.updated_at","optbulletins_T3.date_naissance","optbulletins_T3.lieu_naissance",
        "optbulletins_T3.salle_classe_id","optbulletins_T3.salle_classe","optbulletins_T3.matricule",
        "optbulletins_T3.nom_prenom","optbulletins_T3.annee_id","optbulletins_T3.annee_scolaire","libelle_matiere","optbulletins_T3.trimestre",
        "optbulletins_T1.trimestre", "optbulletins_T2.trimestre","ecole_longitude","ecole_latitude"
    

]
    df_final = df_final.drop(columns=[col for col in colonnes_supprimer if col in df_final.columns], errors='ignore')

    # Renommage (extrait partiel pour exemple)
    renommage = {
    # Identifiants principaux
    "eleve_id": "id_eleve",
    "ecole_id":"id_ecole",
    "ecole_annee_id": "_d_ecole_annee",
    "optbulletins_T3.eleve_id":"id_eleve_t3",
    "optbulletins_T1.eleve_id":"id_eleve_t1",
    "optbulletins_T2.eleve_id":"id_eleve_t2",
    "nom_prof": "nom_enseignant",
    "matiere_id": "id_matiere",
    "code_matiere": "code_matiere",
    "note_matiere_t": "note_matiere",
    "moyenne_compo": "notes_matieres",
    "trimestre":"trimestre",
    "annee_id": "id_annee",
    "scolarite_id": "id_scolarite",
    "classe_id": "id_classe",
    "salle_classe_id": "id_salle_classe",
    "eleve_info_iue" : "identifiant_unique_eleve",
    "eleve_info_matricule": "matricule",
    "eleve_info_prenom": "prenom_eleve",
    "eleve_info_nom": "Nom_eleve",
    "eleve_info_date_naissance" : "date_naissance",
    "eleve_info_lieu_naissance": "lieu_naissance",
    

    # Statut académique
    "etat_scolarite": "statut_scolarite",
    "redoublant": "est_redoublant",
    "affecte": "est_affecte",
    "src_photo": "source_photo",
    
    # Antécédents éducatifs de l'élève
    "eleve_frequent_preced": "frequentation_precedente",
    "eleve_frere_soeur_frequent": "freres_soeurs_frequentant",
    "eleve_bourse_etude": "a_bourse_etude",
    "eleve_classe_redouble_cp1": "redoublement_cp1",
    "eleve_classe_redouble_cp2": "redoublement_cp2",
    "eleve_classe_redouble_ce1": "redoublement_ce1",
    "eleve_classe_redouble_ce2": "redoublement_ce2",
    "eleve_classe_redouble_cm1": "redoublement_cm1",
    "eleve_classe_redouble_cm2": "redoublement_cm2",
    
    # Accès technologique de l'élève
    "eleve_info_telephone": "possede_telephone",
    "eleve_suivi_off": "suivi_officiel",
    "eleve_suivi_domicile": "suivi_a_domicile",
    "eleve_suivi_centre": "suivi_au_centre",
    "eleve_suivi_groupe": "suivi_en_groupe",
    
    # Matériel éducatif
    "eleve_mat_didact_table": "possede_bureau",
    "eleve_mat_didact_livres": "possede_livres",
    "eleve_mat_didact_tableaux": "possede_tableaux",
    "eleve_mat_didact_tablette": "possede_tablette",
    "eleve_mat_didact_autres": "possede_autres_materiels",
    
    # Équipements du foyer
    "eleve_menage_tele": "menage_a_television",
    "eleve_menage_radio": "menage_a_radio",
    "eleve_menage_internet": "menage_a_internet",
    "eleve_menage_electricite": "menage_a_electricite",
    "eleve_menage_autre": "menage_a_autres_equipements",
    
    # Informations sur l'école
    "ecole_ecole_nom": "nom_ecole",
    "ecole_ecole_code": "code_ecole",
    "ecole_ecole_annee_ouverture": "annee_ouverture_ecole",
    "ecole_ecole_statut": "statut_ecole",
    "ecole_ecole_situation_admin": "situation_administrative_ecole",
    "ecole_ecole_ref_arrete_ouverture": "reference_arrete_ouverture",
    "ecole_ecole_type": "type_ecole",
    "ecole_ecole_conventionel": "ecole_conventionnelle",
    "ecole_ecole_mode_recrutement": "mode_recrutement_ecole",
    "ecole_ecole_milieu": "milieu_ecole",
    "ecole_ecole_region": "region_ecole",
    "ecole_ecole_province": "province_ecole",
    "ecole_ecole_commune": "commune_ecole",
    "ecole_ecole_ceb": "ceb_ecole",
    "ecole_ecole_secteur_village": "secteur_village_ecole",
    
    # Informations sur le directeur d'école
    "ecole_ecole_directeur_nom_prenom": "nom_complet_directeur",
    "ecole_ecole_directeur_sexe": "sexe_directeur",
    "ecole_ecole_directeur_matrricule": "matricule_directeur",
    "ecole_ecole_directeur_emploi": "poste_directeur",
    "ecole_ecole_directeur_charge": "responsabilites_directeur",
    
    # Coordonnées de l'école
    "ecole_ecole_email": "email_ecole",
    "ecole_ecole_phone": "telephone_ecole",
    "ecole_ecole_boite_postal": "boite_postale_ecole",
    "ecole_ecole_logo": "logo_ecole",
    
    # Informations académiques de l'école
    "ecole_ecole_cycle": "cycle_ecole",
    "ecole_ecole_type_enseignement": "type_enseignement_ecole",
    "ecole_ecole_db_name": "nom_base_donnees_ecole",
    
    
    # Informations personnelles de l'élève
    "eleve_file_name": "nom_fichier_eleve",
    "eleve_matricule": "matricule_eleve",
    "eleve_nom": "nom_eleve",
    "eleve_prenom": "prenom_eleve",
    "optbulletins_T1.nom_prenom": "nom_complet_eleve",
    "eleve_date_naissance": "date_naissance_eleve",
    "eleve_lieu_naissance": "lieu_naissance_eleve",
    "eleve_n_extrait": "numero_extrait_eleve",
    #"eleve_genre": "genre_eleve",
    "eleve_info_genre": "genre_eleve",
    "eleve_telephone": "telephone_eleve",
    
    # Informations familiales de l'élève
    "eleve_nom_prenom_pere": "nom_complet_pere",
    "eleve_profession_pere": "profession_pere",
    "eleve_nom_prenom_mere": "nom_complet_mere",
    "eleve_profession_mere": "profession_mere",
    "eleve_nom_prenom_tel_pers_pre_besoin": "contact_urgence",
    
    # Conditions de vie de l'élève
    "eleve_vie_parents": "vit_avec_parents",
    "eleve_vie_chrez_parents": "vit_au_domicile_parents",
    "eleve_vie_chrez_tuteur": "vit_avec_tuteur",
    "eleve_eleve_statut": "statut_eleve",
    "eleve_eleve_handicap": "eleve_a_handicap",
    
    # Informations sur le bien-être de l'élève
    "eleve_eleve_victime_violence": "victime_violence",
    "eleve_victime_violence_physique": "victime_violence_physique",
    "eleve_victime_stigmatisation": "victime_stigmatisation",
    "eleve_victime_violence_sexuelle": "victime_violence_sexuelle",
    "eleve_victime_violence_emotionnelle": "victime_violence_emotionnelle",
    "eleve_victime_autre": "victime_autre_violence",
    
    # Informations supplémentaires sur l'élève
    "eleve_eleve_nationalite": "nationalite_eleve",
    "eleve_niveau_instruction_pere": "niveau_education_pere",
    "eleve_niveau_instruction_mere": "niveau_education_mere",
    "eleve_statut_mat_pere": "statut_matrimonial_pere",
    "eleve_statut_mat_mere": "statut_matrimonial_mere",
    "eleve_eleve_dort_moustiquaire": "dort_sous_moustiquaire",
    "eleve_eleve_distance_domicile": "distance_domicile",
    "eleve_eleve_moyen_deplacement": "mode_transport",
    "eleve_domicile_eleve": "residence_eleve",
    "eleve_iue": "identifiant_unique_eleve",
    
    # Informations sur la classe
    "classe_libelle": "nom_classe",
    "classe_code": "code_classe",
    "classe_category": "categorie_classe",
    
    # Informations sur la salle de classe
    "salle_classe_nbr_table": "nombre_tables_salle_classe",
    "salle_classe_libelle": "nom_salle_classe",
    "salle_classe_code": "code_salle_classe",
    
    # Informations du bulletin du premier trimestre
    "optbulletins_T1.id": "id_bulletin_t1",
    "optbulletins_T1.annee_scolaire": "annee_scolaire_t1",
    "optbulletins_T1.totaux": "points_totaux_t1",
    "optbulletins_T1.conduite_label": "appreciation_conduite_t1",
    "optbulletins_T1.sanction": "sanction_disciplinaire_t1",
    "optbulletins_T1.appreciation": "appreciation_enseignant_t1",
    "optbulletins_T1.plus_fort_moyenne": "moyenne_la_plus_elevee_t1",
    "optbulletins_T1.plus_faible_moyenne": "moyenne_la_plus_basse_t1",
    "optbulletins_T1.moyenne_classe": "moyenne_classe_t1",
    "optbulletins_T1.effectif": "effectif_classe_t1",
    "optbulletins_T1.rang": "rang_t1",
    "optbulletins_T1.moyenne": "moyenne_t1",
    "optbulletins_T1.moyenne_t1": "moyenne_periode1_t1",
    "optbulletins_T1.moyenne_annuel": "moyenne_annuelle_t1",
    "optbulletins_T1.rang_annuel": "rang_annuel_t1",
    "optbulletins_T1.decision_conseil": "decision_conseil_t1",
    "optbulletins_T1.composed": "compose_t1_finalise",
    "optbulletins_T1.motif_non_compose": "raison_non_compose_t1",
    
    # Informations du bulletin du deuxième trimestre
    "optbulletins_T2.id": "id_bulletin_t2",
    "optbulletins_T2.annee_scolaire": "annee_scolaire_t2",
    "optbulletins_T2.totaux": "points_totaux_t2",
    "optbulletins_T2.conduite_label": "appreciation_conduite_t2",
    "optbulletins_T2.sanction": "sanction_disciplinaire_t2",
    "optbulletins_T2.appreciation": "appreciation_enseignant_t2",
    "optbulletins_T2.plus_fort_moyenne": "moyenne_la_plus_elevee_t2",
    "optbulletins_T2.plus_faible_moyenne": "moyenne_la_plus_basse_t2",
    "optbulletins_T2.moyenne_classe": "moyenne_classe_t2",
    "optbulletins_T2.effectif": "effectif_classe_t2",
    "optbulletins_T2.rang": "rang_t2",
    "optbulletins_T2.moyenne": "moyenne_t2",
    "optbulletins_T2.moyenne_t1": "moyenne_periode1_t2",
    "optbulletins_T2.moyenne_t2": "moyenne_periode2_t2",
    "optbulletins_T2.moyenne_annuel": "moyenne_annuelle_t2",
    "optbulletins_T2.rang_annuel": "rang_annuel_t2",
    "optbulletins_T2.decision_conseil": "decision_conseil_t2",
    "optbulletins_T2.composed": "compose_t2_finalise",
    "optbulletins_T2.motif_non_compose": "raison_non_compose_t2",

    # Informations du bulletin du troisieme trimestre
    "optbulletins_T3.id": "id_bulletin_t3",
    "optbulletins_T3.totaux": "total_notes_t3",
    "optbulletins_T3.conduite": "conduite_t3",
    "optbulletins_T3.conduite_label": "conduite_label_t3",
    "optbulletins_T3.sanction": "sanction_t3",
    "optbulletins_T3.appreciation": "appreciation_t3",
    "optbulletins_T3.plus_fort_moyenne": "max_moyenne_t3",
    "optbulletins_T3.plus_faible_moyenne": "min_moyenne_t3",
    "optbulletins_T3.moyenne_classe": "moyenne_classe_t3",
    "optbulletins_T3.effectif": "effectif_t3",
    "optbulletins_T3.rang": "rang_t3",
    "optbulletins_T3.moyenne": "moyenne_t3",
    "optbulletins_T3.moyenne_t1": "moyenne_t1_t3",
    "optbulletins_T3.moyenne_t2": "moyenne_t2_t3",
    "optbulletins_T3.moyenne_annuel": "moyenne_annuelle_t3",
    "optbulletins_T3.rang_annuel": "rang_annuel_t3",
    "optbulletins_T3.decision_conseil": "decision_conseil_t3",
    "optbulletins_T3.composed": "compose_t3_finalise",
    "optbulletins_T3.motif_non_compose": "motif_non_compose_t3",
    # Informations de présence et retard
    "retard_absence_annee_id": "id_annee_presence",
    "retard_absence_type": "type_presence",
    "retard_absence_heure_debut": "heure_debut_absence",
    "retard_absence_heure_fin": "heure_fin_absence",
    "retard_absence_date_debut": "date_debut_absence",
    "retard_absence_date_fin": "date_fin_absence",
    "retard_absence_date_abandon": "date_abandon",
    "retard_absence_motif": "motif_absence",
    "retard_absence_matiere_id1": "matiere_absence_id1",
    "retard_absence_matiere_id2": "matiere_absence_id2",
    "retard_absence_matiere_id3": "matiere_absence_id3",
    "retard_absence_demi_jounee": "absence_demie_journee",

    
    
    "eleve_info_profession_pere": "profession_pere",
    "eleve_info_profession_mere": "profession_mere",
    "eleve_info_vie_chrez_tuteur": "vit_chez_tuteur",
    "eleve_info_eleve_statut": "statut_eleve",
    "eleve_info_eleve_handicap": "handicap",
    "eleve_info_eleve_victime_violence": "victime_violence",
    "eleve_info_victime_violence_physique": "victime_violence_physique",
    "eleve_info_victime_stigmatisation": "victime_stigmatisation",
    "eleve_info_victime_violence_sexuelle": "victime_violence_sexuelle",
    "eleve_info_victime_violence_emotionnelle": "victime_violence_emotionnelle",
    "eleve_info_victime_autre": "victime_violence_autre",
    "eleve_info_eleve_nationalite": "nationalite",
    "eleve_info_niveau_instruction_pere": "niveau_education_pere",
    "eleve_info_niveau_instruction_mere": "niveau_education_mere",
    "eleve_info_statut_mat_pere": "statut_matrimonial_pere",
    "eleve_info_statut_mat_mere": "statut_matrimonial_mere",
    "eleve_info_eleve_dort_moustiquaire": "dort_sous_moustiquaire",
    "eleve_info_eleve_distance_domicile": "distance_domicile",
    "eleve_info_eleve_moyen_deplacement": "mode_transport",
    "eleve_info_vie_chrez_parents" : "vit_avec_parents"
   
    
}
    df_final.rename(columns=renommage, inplace=True)

    # Supprimer les doublons
    df_final  = df_final.drop_duplicates()
    # 9. Recodage des variable
    # # Variables binaires Oui/Non
    variables_oui_non = [
        "freres_soeurs_frequentant", "redoublement_cp1", "redoublement_cp2", "redoublement_ce1", 
        "redoublement_ce2", "redoublement_cm1", "redoublement_cm2", "possede_telephone", 
        "suivi_officiel", "suivi_a_domicile", "suivi_au_centre", "suivi_en_groupe", 
        "possede_bureau", "possede_livres", "possede_tableaux", "possede_tablette", 
        "possede_autres_materiels", "menage_a_television", "menage_a_radio", "menage_a_internet", 
        "menage_a_electricite", "menage_a_autres_equipements", "vit_avec_parents", 
        "vit_au_domicile_parents", "vit_chez_tuteur", "victime_violence", 
        "victime_violence_physique", "victime_stigmatisation", "victime_violence_sexuelle", 
        "victime_violence_emotionnelle", "victime_autre_violence", "dort_sous_moustiquaire", "compose_t1_finalise",
        "compose_t2_finalise", "compose_t2_finalise, compose_t3_finalise"
    ]
    for var in variables_oui_non:
        if var in df_final.columns:
            df_final[var] = df_final[var].map({1.0: "Oui", 0.0: "Non"})
    return df_final

import pandas as pd
import pickle
import time

# ========================================
# BOUCLE PRINCIPALE DE TRAITEMENT
# ========================================

while True:
    # ========================================
    # 1. RÉCUPÉRATION ET PRÉPARATION DES DONNÉES
    # ========================================
    
    # Récupération des données depuis l'API
    df_initial = recuperer_donnees_api(data_url, headers)
    df_finale = traiter_donnees(df_initial)
    
    # Préparation des DataFrames pour traitement
    df_final1 = df_finale.copy()
    df_final2 = df_final1[["absence_demie_journee", "id_eleve"]]
    df_final3 = df_final1.drop(columns=["absence_demie_journee"])
    df_final3 = df_final3.drop_duplicates(subset=["id_eleve"])
    
    # Chargement des données pour calculs
    df = df_final3.copy()
    
    # ========================================
    # 2. CALCUL DES MOYENNES ET INDICATEURS DE RÉUSSITE
    # ========================================
    
    # Moyenne annuelle par élève
    df['moyenne_annuelle'] = df[['moyenne_t1', 'moyenne_t2', 'moyenne_t3']].mean(axis=1)
    
    # Taux de réussite global et par trimestre (élèves ayant moyenne ≥ 5)
    df['reussi'] = (df['moyenne_annuelle'] >= 5).astype(int)
    df['reussi_t1'] = (df['moyenne_t1'] >= 5).astype(int)
    df['reussi_t2'] = (df['moyenne_t2'] >= 5).astype(int)
    df['reussi_t3'] = (df['moyenne_t3'] >= 5).astype(int)
    
    # ========================================
    # 3. DÉFINITION DES FONCTIONS UTILITAIRES
    # ========================================
    
    # Fonction pour calculer et formater les taux en pourcentage
    def calculate_rate(df, group_cols, target_col, new_col_name):
        rate = df.groupby(group_cols)[target_col].mean() * 100
        if isinstance(group_cols, list):
            rate = rate.reset_index(name=new_col_name)
        else:
            rate = rate.reset_index().rename(columns={target_col: new_col_name})
        return rate.round(2)

    # Fonction pour calculer les effectifs par sexe avec préfixe personnalisable
    def calculate_effectif_sexe(df, group_cols, prefix=''):
        effectif = df.groupby(group_cols + ['genre_eleve']).size().unstack(fill_value=0)
        effectif.columns = [f'{prefix}effectif_{str(col).lower()}' for col in effectif.columns]
        return effectif.reset_index()

    # ========================================
    # 4. CALCULS DES STATISTIQUES PAR NIVEAU - ÉCOLE
    # ========================================
    
    # Effectifs et taux par sexe (niveau école)
    effectif_ecole_sexe = calculate_effectif_sexe(df, ['code_ecole'], prefix='ecole_')
    df = df.merge(effectif_ecole_sexe, on='code_ecole', how='left')

    taux_ecole_sexe = calculate_rate(df, ['code_ecole', 'genre_eleve'], 'reussi', 'taux_reussite')
    taux_ecole_sexe = taux_ecole_sexe.pivot(index='code_ecole', columns='genre_eleve', values='taux_reussite')
    taux_ecole_sexe.columns = [f'taux_reussite_ecole_{str(col).lower()}' for col in taux_ecole_sexe.columns]
    df = df.merge(taux_ecole_sexe.reset_index(), on='code_ecole', how='left')

    # ========================================
    # 5. CALCULS DES STATISTIQUES PAR NIVEAU - CLASSE
    # ========================================
    
    # Effectifs et taux par sexe (niveau classe)
    effectif_classe_sexe = calculate_effectif_sexe(df, ['code_ecole', 'nom_classe'], prefix='classe_')
    df = df.merge(effectif_classe_sexe, on=['code_ecole', 'nom_classe'], how='left')

    taux_classe_sexe = calculate_rate(df, ['code_ecole', 'nom_classe', 'genre_eleve'], 'reussi', 'taux_reussite')
    taux_classe_sexe = taux_classe_sexe.pivot(index=['code_ecole', 'nom_classe'], columns='genre_eleve', values='taux_reussite')
    taux_classe_sexe.columns = [f'taux_reussite_classe_{str(col).lower()}' for col in taux_classe_sexe.columns]
    df = df.merge(taux_classe_sexe.reset_index(), on=['code_ecole', 'nom_classe'], how='left')

    # ========================================
    # 6. CALCULS DES STATISTIQUES PAR NIVEAU - CEB
    # ========================================
    
    # Effectifs et taux par sexe (niveau CEB)
    effectif_ceb_sexe = calculate_effectif_sexe(df, ['ceb_ecole'], prefix='ceb_')
    df = df.merge(effectif_ceb_sexe, on='ceb_ecole', how='left')

    taux_ceb_sexe = calculate_rate(df, ['ceb_ecole', 'genre_eleve'], 'reussi', 'taux_reussite')
    taux_ceb_sexe = taux_ceb_sexe.pivot(index='ceb_ecole', columns='genre_eleve', values='taux_reussite')
    taux_ceb_sexe.columns = [f'taux_reussite_ceb_{str(col).lower()}' for col in taux_ceb_sexe.columns]
    df = df.merge(taux_ceb_sexe.reset_index(), on='ceb_ecole', how='left')

    # ========================================
    # 7. CALCULS DES EFFECTIFS GLOBAUX
    # ========================================
    
    # Effectifs globaux (école, classe, CEB)
    effectif_ecole = df.groupby('code_ecole').size().reset_index(name='effectif_total_ecole')
    df = df.merge(effectif_ecole, on='code_ecole', how='left')

    effectif_classe = df.groupby(['code_ecole', 'nom_classe']).size().reset_index(name='effectif_total_classe')
    df = df.merge(effectif_classe, on=['code_ecole', 'nom_classe'], how='left')

    effectif_ceb = df.groupby('ceb_ecole').size().reset_index(name='effectif_total_ceb')
    df = df.merge(effectif_ceb, on='ceb_ecole', how='left')

    # Nombre de classes par école
    nb_classes = df[['code_ecole', 'nom_classe']].drop_duplicates().groupby('code_ecole').size().reset_index(name='nb_classes')
    df = df.merge(nb_classes, on='code_ecole', how='left')

    # ========================================
    # 8. STATISTIQUES PAR NIVEAU GÉOGRAPHIQUE
    # ========================================
    
    # Statistiques par commune/province/région
    for niveau in ['commune_ecole', 'province_ecole', 'region_ecole']:
        # Effectifs par sexe avec préfixe spécifique
        effectif_niv_sexe = calculate_effectif_sexe(df, [niveau], prefix=f'{niveau}_')
        df = df.merge(effectif_niv_sexe, on=niveau, how='left')
        
        # Taux de réussite
        taux = calculate_rate(df, niveau, 'reussi', f'taux_reussite_{niveau}')
        df = df.merge(taux, on=niveau, how='left')
    
        # Taux par sexe
        taux_sexe = calculate_rate(df, [niveau, 'genre_eleve'], 'reussi', 'taux_reussite')
        taux_sexe = taux_sexe.pivot(index=niveau, columns='genre_eleve', values='taux_reussite')
        taux_sexe.columns = [f'taux_reussite_{niveau}_{str(col).lower()}' for col in taux_sexe.columns]
        df = df.merge(taux_sexe.reset_index(), on=niveau, how='left')

    # ========================================
    # 9. TAUX DE RÉUSSITE PAR TRIMESTRE
    # ========================================
    
    # Taux de réussite par trimestre aux différents niveaux
    for trim in ['t1', 't2', 't3']:
        # Niveau école
        taux = calculate_rate(df, 'code_ecole', f'reussi_{trim}', f'taux_reussite_ecole_{trim}')
        df = df.merge(taux, on='code_ecole', how='left')
    
        # Niveau classe
        taux_classe = calculate_rate(df, ['code_ecole', 'nom_classe'], f'reussi_{trim}', f'taux_reussite_classe_{trim}')
        df = df.merge(taux_classe, on=['code_ecole', 'nom_classe'], how='left')
    
        # Niveau CEB
        taux_ceb = calculate_rate(df, 'ceb_ecole', f'reussi_{trim}', f'taux_reussite_ceb_{trim}')
        df = df.merge(taux_ceb, on='ceb_ecole', how='left')

    # ========================================
    # 10. FINALISATION DES DONNÉES
    # ========================================
    
    # Suppression des doublons
    df = df.drop_duplicates()
    
    # Merge avec les données d'absence
    df_merg = pd.merge(df, df_final2, on="id_eleve", how="inner")
    df_finale = df_merg.copy()

    # ========================================
    # 11. RECODAGE DES VARIABLES CATÉGORIELLES
    # ========================================
    
    # Dictionnaires de recodage
    recodages = {
        'a_bourse_etude': {
            1.0: "Excellence", 
            2.0: "Ordinaire", 
            3.0: "Spécifique", 
            4.0: "Aucune"
        },
        'statut_ecole': {
            1.0: "Public",
            2.0: "Privé Laïc",
            3.0: "Privé Catholique",
            4.0: "Privé Musulman",
            5.0: "Privé Protestant",
            6.0: "Communautaire (uniquement pour le primaire)"
        },
        'situation_administrative_ecole': {
            1.0: "Reconnu",
            2.0: "Non Reconnu"
        },
        'mode_recrutement_ecole': {
            1.0: "Annuel",
            2.0: "Biennal"
        },
        'milieu_ecole': {
            1.0: "Urbain",
            2.0: "Rural"
        },
        'genre_eleve': {
            1.0: "Masculin",
            2.0: "Féminin"
        },
        'profession_pere': {
            1.0: "Cultivateur",
            2.0: "Eleveur",
            3.0: "Artisan",
            4.0: "Employé secteur public",
            5.0: "Employé secteur privé",
            6.0: "Employé de Commerce",
            7.0: "Retraité",
            8.0: "Profession libérale",
            9.0: "Sans emploi",
            10.0: "Autres (à préciser)"
        },
        'profession_mere': {
            1.0: "Cultivateur",
            2.0: "Eleveur",
            3.0: "Artisan",
            4.0: "Employé secteur public",
            5.0: "Employé secteur privé",
            6.0: "Employé de Commerce",
            7.0: "Retraité",
            8.0: "Profession libérale",
            9.0: "Sans emploi",
            10.0: "Autres (à préciser)"
        },
        'statut_eleve': {
            1.0: "Hôte",
            2.0: "PDI",
            3.0: "Réfugié"
        },
        'niveau_education_pere': {
            0.0: "Aucun",
            1.0: "Primaire",
            2.0: "Post-primaire",
            3.0: "Secondaire",
            4.0: "Supérieur"
        },
        'niveau_education_mere': {
            0.0: "Aucun",
            1.0: "Primaire",
            2.0: "Post-primaire",
            3.0: "Secondaire",
            4.0: "Supérieur"
        },
        'statut_matrimonial_pere': {
            1.0: "Marié",
            2.0: "Marié(e) monogame",
            3.0: "Marié(e) polygame",
            4.0: "Union libre",
            5.0: "Célibataire",
            6.0: "Divorcé(e)",
            7.0: "Séparé(e)",
            8.0: "Veuf/Veuve"
        },
        'statut_matrimonial_mere': {
            1.0: "Marié",
            2.0: "Marié(e) monogame",
            3.0: "Marié(e) polygame",
            4.0: "Union libre",
            5.0: "Célibataire",
            6.0: "Divorcé(e)",
            7.0: "Séparé(e)",
            8.0: "Veuf/Veuve"
        },
        'distance_domicile': {
            1.0: "Moins de 3 km",
            2.0: "3 km à 5 km",
            3.0: "Plus de 5 km"
        },
        'mode_transport': {
            1.0: "Moto",
            2.0: "Vélo",
            3.0: "Bus",
            4.0: "A pied",
            5.0: "Autre à préciser"
        },
        'type_ecole': {
            1.0: "Privée",
            0.0: "Publique"
        },
        'sexe_directeur': {
            1: "homme",
            2: "femme"
        },
        'nationalite': {
            1.0: "Burkinabè",
            0.0: "autre nationalité"
        },
        'commune_ecole': {
            1.0: "BINGO",
            2.0: "MANGA"
        },
        'ceb_ecole': {
            1.0: "CEB de BINGO",
            2.0: "CEB de MANGA"
        },
        'region_ecole': {
            1.0: "Centre-Ouest",
            2.0: "Centre-Sud"
        },
        'province_ecole': {
            1.0: "Boulkiemdé",
            2.0: "Zoundwéogo"
        },
        'handicap': {
            1.0: "Handicap mental",
            2.0: "Handicap visuel",
            3.0: "Handicap auditif",
            4.0: "Handicap physique",
            5.0: "Trouble de langage",
            6.0: "Epilepsie",
            7.0: "Autisme"
        },
        'type_enseignement_ecole': {
            1.0: "Bilingue",
            2.0: "Classique",
            3.0: "Franco-arabe",
            4.0: "Passerelle"
        },
        'motif_absence': {
            1.0: "Absence non justifiée",
            2.0: "Enfant/Famille en voyage",
            3.0: "Décès dans la famille",
            4.0: "Raison familiale",
            5.0: "Maladie de l'enfant"
        },
        'absence_demie_journee': {
            1.0: "matin",
            2.0: "soir"
        }
    }

    # Appliquer tous les recodages
    for variable, mapping in recodages.items():
        if variable in df_finale.columns:
            df_finale[variable] = df_finale[variable].map(mapping)

    # ========================================
    # 12. SAUVEGARDE DES RÉSULTATS
    # ========================================
    
    # Sauvegarde des résultats
    with open("df_finale.pkl", "wb") as f:
        pickle.dump(df_finale, f)

    df_finale.to_csv("donnees_indicateurs.csv", sep=";", index=False)
    print(f"\nNombre de lignes finales : {df_finale.shape[0]}")
    
    # Pause avant la prochaine itération
    time.sleep(20)
