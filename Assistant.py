import os
import re
#import pickle
import pandas as pd
import streamlit as st
from langchain.prompts import PromptTemplate
from langchain_ollama import ChatOllama
from functools import reduce
import operator

# 🎨 Configuration de la page
st.set_page_config(page_title="🎓 Analyse Scolaire", layout="wide")

st.title("🎓 Chatbot Scolaire - Analyse des Performances")

# 🗃️ Chargement des données
@st.cache_data(ttl=5184000)
def load_data():
    df = pd.read_csv("donnees_nettoyees.csv", sep=';', encoding='ISO-8859-1', low_memory=False) 
    return df

df_finale = load_data()

# 🔧 Initialisation du modèle
llm = ChatOllama(model="gemma:2b", temperature=0.7)
#llm = ChatOllama(model="mistral", temperature=0.7)
# 📋 Template prompt
prompt_template = PromptTemplate(
    input_variables=["question", "donnees"],
    template="""
Tu es un enseignant expérimenté au Burkina Faso. 
Tu dois répondre à une question sur les performances scolaires **en te basant uniquement sur les données fournies**. 
Ta réponse doit être claire, naturelle et structurée. ❗N’invente rien et ne fais pas de tableau.

📋 CONTEXTE DE LA QUESTION — Identifie d'abord le type de question :

🧠 LOGIQUE D'ANALYSE :

ÉTAPE 1 - Détecte les indicateurs temporels dans la question :
- Si mentions "premier/1er/T1" → Focus sur colonnes avec "_t1"
- Si mentions "deuxième/2ème/T2" → Focus sur colonnes avec "_t2"  
- Si mentions "troisième/3ème/T3" → Focus sur colonnes avec "_t3"
- Si pas de précision → Donne info des 3 trimestres

Etape 2 - SI LA QUESTION CONCERNE UN ÉLÈVE (id_eleve, identifiant_unique_eleveou ) SPÉCIFIQUE :

### Dabord
- Présente l’école et la classe.
### ensuite 
- donne
  • ses notes des matières (Calcul_T1, Conjugaison_T1, Copie_T1, Dessin_T1, Dictée_T1, 
  Ecriture_T1, Etude de Texte_T1, Exercices d'Observation_T1) en fonction de trimestre demande 
  • ses moyennes (moyenne_t1, moyenne_t2, moyenne_t3), 
  • ses rangs (rang_t1, rang_t2, rang_t3)
  • Analyse des points forts et des matières faibles
  • Comparaison avec la moyenne de la classe (moyenne_classe_t1, moyenne_classe_t2, moyenne_classe_t3)
### en outre
- donne
  • Conditions de vie (mode_transport, dort_sous_moustiquaire, vit_avec_parents, vit_chez_tuteur)
  • Ressources personnelles (possede_telephone, eleve_possede_tel, possede_bureau, possede_livres, possede_tableaux,
   possede_tablette, possede_autres_materiels)
  • Équipements du ménage (menage_a_television,menage_a_radio, menage_a_internet, menage_a_electricite, menage_a_autres_equipements)
  • Bien-être et santé (handicap, victime_violence,victime_violence_physique, victime_stigmatisation,victime_violence_sexuelle,
   victime_violence_emotionnelle, victime_violence_autre)
  • Suivi pédagogique (suivi_officiel, suivi_a_domicile, suivi_au_centre,suivi_en_groupe)
  • Parcours scolaire (est_redoublant, est_affecte,frequentation_precedente,freres_soeurs_frequentant,
  a_bourse_etude, redoublement_cp1, redoublement_cp2, redoublement_ce1, redoublement_ce2, redoublement_cm1,redoublement_cm2,
    statut_eleve)
  • Assiduité et présence (type_presence, heure_debut_absence, heure_fin_absence, date_debut_absence,
   date_fin_absence,date_abandon, motif_absence,absence_demie_journee)

📊 QUESTION SUR UNE CLASSE OU ÉCOLE :
- Fournis les statistiques générales (moyenne_classe_t1, moyenne_classe_t2, moyenne_classe_t3) demandées
- Compare les élèves si c’est pertinent
- Dégage des tendances

📘 QUESTION SUR UNE MATIÈRE SPÉCIFIQUE :
- Analyse uniquement les performances dans cette matière

🔍 QUESTION SUR UN ASPECT PARTICULIER :
- Traite uniquement cet aspect
- Ne mentionne pas d’informations non demandées

⚡ PRINCIPES CLÉS :
- Réponds uniquement à ce qui est demandé
- N’ajoute aucune supposition
- Sois structuré, naturel et pédagogique
- Propose des conseils
- commenter chaque phrase 
- soit autononne dans tes commentaire et interpretation

Question :
{question}

Données :
{donnees}

➡️ Donne une réponse professionnelle, claire et adaptée au contexte scolaire.
"""
)


# 🔍 Fonction de détection de filtre
def extraire_filtre(question, valeurs_connues):
    for val in valeurs_connues:
        if val and str(val).lower() in question.lower():
            return val
    return None

# 🔁 Fonction principale
def get_response_from_dataframe(question, df):
    reponses = []
    question_lower = question.lower()

    id_eleve = extraire_filtre(question_lower, df['id_eleve'].astype(str).unique())
    identifiant_unique = extraire_filtre(question_lower, df['identifiant_unique_eleve'].astype(str).unique())
    id_classe = extraire_filtre(question_lower, df['id_classe'].astype(str).unique())
    code_classe = extraire_filtre(question_lower, df['code_classe'].astype(str).unique())
    nom_classe = extraire_filtre(question_lower, df['nom_classe'].astype(str).unique())
    nom_ecole = extraire_filtre(question_lower, df['nom_ecole'].astype(str).unique())
    code_ecole = extraire_filtre(question_lower, df['code_ecole'].astype(str).unique())
    ceb = extraire_filtre(question_lower, df['ceb_ecole'].astype(str).unique())
    commune = extraire_filtre(question_lower, df['commune_ecole'].astype(str).unique())
    id_ecole = extraire_filtre(question_lower, df['id_ecole'].astype(str).unique())

    # 🎯 Par élève
    if id_eleve or identifiant_unique:
        ident = id_eleve or identifiant_unique
        ligne = df[(df['id_eleve'].astype(str) == ident) | (df['identifiant_unique_eleve'].astype(str) == ident)]
        if not ligne.empty:
            ligne = ligne.iloc[0]
            donnees_texte = "\n".join([f"{col} : {ligne[col]}" for col in df.columns if col in ligne])
            prompt = prompt_template.format(question=question, donnees=donnees_texte)
            resultat = llm.invoke(prompt)
            return resultat.content if hasattr(resultat, 'content') else resultat

    # 🎯 Par classe / école
    filtres = []
    if nom_ecole: filtres.append(df['nom_ecole'].str.lower() == nom_ecole.lower())
    if code_ecole: filtres.append(df['code_ecole'].astype(str) == str(code_ecole))
    if ceb: filtres.append(df['ceb_ecole'].astype(str) == str(ceb))
    if commune: filtres.append(df['commune_ecole'].astype(str) == str(commune))
    if code_classe: filtres.append(df['code_classe'].astype(str) == str(code_classe))
    if nom_classe: filtres.append(df['nom_classe'].str.lower() == nom_classe.lower())
    if id_classe: filtres.append(df['id_classe'].astype(str) == str(id_classe))
    if id_ecole: filtres.append(df['id_ecole'].astype(str) == str(id_ecole))

    if filtres:
        condition = reduce(operator.and_, filtres)
        df_filtre = df[condition]
        if df_filtre.empty:
            return "Aucune donnée trouvée avec les critères spécifiés."

        nb_eleves = df_filtre.shape[0]

        if "classe" in question_lower or "classes" in question_lower:
            classes = df_filtre['nom_classe'].unique()
            for classe in classes:
                df_classe = df_filtre[df_filtre['nom_classe'] == classe]
                resume = {col: df_classe[col].mean() for col in df_classe.columns if df_classe[col].dtype != 'object'}
                donnees_texte = f"Classe : {classe}\n" + "\n".join([f"{k} : {v:.2f}" for k, v in resume.items()])
                prompt = prompt_template.format(question=question, donnees=donnees_texte)
                resultat = llm.invoke(prompt)
                if hasattr(resultat, 'content'):
                    resultat = resultat.content
                reponses.append(f"Classe {classe} :\n{resultat}")
            return "\n\n---\n\n".join(reponses)

        elif "école" in question_lower or "ecole" in question_lower or "établissement" in question_lower:
            resume = {col: df_filtre[col].mean() for col in df_filtre.columns if df_filtre[col].dtype != 'object'}
            donnees_texte = f"Ecole : {df_filtre['nom_ecole'].iloc[0]}\n" + "\n".join([f"{k} : {v:.2f}" for k, v in resume.items()])
            prompt = prompt_template.format(question=question, donnees=donnees_texte)
            resultat = llm.invoke(prompt)
            return resultat.content if hasattr(resultat, 'content') else resultat

        elif "ceb" in question_lower or "commune" in question_lower:
            resume = df_filtre.groupby("nom_ecole").mean(numeric_only=True)
            donnees_texte = resume.round(2).to_string()
            prompt = prompt_template.format(question=question, donnees=donnees_texte)
            resultat = llm.invoke(prompt)
            return resultat.content if hasattr(resultat, 'content') else resultat

        resume = {col: df_filtre[col].mean() for col in df_filtre.columns if df_filtre[col].dtype != 'object'}
        donnees_texte = "Résumé global :\n" + "\n".join([f"{k} : {v:.2f}" for k, v in resume.items()])
        prompt = prompt_template.format(question=question, donnees=donnees_texte)
        resultat = llm.invoke(prompt)
        return resultat.content if hasattr(resultat, 'content') else resultat

    return "Aucun filtre détecté dans la question. Veuillez spécifier un élève, une classe ou une école."

# Initialisation des états
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "reset_chat" not in st.session_state:
    st.session_state.reset_chat = False

# 📋 Barre latérale
with st.sidebar:
    st.header("🗂️ Menu")

    if st.button("🆕 Nouveau Chat"):
        st.session_state.reset_chat = True  # Active le reset

    st.markdown("---")
    st.subheader("📜 Historique")

    if st.session_state.chat_history:
        conversations = []
        buffer = []
        for msg in st.session_state.chat_history:
            buffer.append(msg)
            if len(buffer) == 2:
                conversations.append(buffer)
                buffer = []

        for i, conv in enumerate(conversations):
            question = conv[0]["content"].strip().split("\n")[0][:60]
            if st.button(f"🗨️ {question}", key=f"conv_{i}"):
                st.session_state.selected_chat = conv
    else:
        st.info("Aucun échange pour le moment.")
# 🔁 Réinitialisation du chat (à faire *hors* du bouton)
if st.session_state.reset_chat:
    st.session_state.chat_history = []
    st.session_state.selected_chat = []
    st.success("Nouveau chat démarré.")
    st.session_state.reset_chat = False  # On désactive le flag

# 💬 Saisie utilisateur
user_input = st.chat_input("Pose ta question")

if user_input:
    response = get_response_from_dataframe(user_input, df_finale)
    st.session_state.chat_history.append({"role": "user", "content": user_input})
    st.session_state.chat_history.append({"role": "assistant", "content": response})

    with st.chat_message("user"):
        st.write(user_input)
    with st.chat_message("assistant"):
        st.write(response)

# 🔄 Affichage historique principal
if "selected_chat" in st.session_state:
    for message in st.session_state.selected_chat:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
else:
    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
