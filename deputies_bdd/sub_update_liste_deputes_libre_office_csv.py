import os
import io
import sys
import requests
import pandas as pd
from datetime import datetime  # même si on ne l'utilise pas directement, je laisse comme dans ton code


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les noms de colonnes :
    - trim des espaces
    - supprime les guillemets "
    - supprime un éventuel BOM au début
    """
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.replace('"', '', regex=False)
        .str.replace('\ufeff', '', regex=False)
    )
    return df


def get_project_root():
    """
    Retourne le chemin absolu du dossier racine 'RÉCUPÉRATION INFO SCRUTINS'
    en partant du script (qui est placé dans deputies_bdd/).
    """
    this_file = os.path.abspath(__file__)
    deputies_bdd_dir = os.path.dirname(this_file)
    project_root = os.path.dirname(deputies_bdd_dir)
    return project_root


def find_remote_csv_url():
    """
    Trouve l’URL du CSV sur la page des députés en exercice.
    """
    page_url = "https://data.assemblee-nationale.fr/acteurs/deputes-en-exercice"
    print(f"Téléchargement de la page: {page_url}")
    resp = requests.get(page_url, timeout=20)
    resp.raise_for_status()

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(resp.text, "html.parser")
    csv_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".csv" in href.lower():
            if href.startswith("http"):
                csv_links.append(href)
            else:
                csv_links.append(requests.compat.urljoin(page_url, href))

    if not csv_links:
        raise RuntimeError("Impossible de trouver un lien CSV sur la page.")

    csv_url = csv_links[0]
    print(f"URL CSV trouvée: {csv_url}")
    return csv_url


def download_remote_csv(csv_url):
    """
    Télécharge le CSV distant et le retourne sous forme de DataFrame pandas.
    ⚠️ Ici on utilise sep="," car le fichier est séparé par des virgules.
    """
    print(f"Téléchargement du CSV distant: {csv_url}")
    resp = requests.get(csv_url, timeout=60)
    resp.raise_for_status()

    df = pd.read_csv(
        io.StringIO(resp.content.decode("utf-8", errors="replace")),
        sep=",",
        dtype=str
    )
    df = clean_columns(df)
    print("Colonnes du CSV distant :", list(df.columns))
    print(f"CSV distant: {len(df)} lignes chargées.")
    return df


def load_local_csv(local_path):
    """
    Charge le CSV local s’il existe, avec le même séparateur que le distant.
    """
    if not os.path.exists(local_path):
        print(f"⚠️ Fichier local inexistant: {local_path}")
        return None

    print(f"Chargement du CSV local: {local_path}")
    df = pd.read_csv(local_path, sep=",", dtype=str)
    df = clean_columns(df)
    print("Colonnes du CSV local   :", list(df.columns))
    print(f"CSV local: {len(df)} lignes chargées.")
    return df


def append_new_rows(local_path, df_remote, df_local, id_columns=None):
    """
    Compare df_remote et df_local, trouve les nouvelles lignes
    et les ajoute au CSV local.
    """
    if df_local is None:
        print("Aucun fichier local: on crée un nouveau CSV avec toutes les lignes distantes.")
        df_remote.to_csv(local_path, sep=",", index=False)
        print(f"✅ Nouveau fichier créé: {local_path} ({len(df_remote)} lignes).")
        return

    # Colonnes communes
    common_cols = [c for c in df_remote.columns if c in df_local.columns]
    if not common_cols:
        raise RuntimeError("Aucune colonne commune entre le CSV local et le CSV distant !")

    df_remote_cmp = df_remote[common_cols].copy()
    df_local_cmp = df_local[common_cols].copy()

    # Colonnes identifiantes
    if id_columns is not None:
        for c in id_columns:
            if c not in common_cols:
                raise RuntimeError(f"La colonne d'identifiant '{c}' n'existe pas dans le CSV.")
    else:
        id_columns = common_cols

    def make_key(df, cols):
        return df[cols].fillna("").agg("||".join, axis=1)

    local_keys = set(make_key(df_local_cmp, id_columns))
    remote_keys = make_key(df_remote_cmp, id_columns)

    is_new = ~remote_keys.isin(local_keys)
    df_new = df_remote[is_new].copy()

    if df_new.empty:
        print("✅ Aucun nouveau député à ajouter (0 nouvelle ligne).")
        return

    print(f"👉 Nouvelles lignes détectées: {len(df_new)}")

    df_updated = pd.concat([df_local, df_new], ignore_index=True)
    df_updated.to_csv(local_path, sep=",", index=False)
    print(f"✅ Fichier mis à jour: {local_path}")
    print(f"   - Anciennes lignes : {len(df_local)}")
    print(f"   - Nouvelles lignes : {len(df_new)}")
    print(f"   - Total           : {len(df_updated)}")


def generate_deputies_csv(deputies_bdd_dir: str, local_csv_path: str):
    """
    À partir de liste_deputes_libre_office.csv (mis à jour),
    génère le fichier deputies.csv en enrichissant avec departement.csv et acteurs.csv.
    """
    print("🔧 Génération de deputies.csv à partir de liste_deputes_libre_office.csv...")

    # --- 1) Charger les fichiers ---------------------------------------------------
    df = pd.read_csv(local_csv_path)
    dept_path = os.path.join(deputies_bdd_dir, "data_source", "departement.csv")
    acteurs_path = os.path.join(deputies_bdd_dir, "data_source", "acteur", "acteurs.csv")

    dept = pd.read_csv(dept_path)
    # Charger acteurs.csv (colonnes: depute_id, sexe, date_naissance, etc.)
    acteurs = pd.read_csv(acteurs_path, sep=";")

    # Supprimer la colonne source_file si elle existe
    if "source_file" in acteurs.columns:
        acteurs = acteurs.drop(columns=["source_file"])

    # --- 2) Renommer colonnes de base ------------------------------------------------
    df = df.rename(columns={
        "identifiant": "depute_id",
        "Numéro de circonscription": "circonscription_id"
    })

    # --- 3) Ajouter URL photo --------------------------------------------------------
    base_url = "https://www.assemblee-nationale.fr/dyn/static/tribun/17/photos/carre/"
    df["photo_url"] = df["depute_id"].astype(str).apply(lambda x: f"{base_url}{x}.jpg")

    # --- 4) Join avec departement.csv ------------------------------------------------
    df = df.merge(
        dept,
        left_on="Département",
        right_on="nom",
        how="left"
    )

    # --- 5) circonscription_id sur 2 caractères --------------------------------------
    df["circonscription_id"] = (
        df["circonscription_id"]
        .astype("Int64")
        .astype(str)
        .str.zfill(2)
    )

    # --- 6) Créer la colonne real_circonscription_id ---------------------------------
    df["real_circonscription_id"] = ""
    dep_col = df["Département"].fillna("")

    # DOM-TOM (codes spéciaux)
    df.loc[dep_col.str.contains("Réunion", case=False), "real_circonscription_id"] = "ZD" + df["circonscription_id"]
    df.loc[dep_col.str.contains("Guadeloupe", case=False), "real_circonscription_id"] = "ZA" + df["circonscription_id"]
    df.loc[dep_col.str.contains("Martinique", case=False), "real_circonscription_id"] = "ZB" + df["circonscription_id"]
    df.loc[dep_col.str.contains("Guyane", case=False), "real_circonscription_id"] = "ZC" + df["circonscription_id"]
    df.loc[dep_col.str.contains("Saint-Pierre-et-Miquelon", case=False), "real_circonscription_id"] = "ZS" + df["circonscription_id"]
    df.loc[dep_col.str.contains("Mayotte", case=False), "real_circonscription_id"] = "ZM" + df["circonscription_id"]

    mask_zt = (
        dep_col.str.contains("Saint-Barth", case=False) |
        dep_col.str.contains("Saint-Martin", case=False)
    )
    df.loc[mask_zt, "real_circonscription_id"] = "ZT" + df["circonscription_id"]

    df.loc[dep_col.str.contains("Polynésie", case=False), "real_circonscription_id"] = "ZP" + df["circonscription_id"]
    df.loc[dep_col.str.contains("Wallis-et-Futuna", case=False), "real_circonscription_id"] = "ZW" + df["circonscription_id"]
    df.loc[dep_col.str.contains("Nouvelle-Calédonie", case=False), "real_circonscription_id"] = "ZN" + df["circonscription_id"]

    # Ajouter code standard si applicable
    mask_has_code = df["code"].notna()
    mask_empty_real = df["real_circonscription_id"] == ""

    df.loc[mask_has_code & mask_empty_real, "real_circonscription_id"] = (
        df.loc[mask_has_code & mask_empty_real, "code"].astype(str)
        + df.loc[mask_has_code & mask_empty_real, "circonscription_id"].astype(str)
    )

    # --- 7) Drop et renommage final ---------------------------------------------------
    df = df.drop(columns=["code", "nom", "circonscription_id"])
    df = df.rename(columns={
        "real_circonscription_id": "circonscription_id",
        "Prénom": "first_name",
        "Nom": "last_name",
        "Région": "region_name",
        "Département": "department_name",
        "Profession": "profession",
        "Groupe politique (complet)": "political_party_name",
        "Groupe politique (abrégé)": "political_party_abbreviation"
    })

    # --- 8) Merge avec acteurs.csv sur depute_id --------------------------------------
    df = df.merge(
        acteurs,
        on="depute_id",
        how="left"
    )

    # --- 8 bis) Ajouter la colonne AGE ------------------------------------------------
    # Convertir les dates en datetime (avec gestion des NaN)
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")
    df["date_deces"] = pd.to_datetime(df["date_deces"], errors="coerce")

    today = pd.Timestamp.today()

    # Si date_deces existe → âge à la mort
    # Sinon → âge aujourd'hui
    df["age"] = df.apply(
        lambda row: (
            (row["date_deces"] - row["date_naissance"]).days // 365
            if pd.notna(row["date_deces"])
            else (today - row["date_naissance"]).days // 365
        ) if pd.notna(row["date_naissance"]) else None,
        axis=1
    )

    # --- 9) Réordonner les colonnes ---------------------------------------------------
    final_cols = [
        "depute_id",
        "first_name",
        "last_name",
        "sexe",
        "date_naissance",
        "age",
        "ville_naissance",
        "departement_naissance",
        "pays_naissance",
        "date_deces",
        "circonscription_id",
        "region_name",
        "department_name",
        "profession",
        "political_party_name",
        "political_party_abbreviation",
        "photo_url",
        "type_mandat",
        "mandat_ref",
        "acteur_ref",
        "mandat_debut",
        "mandat_fin"
    ]

    other_cols = [c for c in df.columns if c not in final_cols]
    df = df[final_cols + other_cols]

    # --- 10) Export final -------------------------------------------------------------
    output_path = os.path.join(deputies_bdd_dir, "deputies.csv")
    df.to_csv(output_path, index=False)

    print(f"✅ deputies.csv généré avec succès ! ({len(df)} lignes)")
    print(f"   → {output_path}")


def main():
    project_root = get_project_root()
    deputies_bdd_dir = os.path.join(project_root, "deputies_bdd")

    local_csv_path = os.path.join(
        deputies_bdd_dir,
        "data_source",
        "liste_deputes_libre_office.csv"
    )

    try:
        # 1) Mise à jour du CSV brut
        csv_url = find_remote_csv_url()
        df_remote = download_remote_csv(csv_url)
        df_local = load_local_csv(local_csv_path)

        # Maintenant la colonne 'identifiant' existe bien
        append_new_rows(local_csv_path, df_remote, df_local, id_columns=["identifiant"])

        # 2) Génération du fichier enrichi deputies.csv
        generate_deputies_csv(deputies_bdd_dir, local_csv_path)

    except Exception as e:
        print("❌ Erreur lors de la mise à jour du CSV des députés :", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
