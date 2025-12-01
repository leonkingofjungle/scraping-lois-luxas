import pandas as pd
from pathlib import Path
import zipfile
from io import BytesIO
import time
import requests

# URL OFFICIELLE (sans faute)
ZIP_URL = (
    "http://data.assemblee-nationale.fr/static/openData/repository/17/amo/"
    "deputes_actifs_mandats_actifs_organes_divises/"
    "AMO40_deputes_actifs_mandats_actifs_organes_divises.csv.zip"
)

# Dossier local où tu stockes les CSV "acteur"
LOCAL_ACTEUR_FOLDER = Path("data_source/acteur")


def build_deputes_from_acteurs(folder_path: str | Path, output_filename: str = "acteurs.csv"):
    """
    Parcourt tous les fichiers CSV du dossier 'acteur',
    en extrait les infos utiles et construit un gros CSV fusionné : acteurs.csv
    """
    folder = Path(folder_path)

    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"Le dossier '{folder_path}' n'existe pas ou n'est pas un dossier.")

    # Chercher tous les CSV du dossier
    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Aucun fichier .csv trouvé dans le dossier '{folder_path}'.")

    print(f"{len(csv_files)} fichiers CSV trouvés dans {folder.resolve()} :")
    for f in csv_files:
        print(f" - {f.name}")

    all_rows = []

    for csv_file in csv_files:
        print(f"\nLecture et traitement de {csv_file.name} ...")

        try:
            df = pd.read_csv(csv_file, sep=";", encoding="utf-8", dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(csv_file, sep=";", encoding="latin-1", dtype=str)

        # Sous-dataframe avec les colonnes qui t’intéressent
        sub = pd.DataFrame()

        # 1) uid[1] -> depute_id (enlevant "PA" au début)
        col_uid = "uid[1]"
        if col_uid in df.columns:
            sub["depute_id"] = (
                df[col_uid]
                .astype(str)
                .str.replace(r"^PA", "", regex=True)
            )
        else:
            sub["depute_id"] = pd.NA

        # 2) etatCivil[1]/ident[1]/civ[1] -> sexe (M. -> Homme, Mme -> Femme)
        col_civ = "etatCivil[1]/ident[1]/civ[1]"
        if col_civ in df.columns:
            civ = df[col_civ].astype(str).str.strip()
            sub["sexe"] = civ.map({
                "M.": "Homme",
                "Mme": "Femme"
            })
        else:
            sub["sexe"] = pd.NA

        # 3) etatCivil[1]/infoNaissance[1]/dateNais[1] -> date_naissance
        col_date_nais = "etatCivil[1]/infoNaissance[1]/dateNais[1]"
        sub["date_naissance"] = df[col_date_nais] if col_date_nais in df.columns else pd.NA

        # 4) etatCivil[1]/infoNaissance[1]/villeNais[1] -> ville_naissance
        col_ville_nais = "etatCivil[1]/infoNaissance[1]/villeNais[1]"
        sub["ville_naissance"] = df[col_ville_nais] if col_ville_nais in df.columns else pd.NA

        # 5) etatCivil[1]/infoNaissance[1]/depNais[1] -> departement_naissance
        col_dep_nais = "etatCivil[1]/infoNaissance[1]/depNais[1]"
        sub["departement_naissance"] = df[col_dep_nais] if col_dep_nais in df.columns else pd.NA

        # 6) etatCivil[1]/infoNaissance[1]/paysNais[1] -> pays_naissance
        col_pays_nais = "etatCivil[1]/infoNaissance[1]/paysNais[1]"
        sub["pays_naissance"] = df[col_pays_nais] if col_pays_nais in df.columns else pd.NA

        # 7) etatCivil[1]/dateDeces[1] -> date_deces
        col_date_deces = "etatCivil[1]/dateDeces[1]"
        sub["date_deces"] = df[col_date_deces] if col_date_deces in df.columns else pd.NA

        # 8) mandats[1]/mandat[1]@xsi:type -> type_mandat
        col_type_mandat = "mandats[1]/mandat[1]@xsi:type"
        sub["type_mandat"] = df[col_type_mandat] if col_type_mandat in df.columns else pd.NA

        # 9) mandats[1]/mandat[1]/uid[1] -> mandat_ref
        col_mandat_uid = "mandats[1]/mandat[1]/uid[1]"
        sub["mandat_ref"] = df[col_mandat_uid] if col_mandat_uid in df.columns else pd.NA

        # 10) mandats[1]/mandat[1]/acteurRef[1] -> acteur_ref
        col_acteur_ref = "mandats[1]/mandat[1]/acteurRef[1]"
        sub["acteur_ref"] = df[col_acteur_ref] if col_acteur_ref in df.columns else pd.NA

        # 11) mandats[1]/mandat[1]/dateDebut[1] -> mandat_debut
        col_mandat_debut = "mandats[1]/mandat[1]/dateDebut[1]"
        sub["mandat_debut"] = df[col_mandat_debut] if col_mandat_debut in df.columns else pd.NA

        # 12) mandats[1]/mandat[1]/dateFin[1] -> mandat_fin
        col_mandat_fin = "mandats[1]/mandat[1]/dateFin[1]"
        sub["mandat_fin"] = df[col_mandat_fin] if col_mandat_fin in df.columns else pd.NA

        # Optionnel : trace du fichier source
        sub["source_file"] = csv_file.name

        all_rows.append(sub)

    # Concat finale
    full_df = pd.concat(all_rows, ignore_index=True)

    # Sauvegarde
    output_path = folder / output_filename
    full_df.to_csv(output_path, sep=";", index=False, encoding="utf-8")

    print("\n✅ Fichier CSV final généré.")
    print(f"➡ {output_path.resolve()}")
    print(f"➡ Nombre total de lignes : {len(full_df)}")
    print(f"➡ Colonnes : {list(full_df.columns)}")


def download_zip_to_bytes(url: str, max_retries: int = 3) -> BytesIO:
    """
    Télécharge un ZIP en mémoire avec requests en mode streaming.
    - Tolère une connexion interrompue pendant la lecture (on garde les octets déjà reçus)
    - Vérifie que le contenu est bien un ZIP valide avant de le renvoyer
    """
    last_error = None

    for attempt in range(1, max_retries + 1):
        buf = BytesIO()

        try:
            print(f"⬇️ Tentative de téléchargement {attempt}/{max_retries} (requests) : {url}")
            resp = requests.get(url, stream=True, timeout=120)
            resp.raise_for_status()

            try:
                for chunk in resp.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        buf.write(chunk)
            except requests.exceptions.ChunkedEncodingError as e:
                # On garde ce qu'on a déjà dans buf et on teste quand même
                print(f"⚠️ Connexion interrompue (ChunkedEncodingError) : {e}")
                print("   → On garde les octets déjà reçus pour tester le ZIP.")

            data = buf.getvalue()
            if len(data) == 0:
                raise RuntimeError("Le téléchargement a renvoyé 0 octet.")

            # Vérification rapide de la signature ZIP (PK)
            if not data.startswith(b"PK"):
                raise RuntimeError("Les données reçues ne ressemblent pas à un fichier ZIP (signature invalide).")

            # Vérification plus poussée : on essaie d'ouvrir et de tester l'archive
            tmp = BytesIO(data)
            try:
                with zipfile.ZipFile(tmp) as z:
                    bad_file = z.testzip()
                    if bad_file is not None:
                        raise RuntimeError(f"Archive ZIP corrompue, fichier en erreur: {bad_file}")
            except zipfile.BadZipFile as e:
                raise RuntimeError(f"Archive ZIP invalide (BadZipFile) : {e}")

            print(f"✅ Téléchargement terminé. Taille : {len(data)} octets (ZIP valide)")
            tmp.seek(0)
            return tmp

        except Exception as e:
            print(f"❌ Erreur lors du téléchargement/validation du ZIP : {e}")
            last_error = e

            if attempt < max_retries:
                sleep_time = 2 * attempt
                print(f"⏳ Nouvelle tentative dans {sleep_time} secondes...")
                time.sleep(sleep_time)
            else:
                print("❌ Échec après plusieurs tentatives de téléchargement.")

    # Si on sort de la boucle sans succès :
    raise last_error if last_error else RuntimeError("Échec de téléchargement inconnu.")


def download_and_update_acteur_folder():
    """
    1) Télécharge le zip depuis ZIP_URL
    2) Parcourt les fichiers .csv dans le dossier 'acteur/' du zip
    3) Copie uniquement ceux qui n'existent pas déjà dans LOCAL_ACTEUR_FOLDER
    4) Relance build_deputes_from_acteurs sur le dossier LOCAL_ACTEUR_FOLDER

    Si le téléchargement échoue complètement, on reconstruit quand même acteurs.csv
    à partir des fichiers déjà présents.
    """
    LOCAL_ACTEUR_FOLDER.mkdir(parents=True, exist_ok=True)

    # Liste des CSV déjà présents localement
    existing_csv = {p.name for p in LOCAL_ACTEUR_FOLDER.glob("*.csv")}
    print(f"📁 Dossier local acteur : {LOCAL_ACTEUR_FOLDER.resolve()}")
    print(f"   {len(existing_csv)} fichiers CSV déjà présents.\n")

    # 1) Téléchargement du ZIP en mémoire (robuste)
    try:
        zip_bytes = download_zip_to_bytes(ZIP_URL)
    except Exception as e:
        print("\n⚠️ Impossible de télécharger le ZIP acteurs après plusieurs tentatives.")
        print("   Raison :", e)
        if existing_csv:
            print("   → On reconstruit seulement acteurs.csv avec les fichiers déjà présents.")
            build_deputes_from_acteurs(LOCAL_ACTEUR_FOLDER, output_filename="acteurs.csv")
            return
        else:
            print("   → Aucun fichier acteur local, on ne peut pas continuer.")
            raise

    # 2) Ouverture du zip en mémoire
    new_files_count = 0
    with zipfile.ZipFile(zip_bytes) as z:
        # On parcourt tous les fichiers du zip
        for member in z.infolist():
            path = Path(member.filename)

            # On ne garde que les CSV dans un dossier 'acteur'
            if path.suffix.lower() != ".csv":
                continue
            if "acteur" not in path.parts:
                continue

            target_name = path.name  # ex: 'PA123456.csv'

            if target_name in existing_csv:
                print(f"   ⚠️ {target_name} déjà présent localement, on ignore.")
                continue

            # Extraire le fichier dans le dossier local
            target_path = LOCAL_ACTEUR_FOLDER / target_name
            print(f"   ➕ Nouveau fichier CSV trouvé : {target_name}")

            with z.open(member, "r") as src, open(target_path, "wb") as dst:
                dst.write(src.read())

            new_files_count += 1

    print(f"\n📊 Nombre de nouveaux fichiers CSV ajoutés : {new_files_count}")

    # 3) Relancer la construction du CSV fusionné
    print("\n🔁 Reconstruction de acteurs.csv à partir de tous les fichiers du dossier acteur...")
    build_deputes_from_acteurs(LOCAL_ACTEUR_FOLDER, output_filename="acteurs.csv")


if __name__ == "__main__":
    download_and_update_acteur_folder()
