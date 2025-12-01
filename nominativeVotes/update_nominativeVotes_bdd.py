import io
import zipfile
from pathlib import Path
import time
import subprocess
import tempfile
import os
import json
import csv

import requests

# ==========================
# 1) Paramètres généraux
# ==========================

SCRUTINS_ZIP_URL = (
    "http://data.assemblee-nationale.fr/static/openData/repository/17/loi/scrutins/Scrutins.json.zip"
)

# Dossier où sont (ou seront) stockés les fichiers JSON de scrutins
DOSSIER_JSON = "json"
# Nom du CSV de votes nominatifs produit à partir des JSON
NOM_FICHIER_CSV_NOMINATIF = "nominativeVotes.csv"

# Colonnes attendues dans le CSV des votes nominatifs
COLONNES_CSV_NOMINATIF = [
    "scrutin_uid",
    "numero_scrutin",
    "depute_id",
    "vote",
    "cause_non_vote",
]

# ==========================
# 2) Téléchargement du ZIP
#    (reprise de ton script)
# ==========================


def download_zip_with_requests(url: str, max_retries: int = 3) -> io.BytesIO:
    """
    Tentative de téléchargement du zip avec requests (simple).
    Si ça plante (erreurs réseau / IncompleteRead), on laisse l'appelant gérer.
    """
    for attempt in range(1, max_retries + 1):
        try:
            print(
                f"Téléchargement du zip avec requests (tentative {attempt}/{max_retries})..."
            )
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            buffer = io.BytesIO(resp.content)
            buffer.seek(0)
            print("✅ Téléchargement réussi avec requests.")
            return buffer
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur réseau avec requests : {e}")
            if attempt < max_retries:
                time.sleep(3)
            else:
                raise


def download_zip_with_curl(url: str) -> io.BytesIO:
    """
    Fallback robuste : utilise 'curl' en ligne de commande pour télécharger le zip
    (avec retries + reprise) dans un fichier temporaire, puis le charge en mémoire.
    """
    print("➡️  Passage au fallback avec curl...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_zip_path = Path(tmpdir) / "Scrutins.json.zip"

        cmd = [
            "curl",
            "-L",  # follow redirects
            "--retry",
            "10",  # jusqu'à 10 tentatives
            "--retry-delay",
            "5",  # 5 secondes entre tentatives
            "-C",
            "-",  # reprise si téléchargement partiel
            "-o",
            str(tmp_zip_path),
            url,
        ]

        print("Commande exécutée :", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print("❌ curl a échoué.")
            print("stderr curl :", result.stderr.strip())
            raise RuntimeError("curl n'a pas réussi à télécharger le fichier.")

        # On lit le zip téléchargé
        data = tmp_zip_path.read_bytes()
        buffer = io.BytesIO(data)
        buffer.seek(0)
        print("✅ Téléchargement réussi avec curl.")
        return buffer


def download_zip(url: str) -> io.BytesIO:
    """
    Wrapper : essaie d'abord requests, puis fallback curl si nécessaire.
    """
    try:
        return download_zip_with_requests(url)
    except Exception as e:
        print("⚠️  Impossible de récupérer le fichier correctement avec requests :", e)
        print("On tente maintenant avec curl.")
        return download_zip_with_curl(url)


def update_json_files():
    """
    Télécharge Scrutins.json.zip (avec une stratégie robuste),
    et ajoute dans le dossier 'json' seulement les fichiers .json
    qui n'existent pas encore.
    """
    base_dir = Path(__file__).resolve().parent
    json_dir = base_dir / DOSSIER_JSON
    json_dir.mkdir(exist_ok=True)

    existing_files = {p.name for p in json_dir.glob("*.json")}
    print(f"{len(existing_files)} fichiers JSON déjà présents dans '{json_dir}'.")

    try:
        zip_bytes = download_zip(SCRUTINS_ZIP_URL)
    except Exception as e:
        print("❌ Échec total du téléchargement du zip :", e)
        return

    new_files_count = 0

    try:
        with zipfile.ZipFile(zip_bytes) as zf:
            json_members = [
                info
                for info in zf.infolist()
                if info.filename.lower().endswith(".json")
            ]

            print(f"{len(json_members)} fichiers JSON trouvés dans le zip.")

            for member in json_members:
                filename = Path(member.filename).name

                if filename in existing_files:
                    continue

                target_path = json_dir / filename

                with zf.open(member) as src, open(target_path, "wb") as dst:
                    dst.write(src.read())

                new_files_count += 1
                print(f"➡️  Nouveau fichier ajouté : {filename}")

    except zipfile.BadZipFile:
        print(
            "❌ Erreur: le fichier téléchargé n'est pas un zip valide (peut-être toujours tronqué)."
        )
        return

    if new_files_count == 0:
        print("✅ Aucun nouveau fichier à ajouter, tout est déjà à jour.")
    else:
        print(
            f"✅ Terminé : {new_files_count} nouveaux fichiers JSON ajoutés dans '{json_dir}'."
        )


# ==========================
# 3) Traitement des JSON
#    -> CSV nominatif
# ==========================


def _as_list(x):
    """Utilitaire : transforme un dict ou un élément seul en liste."""
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def extraire_votes_nominatifs(chemin_fichier_json: Path):
    """
    Extrait, pour un fichier JSON de scrutin, les votes nominatifs sous la forme
    d'une liste de lignes (dictionnaires) correspondant à COLONNES_CSV_NOMINATIF.

    ⚠️ Le schéma des fichiers JSON de l'Assemblée nationale peut évoluer.
    Cette fonction essaie d'être robuste, mais n'hésite pas à l'ajuster
    si tu vois des champs un peu différents dans tes fichiers.
    """
    try:
        with open(chemin_fichier_json, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Erreur de lecture JSON pour {chemin_fichier_json.name} : {e}")
        return []

    # La plupart du temps, la racine utile est dans data["scrutin"]
    scrutin = data.get("scrutin", data)
    scrutin_uid = str(scrutin.get("uid", "")).strip()
    numero_scrutin = scrutin.get("numero") or scrutin.get("numeroScrutin") or ""
    if numero_scrutin is None:
        numero_scrutin = ""
    numero_scrutin = str(numero_scrutin).strip()

    lignes = []

    ventilation = scrutin.get("ventilationVotes") or scrutin.get("ventilation") or {}
    organes = _as_list(ventilation.get("organe"))

    # Structure typique : ventilationVotes -> organe -> groupes / nonInscrits
    for org in organes:
        # 1) Groupes
        groupes = org.get("groupes", {})
        groupes_list = _as_list(groupes.get("groupe"))
        for g in groupes_list:
            # Par groupe : un bloc "vote" qui contient "noms" -> "nom"
            vote_block = g.get("vote", {})
            noms_block = vote_block.get("noms", {})
            noms_list = _as_list(noms_block.get("nom"))

            for n in noms_list:
                # Id du député
                depute_id = (
                    n.get("acteurRef")
                    or n.get("acteurRef_depute")
                    or n.get("deputeRef")
                    or ""
                )
                depute_id = str(depute_id).strip()

                # Nature du vote (pour / contre / abstention / nonVotant...)
                vote_val = (
                    n.get("vote")
                    or n.get("sensVote")
                    or vote_block.get("codeTypeVote")
                    or ""
                )
                vote_val = str(vote_val).strip()

                # Cause de non vote éventuelle
                cause_non_vote = (
                    n.get("causeDeNonVote")
                    or n.get("causeNonVote")
                    or n.get("cause")
                    or ""
                )
                cause_non_vote = str(cause_non_vote).strip()

                if depute_id:
                    lignes.append(
                        {
                            "scrutin_uid": scrutin_uid,
                            "numero_scrutin": numero_scrutin,
                            "depute_id": depute_id,
                            "vote": vote_val,
                            "cause_non_vote": cause_non_vote,
                        }
                    )

        # 2) Non inscrits (souvent à la racine de l'organe)
        non_inscrits = org.get("nonInscrits", {})
        if non_inscrits:
            vote_block = non_inscrits.get("vote", non_inscrits)
            noms_block = vote_block.get("noms", vote_block.get("nom", {}))
            noms_list = _as_list(noms_block.get("nom", noms_block if isinstance(noms_block, list) else None))

            for n in noms_list:
                depute_id = (
                    n.get("acteurRef")
                    or n.get("acteurRef_depute")
                    or n.get("deputeRef")
                    or ""
                )
                depute_id = str(depute_id).strip()

                vote_val = (
                    n.get("vote")
                    or n.get("sensVote")
                    or vote_block.get("codeTypeVote")
                    or ""
                )
                vote_val = str(vote_val).strip()

                cause_non_vote = (
                    n.get("causeDeNonVote")
                    or n.get("causeNonVote")
                    or n.get("cause")
                    or ""
                )
                cause_non_vote = str(cause_non_vote).strip()

                if depute_id:
                    lignes.append(
                        {
                            "scrutin_uid": scrutin_uid,
                            "numero_scrutin": numero_scrutin,
                            "depute_id": depute_id,
                            "vote": vote_val,
                            "cause_non_vote": cause_non_vote,
                        }
                    )

    return lignes


def traiter_tous_les_fichiers_nominatifs():
    """
    Parcourt tous les fichiers JSON du dossier DOSSIER_JSON
    et produit un unique fichier CSV (NOM_FICHIER_CSV_NOMINATIF)
    avec toutes les lignes de votes nominatifs.
    """
    base_dir = Path(__file__).resolve().parent
    json_dir = base_dir / DOSSIER_JSON
    csv_path = base_dir / NOM_FICHIER_CSV_NOMINATIF

    if not json_dir.exists():
        print(f"⚠️ Le dossier JSON '{json_dir}' n'existe pas. Rien à traiter.")
        return

    fichiers_json = sorted(json_dir.glob("*.json"))
    if not fichiers_json:
        print(f"⚠️ Aucun fichier JSON trouvé dans '{json_dir}'.")
        return

    print(f"\n➡️ Traitement des votes nominatifs dans {len(fichiers_json)} fichiers JSON...")

    tous_les_votes = []
    fichiers_traites = 0
    fichiers_erreurs = 0

    for fichier in fichiers_json:
        try:
            lignes = extraire_votes_nominatifs(fichier)
            tous_les_votes.extend(lignes)
            fichiers_traites += 1
        except Exception as e:
            print(f"❌ Erreur lors du traitement de {fichier.name} : {e}")
            fichiers_erreurs += 1

    if tous_les_votes:
        try:
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COLONNES_CSV_NOMINATIF)
                writer.writeheader()
                writer.writerows(tous_les_votes)

            print(
                f"\n✅ Succès : le fichier '{csv_path.name}' a été créé avec {len(tous_les_votes)} lignes."
            )

        except Exception as e:
            print(f"❌ Erreur lors de l'écriture du fichier CSV : {e}")
    else:
        print("\n⚠️ Aucun vote nominatif n'a été trouvé dans les fichiers JSON.")

    print("\nStatistiques :")
    print(f"- Fichiers traités avec succès : {fichiers_traites}")
    print(f"- Fichiers avec erreurs/manquants : {fichiers_erreurs}")


# ==========================
# 4) Point d'entrée unique
# ==========================

if __name__ == "__main__":
    # 1) On met à jour / télécharge les JSON
    update_json_files()

    # 2) Puis on (re)génère le CSV des votes nominatifs à partir de ces JSON
    traiter_tous_les_fichiers_nominatifs()
