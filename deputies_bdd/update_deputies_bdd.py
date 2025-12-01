import sys

# === 1) Import des scripts existants ================================
# On importe seulement les fonctions "main" des deux scripts.
from sub_update_acteur_csv import download_and_update_acteur_folder
from  sub_update_liste_deputes_libre_office_csv import main as update_deputies_main


# === 2) Script orchestrateur ========================================
def main():
    print("\n==============================")
    print("   🚀 DÉMARRAGE UPDATE GLOBAL")
    print("==============================\n")

    try:
        # Étape 1 : Mise à jour des fichiers acteur + fusion en acteurs.csv
        print("\n👤 Étape 1/2 : Mise à jour des ACTEURS\n")
        download_and_update_acteur_folder()

    except Exception as e:
        print("\n❌ ERREUR dans update_acteur_csv.py :")
        print(e)
        sys.exit(1)

    try:
        # Étape 2 : Mise à jour du CSV des députés + génération deputies.csv
        print("\n🏛️ Étape 2/2 : Mise à jour des DÉPUTÉS\n")
        update_deputies_main()

    except Exception as e:
        print("\n❌ ERREUR dans update_deputies_csv.py :")
        print(e)
        sys.exit(1)

    print("\n===============================================")
    print("   🎉 Mise à jour terminée avec succès !")
    print("===============================================\n")


if __name__ == "__main__":
    main()
