import os
import boto3
from dotenv import load_dotenv
import warnings

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
ENDPOINT_URL = "https://s3.fr-par.scw.cloud"
ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")

s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    verify=False
)

# Liste des fichiers à déplacer
files_to_move = [
    "pipeline_2025-11-17_18-59.log",
    "pipeline_2025-11-19_10-38.log",
    "pipeline_2025-11-20_18-41.log",
    "pipeline_2025-11-21_07-50.log",
    "pipeline_2025-11-22_02-45.log",
    "pipeline_2025-11-23_03-09.log",
    "pipeline_2025-11-24_03-05.log",
    "pipeline_2025-11-25_02-55.log",
    "pipeline_2025-11-26_02-55.log",
    "pipeline_2025-11-27_02-52.log",
    "pipeline_2025-11-28_02-54.log",
    "pipeline_2025-11-29_02-53.log",
    "pipeline_2025-11-30_03-12.log",
    "pipeline_2025-12-01_03-23.log"
]

print("🔍 Recherche des fichiers logs dans le bucket...\n")

try:
    # Lister les objets avec le bon préfixe
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="pdfs-assemblee-nationale/logs/")
    
    # Créer un dictionnaire des fichiers trouvés
    found_files = {}
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            filename = os.path.basename(key)
            if filename in files_to_move:
                found_files[filename] = key
                print(f"📍 Trouvé: {key}")
    
    print(f"\n📊 {len(found_files)}/{len(files_to_move)} fichiers trouvés\n")
    
    if len(found_files) == 0:
        print("⚠️ Aucun fichier à déplacer trouvé")
        exit(0)
    
    print("="*50)
    print("🚀 Démarrage du déplacement...\n")
    
    moved_count = 0
    error_count = 0
    not_found_count = 0
    
    for filename in files_to_move:
        if filename not in found_files:
            not_found_count += 1
            continue
        
        old_key = found_files[filename]
        new_key = f"pdfs-assemblee-nationale/logs/pipeline_scraping_pdf_main/{filename}"
        
        try:
            # Copier le fichier vers le nouveau dossier
            s3.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={'Bucket': BUCKET_NAME, 'Key': old_key},
                Key=new_key
            )
            
            # Supprimer l'ancien fichier
            s3.delete_object(Bucket=BUCKET_NAME, Key=old_key)
            
            print(f"✅ {filename}")
            moved_count += 1
            
        except Exception as e:
            print(f"❌ {filename} — Erreur: {e}")
            error_count += 1
    
    print("\n" + "="*50)
    print("📊 RÉSUMÉ")
    print("="*50)
    print(f"✅ Fichiers déplacés : {moved_count}")
    print(f"❌ Erreurs : {error_count}")
    print(f"⚠️ Non trouvés : {not_found_count}")
    print("="*50)
    print("\n🎉 Terminé!")

except Exception as e:
    print(f"❌ ERREUR CRITIQUE: {e}")