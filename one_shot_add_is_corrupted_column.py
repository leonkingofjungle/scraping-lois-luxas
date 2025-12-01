import polars as pl
import boto3
from dotenv import load_dotenv
import os
import warnings

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")

s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    verify=False
)

print("="*60)
print("🔧 AJOUT DE LA COLONNE 'is_corrupted' À LA DB")
print("="*60)

DB_CLOUD_PATH = "db_urls.parquet"
DB_LOCAL_TEMP = "db_urls_temp.parquet"

try:
    # 1. Télécharger la DB
    print("\n📥 Téléchargement de la DB depuis Scaleway...")
    s3.download_file(BUCKET_NAME, DB_CLOUD_PATH, DB_LOCAL_TEMP)
    print("✅ DB téléchargée")
    
    # 2. Lire le parquet
    df = pl.read_parquet(DB_LOCAL_TEMP)
    print(f"\n📊 DB actuelle:")
    print(f"   Lignes: {len(df)}")
    print(f"   Colonnes: {', '.join(df.columns)}")
    
    # 3. Ajouter ou réinitialiser la colonne is_corrupted
    if "is_corrupted" in df.columns:
        print("\n⚠️  La colonne 'is_corrupted' existe déjà")
        print("   → Réinitialisation à False pour toutes les lignes")
        df = df.with_columns(pl.lit(False).alias("is_corrupted"))
    else:
        print("\n✨ Ajout de la colonne 'is_corrupted' (valeur: False)")
        df = df.with_columns(pl.lit(False).alias("is_corrupted"))
    
    # 4. Sauvegarder localement
    df.write_parquet(DB_LOCAL_TEMP)
    print("💾 DB mise à jour localement")
    
    # 5. Upload sur Scaleway
    print("\n☁️  Upload de la DB vers Scaleway...")
    s3.upload_file(DB_LOCAL_TEMP, BUCKET_NAME, DB_CLOUD_PATH)
    print("✅ DB synchronisée sur le Cloud")
    
    # 6. Afficher un aperçu
    print("\n📋 Aperçu de la DB mise à jour:")
    print(df.head(3))
    
    # 7. Statistiques
    print(f"\n📊 STATISTIQUES:")
    print(f"   Total de lignes: {len(df)}")
    print(f"   Colonnes: {', '.join(df.columns)}")
    
    downloaded_count = df.filter(pl.col("downloaded") == True).height
    not_404_count = df.filter(pl.col("is_404") == False).height
    corrupted_count = df.filter(pl.col("is_corrupted") == True).height
    
    print(f"\n   Downloaded: {downloaded_count}/{len(df)}")
    print(f"   Non-404: {not_404_count}/{len(df)}")
    print(f"   Corrompus: {corrupted_count}/{len(df)}")
    
    # 8. Nettoyer
    os.remove(DB_LOCAL_TEMP)
    print("\n🗑️  Fichier temporaire supprimé")
    
except Exception as e:
    print(f"\n❌ ERREUR: {e}")
    if os.path.exists(DB_LOCAL_TEMP):
        os.remove(DB_LOCAL_TEMP)
        print("🗑️  Fichier temporaire supprimé")

print("\n" + "="*60)
print("🎉 OPÉRATION TERMINÉE")
print("="*60)
print("\n💡 Prochaine étape:")
print("   Lance 'python scraping_lois/verif_pdfs_db.py' pour vérifier les PDFs")
