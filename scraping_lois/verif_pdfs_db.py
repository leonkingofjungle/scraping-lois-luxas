import os
import polars as pl
import boto3
from dotenv import load_dotenv
from PyPDF2 import PdfReader
from botocore.exceptions import ClientError
from datetime import datetime
import io
import warnings
import tempfile
import shutil

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

load_dotenv()

TEMP_DIR = tempfile.gettempdir()
LOG_VERIF_DIR = os.path.join(TEMP_DIR, "verif_db_logs")

os.makedirs(LOG_VERIF_DIR, exist_ok=True)

DB_TEMP_PATH = os.path.join(TEMP_DIR, "db_urls.parquet.tmp")
DB_FILENAME = "db_urls.parquet"

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

logfile = os.path.join(LOG_VERIF_DIR, f"pdf_verification_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log")

def log(message: str):
    timestamp = datetime.now().isoformat()
    line = f"{timestamp} — {message}"
    print(line)
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    return line

def verify_pdf_readability(pdf_stream, pdf_name):
    """
    Vérifie si un PDF peut être ouvert et lu
    Retourne: (is_readable: bool, error_message: str)
    """
    try:
        pdf_reader = PdfReader(pdf_stream)
        num_pages = len(pdf_reader.pages)
        
        if num_pages > 0:
            _ = pdf_reader.pages[0].extract_text()
        
        return True, None
    except Exception as e:
        return False, str(e)

def check_all_pdfs_on_cloud():
    """
    Vérifie tous les PDFs sur Scaleway et met à jour is_corrupted
    """
    log("\n" + "="*50)
    log("🔍 VÉRIFICATION DES PDFs SUR SCALEWAY")
    log("="*50)
    
    corrupted_urls = []
    readable_count = 0
    total_checked = 0
    
    try:
        log("📥 Téléchargement de la DB...")
        s3.download_file(BUCKET_NAME, DB_FILENAME, DB_TEMP_PATH)
        df = pl.read_parquet(DB_TEMP_PATH)
    
        
        cloud_keys = (
            df.filter(pl.col("downloaded") == True)
              .select([
                  pl.concat_str([pl.lit("pdfs/"), pl.col("pdf_name")]).alias("cloud_key"),
                  pl.col("pdf_name"),
                  pl.col("url")
              ])
        )
        
        total_pdfs = len(cloud_keys)
        log(f"📊 {total_pdfs} PDFs à vérifier\n")
        
        for idx, row in enumerate(cloud_keys.iter_rows(named=True), 1):
            cloud_key = row["cloud_key"]
            pdf_name = row["pdf_name"]
            url = row["url"]
            
            try:
                pdf_obj = s3.get_object(Bucket=BUCKET_NAME, Key=cloud_key)
                pdf_stream = io.BytesIO(pdf_obj['Body'].read())
                
                is_readable, error_msg = verify_pdf_readability(pdf_stream, pdf_name)
                
                total_checked += 1
                
                if is_readable:
                    readable_count += 1
                    log(f"[{idx}/{total_pdfs}] ✅ {pdf_name}")
                else:
                    corrupted_urls.append(url)
                    log(f"[{idx}/{total_pdfs}] ❌ {pdf_name} — ERREUR: {error_msg}")
                
            except ClientError as e:
                log(f"[{idx}/{total_pdfs}] ⚠️ {pdf_name} — ERREUR CLOUD: {e}")
                corrupted_urls.append(url)
            except Exception as e:
                log(f"[{idx}/{total_pdfs}] ⚠️ {pdf_name} — ERREUR INCONNUE: {e}")
                corrupted_urls.append(url)
        
        log("\n📝 Mise à jour de la DB...")
        df = df.with_columns(
            pl.when(pl.col("url").is_in(corrupted_urls))
              .then(True)
              .otherwise(pl.col("is_corrupted"))
              .alias("is_corrupted")
        )
        
        corrupted_count = len(corrupted_urls)
        
        log("\n" + "="*50)
        log("📈 RÉSUMÉ DE LA VÉRIFICATION")
        log("="*50)
        log(f"Total vérifié : {total_checked}")
        log(f"✅ Lisibles : {readable_count}")
        log(f"❌ Corrompus/Illisibles : {corrupted_count}")
        
        if corrupted_urls:
            log("\n" + "="*50)
            log("⚠️ LISTE DES PDFs CORROMPUS/ILLISIBLES:")
            log("="*50)
            
            corrupted_df = df.filter(pl.col("url").is_in(corrupted_urls))
            for row in corrupted_df.iter_rows(named=True):
                log(f"  - {row['pdf_name']}")
                log(f"    URL: {row['url']}")
        else:
            log("\n🎉 Tous les PDFs sont lisibles!")
        
        df.write_parquet(DB_TEMP_PATH)
        log("\n💾 DB mise à jour localement")
        
        log("☁️ Upload de la DB vers Scaleway...")
        s3.upload_file(DB_TEMP_PATH, BUCKET_NAME, DB_FILENAME)
        log("✅ DB synchronisée sur le Cloud")
        
        log("\n☁️ Upload du log...")
        log_name = os.path.basename(logfile)
        s3.upload_file(logfile, BUCKET_NAME, f"pdfs-assemblee-nationale/logs/verif_db/{log_name}")
        log("✅ Log uploadé sur Scaleway")
        
        log("\n" + "="*50)
        log("🏁 VÉRIFICATION TERMINÉE")
        log("="*50)
        
    except Exception as e:
        log(f"\n❌ ERREUR CRITIQUE: {e}")
        import traceback
        log(traceback.format_exc())
    
    finally:
        try:
            if os.path.exists(DB_TEMP_PATH):
                os.remove(DB_TEMP_PATH)
                print("🗑️ Fichier DB temporaire supprimé")
            
            if os.path.exists(logfile):
                os.remove(logfile)
            
            if os.path.exists(LOG_VERIF_DIR):
                shutil.rmtree(LOG_VERIF_DIR)
            
            print("🗑️ Fichiers temporaires supprimés")
            
        except Exception as e:
            print(f"⚠️ Erreur nettoyage: {e}")

if __name__ == "__main__":
    check_all_pdfs_on_cloud()
