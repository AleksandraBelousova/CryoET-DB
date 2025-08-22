import os
import sys
import logging
import time
from pathlib import Path

import hvac
import pandas as pd
from sqlalchemy import create_engine, text, exc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
VAULT_ADDR = os.getenv("VAULT_ADDR", "http://vault:8200")
VAULT_TOKEN = os.getenv("VAULT_TOKEN", "root")
SECRET_PATH = 'cryoet'

def get_db_credentials_from_vault(client):
    try:
        secret_response = client.secrets.kv.v2.read_secret_version(path=SECRET_PATH)
        return secret_response['data']['data']
    except hvac.exceptions.InvalidPath:
        logging.warning("Secrets not found in Vault. This appears to be the first run.")
        logging.info("Attempting to write secrets to Vault...")
        pg_user = os.getenv("POSTGRES_USER")
        pg_password = os.getenv("POSTGRES_PASSWORD")
        pg_db = os.getenv("POSTGRES_DB")

        if not all([pg_user, pg_password, pg_db]):
            logging.error("FATAL: On first run, POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB must be set for Vault initialization.")
            sys.exit(1)
            
        try:
            client.secrets.kv.v2.create_or_update_secret(
                path=SECRET_PATH,
                secret=dict(
                    POSTGRES_USER=pg_user,
                    POSTGRES_PASSWORD=pg_password,
                    POSTGRES_DB=pg_db,
                ),
            )
            logging.info("Secrets successfully written to Vault. Please re-run the command.")
            sys.exit(0)
        except Exception as write_e:
            logging.error(f"FATAL: Could not write secrets to Vault: {write_e}")
            sys.exit(1)

def main():
    logging.info("Starting ETL pipeline...")
    try:
        vault_client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)
        for i in range(5):
            if vault_client.sys.read_health_status().ok:
                break
            logging.info(f"Waiting for Vault to be ready... ({i+1}/5)")
            time.sleep(2)
        else:
            raise Exception("Vault is not available.")

        db_creds = get_db_credentials_from_vault(vault_client)
        DB_USER = db_creds['POSTGRES_USER']
        DB_PASSWORD = db_creds['POSTGRES_PASSWORD']
        DB_NAME = db_creds['POSTGRES_DB']
        DB_HOST = os.getenv("DB_HOST", "db")
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"

        engine = create_engine(DATABASE_URL)
        with engine.begin() as connection:
            logging.info("Applying database schema...")
            SQL_SCHEMA_PATH = Path("/app/sql/schema.sql")
            connection.execute(text(SQL_SCHEMA_PATH.read_text()))

            LABELS_CSV_PATH = Path("/app/data/labels.csv")
            if not LABELS_CSV_PATH.exists():
                logging.error(f"Source data file not found: {LABELS_CSV_PATH}. Aborting.")
                return
            
            df_labels = pd.read_csv(LABELS_CSV_PATH).rename(columns={'tomo_id': 'tomo_name'})

            unique_tomos = pd.DataFrame(df_labels['tomo_name'].unique(), columns=['tomo_name'])
            unique_tomos['raw_volume_path'] = unique_tomos['tomo_name'].apply(lambda n: f"volumes/{n}.mrc")
            
            unique_tomos.to_sql('temp_tomograms', connection, if_exists='replace', index=False)
            insert_sql = text("""
                INSERT INTO tomograms (tomo_name, raw_volume_path)
                SELECT tomo_name, raw_volume_path FROM temp_tomograms
                ON CONFLICT (tomo_name) DO NOTHING;
            """)
            connection.execute(insert_sql)
            logging.info("Tomograms table synchronized.")

            tomo_map_df = pd.read_sql("SELECT tomo_id, tomo_name FROM tomograms", connection)
            df_labels['tomo_id'] = df_labels['tomo_name'].map(dict(zip(tomo_map_df['tomo_name'], tomo_map_df['tomo_id'])))

            df_annotations = df_labels[['tomo_id', 'x', 'y', 'z']].rename(columns={
                'x': 'coord_x', 'y': 'coord_y', 'z': 'coord_z'
            })

            logging.info("Clearing old annotations...")
            connection.execute(text("TRUNCATE TABLE annotations RESTART IDENTITY CASCADE;"))
            
            logging.info(f"Loading {len(df_annotations)} new annotations...")
            df_annotations.to_sql('annotations', connection, if_exists='append', index=False, chunksize=10000)
            
        logging.info("ETL pipeline completed successfully.")

    except exc.OperationalError as e:
        logging.error(f"Database connection failed: {e}. Ensure the 'db' container is running and healthy.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred during the ETL process: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()