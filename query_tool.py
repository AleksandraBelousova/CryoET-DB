import argparse
import os
import sys
import logging
import time
from pathlib import Path
import hvac
import matplotlib.pyplot as plt
import numpy as np
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
        logging.error("FATAL: Secrets not found in Vault. Please run the ETL script first to initialize them.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"FATAL: Could not retrieve secrets from Vault: {e}")
        sys.exit(1)
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
    
    ENGINE = create_engine(DATABASE_URL)

except Exception as e:
    print(f"Failed to establish database connection via Vault: {e}", file=sys.stderr)
    sys.exit(1)

APP_DIR = Path("/app")
DATA_DIR = APP_DIR / "data"
OUTPUT_DIR = APP_DIR / "output"

def count_annotations(args):
    query = text("SELECT COUNT(a.annotation_id) FROM annotations a JOIN tomograms t ON a.tomo_id = t.tomo_id WHERE t.tomo_name = :tomo_name;")
    with ENGINE.connect() as conn:
        result = conn.execute(query, {"tomo_name": args.tomo_name}).scalar_one_or_none()
    print(f"Tomogram '{args.tomo_name}' has {result or 0} annotations.")

def find_rich_tomograms(args):
    query = text("""
        SELECT t.tomo_name, COUNT(a.annotation_id) as annotation_count
        FROM tomograms t JOIN annotations a ON t.tomo_id = a.tomo_id
        GROUP BY t.tomo_name HAVING COUNT(a.annotation_id) > :min_annotations
        ORDER BY annotation_count DESC;
    """)
    df = pd.read_sql(query, ENGINE, params={"min_annotations": args.min_annotations})
    print(df.to_string(index=False) if not df.empty else f"No tomograms found with > {args.min_annotations} annotations.")

def visualize_annotation(args):
    query = text("SELECT t.raw_volume_path, a.coord_x, a.coord_y, a.coord_z FROM annotations a JOIN tomograms t ON a.tomo_id = t.tomo_id WHERE a.annotation_id = :aid;")
    with ENGINE.connect() as conn:
        result = conn.execute(query, {"aid": args.annotation_id}).first()

    if not result:
        print(f"Error: Annotation with ID {args.annotation_id} not found.", file=sys.stderr)
        return

    volume_path, x, y, z = result
    volume_path_npy = DATA_DIR / Path(volume_path).with_suffix('.npy')

    try:
        volume = np.load(volume_path_npy)
        z_slice = int(round(z))
        
        if not (0 <= z_slice < volume.shape[0]):
            raise IndexError(f"Z-coordinate {z_slice} is out of bounds for volume shape {volume.shape}")

        slice_2d = volume[z_slice, :, :]
        
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(10, 10))
        ax.imshow(slice_2d.T, cmap='gray', origin='lower')
        ax.scatter(x, y, s=120, facecolors='none', edgecolors='cyan', linewidths=1.5)
        ax.set_title(f"Annotation ID: {args.annotation_id} on Tomogram Slice Z={z_slice}")
        ax.axis('off')

        OUTPUT_DIR.mkdir(exist_ok=True)
        output_path = OUTPUT_DIR / f"annotation_{args.annotation_id}.png"
        plt.savefig(output_path, bbox_inches='tight', pad_inches=0.1, dpi=150)
        print(f"Image saved to: {output_path}")

    except FileNotFoundError:
        print(f"Error: Volume file not found at '{volume_path_npy}'", file=sys.stderr)
    except Exception as e:
        print(f"An error occurred during visualization: {e}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="CryoET-DB Query and Analysis Tool.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_count = subparsers.add_parser("count-annotations", help="Count annotations for a tomogram.")
    p_count.add_argument("--tomo-name", required=True)
    p_count.set_defaults(func=count_annotations)

    p_find = subparsers.add_parser("find-rich-tomograms", help="Find tomograms with N+ annotations.")
    p_find.add_argument("--min-annotations", type=int, default=20)
    p_find.set_defaults(func=find_rich_tomograms)

    p_viz = subparsers.add_parser("visualize", help="Visualize an annotation.")
    p_viz.add_argument("--annotation-id", type=int, required=True)
    p_viz.set_defaults(func=visualize_annotation)
    
    args = parser.parse_args()
    
    try:
        args.func(args)
    except exc.OperationalError as e:
        print(f"Database connection failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"A critical error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()