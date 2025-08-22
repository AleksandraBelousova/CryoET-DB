
CryoET-DB is a production-ready, secure system for ingesting, storing, and analysing cryo-electron tomography annotation data. The architecture is fully containerised with Docker, ensuring portability and reproducibility. It leverages PostgreSQL for structured data storage, a Python/Pandas ETL pipeline for robust data ingestion, and HashiCorp Vault for enterprise-grade secrets management.

## Core Architecture

-   **Database**: PostgreSQL 14 running in a dedicated, network-isolated Docker container. The database port is **not** exposed to the host, ensuring access is strictly limited to the internal Docker network.
-   **ETL & Query Tool**: A Python application, running in its own Docker container, which communicates with the database and Vault over the internal network.
-   **Secrets Management**: HashiCorp Vault runs as a third container, acting as the single source of truth for database credentials. The Python application fetches these credentials from Vault at runtime.

## Quick Start

### 1. Prerequisites

-   Docker & Docker Compose
-   Git

### 2. Setup

1.  **Clone the Repository**:
    ```bash
    git clone <your-repo-url>
    cd CryoET-DB
    ```

2.  **Prepare Data**:
    Manually download `labels.csv` and at least one `.npy` volume file (e.g., `aba2015-06-04-15.npy`) from the [Kaggle Dataset](https://www.kaggle.com/datasets/brendanartley/cryoet-flagellar-motors-dataset/data). Arrange them in the correct structure:
    ```
    data/
    ├── labels.csv
    └── volumes/
        └── aba2015-06-04-15.npy
    ```

3.  **Configure Environment**:
    Set the following environment variables in your shell. They are required to initialise the database container and for the application's first-run Vault initialisation.

    *For Linux/macOS:*
    ```bash
    export POSTGRES_USER="user"
    export POSTGRES_PASSWORD="password"
    export POSTGRES_DB="cryoetdb"
    ```
    *For PowerShell:*
    ```powershell
    $env:POSTGRES_USER="user"
    $env:POSTGRES_PASSWORD="password"
    $env:POSTGRES_DB="cryoetdb"
    ```

### 3. Execution

1.  **Launch All Services**:
    This command builds the Python application image and starts all three containers (`db`, `vault`, `app`).
    ```bash
    docker-compose up -d --build
    ```

2.  **Run ETL (Two-Step Initialisation)**:
    The first run will automatically initialise Vault with your secrets. The second run will perform the data ingestion.
    ```bash
    # First run (writes secrets to Vault)
    docker-compose run --rm app python etl/load_data.py
    # Second run (ingests data)
    docker-compose run --rm app python etl/load_data.py
    ```

3.  **Use the Query Tool**:
    Execute commands via `docker-compose run`. The `app` container will fetch credentials and query the `db`.

    -   **Count annotations**:
        ```bash
        docker-compose run --rm app python query_tool.py count-annotations --tomo-name "aba2015-06-04-15"
        ```
    -   **Find "rich" tomograms**:
        ```bash
        docker-compose run --rm app python query_tool.py find-rich-tomograms --min-annotations 20
        ```
    -   **Visualise an annotation**:
        (Find a valid ID via `docker exec -it cryoet-db-db-1 psql -U user -d cryoetdb`)
        ```bash
        docker-compose run --rm app python query_tool.py visualize --annotation-id 269
        ```
    The output image will be saved to your local `output/` directory.
