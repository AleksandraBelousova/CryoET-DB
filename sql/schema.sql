CREATE TABLE IF NOT EXISTS tomograms (
    tomo_id             SERIAL PRIMARY KEY,
    tomo_name           VARCHAR(255) UNIQUE NOT NULL,
    raw_volume_path     TEXT NOT NULL,
    dataset_id          INTEGER,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tomo_name ON tomograms (tomo_name);
CREATE TABLE IF NOT EXISTS annotations (
    annotation_id       SERIAL PRIMARY KEY,
    tomo_id             INTEGER NOT NULL,
    coord_x             DOUBLE PRECISION NOT NULL,
    coord_y             DOUBLE PRECISION NOT NULL,
    coord_z             DOUBLE PRECISION NOT NULL,

    CONSTRAINT fk_tomogram
        FOREIGN KEY(tomo_id)
        REFERENCES tomograms(tomo_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_annotations_tomo_id ON annotations (tomo_id);