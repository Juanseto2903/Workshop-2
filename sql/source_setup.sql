-- Tabla fuente operacional de Grammy (preparación de la fuente, NO es la carga al DW)
DROP TABLE IF EXISTS grammy_awards;

CREATE TABLE grammy_awards (
    grammy_id    SERIAL PRIMARY KEY,
    year         INTEGER,
    title        TEXT,
    published_at TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    category     TEXT,
    nominee      TEXT,
    artist       TEXT,
    workers      TEXT,
    img          TEXT,
    winner       BOOLEAN
);