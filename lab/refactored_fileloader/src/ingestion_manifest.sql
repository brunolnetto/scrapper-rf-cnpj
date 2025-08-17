-- Manifest table for processed files
CREATE TABLE IF NOT EXISTS ingestion_manifest (
  filename TEXT PRIMARY KEY,
  checksum BYTEA,
  filesize BIGINT,
  processed_at TIMESTAMP WITH TIME ZONE,
  rows BIGINT,
  status TEXT,
  run_id TEXT,
  notes TEXT
);
