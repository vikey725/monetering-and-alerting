-- Group records arrive from a compacted, multi-partition topic in arbitrary order (children before parents).
-- The mirror is eventually consistent; the upstream registry owns referential integrity.
ALTER TABLE station_groups DROP CONSTRAINT IF EXISTS station_groups_parent_id_fkey;
