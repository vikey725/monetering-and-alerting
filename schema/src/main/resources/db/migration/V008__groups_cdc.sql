-- station_groups is now the source of truth for the group hierarchy; Debezium streams it to the `groups` topic.
-- Debezium needs full row images for deletes / compaction keys.
ALTER TABLE station_groups REPLICA IDENTITY FULL;
