package com.chargemon.flink.model;

/** A station entered or left a group (including ancestors). Feeds group-level aggregations. */
public record GroupMemberDelta(String groupId, String stationId, boolean added) {
}
