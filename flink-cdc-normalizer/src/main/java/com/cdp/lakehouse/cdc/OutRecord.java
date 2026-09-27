package com.cdp.lakehouse.cdc;

/** A record for the single exactly-once sink: clean, DLQ and HOLD records all travel as this. */
public class OutRecord {
    public String topic;
    public byte[] key;
    public byte[] value;
    /** Set only for DLQ and HOLD records: why the change was not applied, and where it sits in the raw topic. */
    public String rejectReason;
    public String rawPosition;

    public OutRecord() {}

    public OutRecord(String topic, byte[] key, byte[] value, String rejectReason, String rawPosition) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.rejectReason = rejectReason;
        this.rawPosition = rawPosition;
    }
}
