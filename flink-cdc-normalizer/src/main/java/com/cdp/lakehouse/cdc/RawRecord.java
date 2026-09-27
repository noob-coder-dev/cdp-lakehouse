package com.cdp.lakehouse.cdc;

/** One record from a raw CDC topic, still undecoded. A Flink POJO: public fields and a no-arg constructor. */
public class RawRecord {
    public String topic;
    public int partition;
    public long offset;
    public byte[] key;
    public byte[] value;

    public RawRecord() {}

    public RawRecord(String topic, int partition, long offset, byte[] key, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }
}
