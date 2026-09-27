package com.cdp.lakehouse.cdc;

import java.math.BigInteger;

/**
 * The one definition of "newer" for a captured change (spec 01, section 4.1).
 *
 * <pre>
 *   cdc_version = epoch * 10^27 + phase * 10^26 + position * 10^6 + row      -- fits Snowflake NUMBER(38,0)
 * </pre>
 *
 * <ul>
 *   <li><b>epoch</b>: the timeline a position belongs to. Stamped at capture and bumped before every snapshot and
 *       at every topology change (MySQL failover, lost replication slot), so a newer epoch always wins.
 *   <li><b>phase</b>: 0 for a snapshot read, 1 for a live change. Within an epoch a snapshot row loses to any live
 *       change: the epoch's live changes are a complete, ordered tail of the log, so the highest one is the truth.
 *       This is what makes a Postgres transaction that was still open during the snapshot win, even though its
 *       LSN is below the snapshot point.
 *   <li><b>position</b>: where the change sits in the source log. Postgres: the LSN, which orders two writes to the
 *       same row because row locks stop them overlapping. MySQL: binlog file number * 10^10 + position.
 *   <li><b>row</b>: the row inside one MySQL binlog event.
 * </ul>
 *
 * <p>Every part is range-checked so that no part can spill into the next. Bad input throws
 * {@link IllegalArgumentException}; the caller routes the change to the dead-letter topic.
 */
public final class CdcVersion {

    private static final BigInteger EPOCH_SCALE = BigInteger.TEN.pow(27);
    private static final BigInteger PHASE_SCALE = BigInteger.TEN.pow(26);
    private static final BigInteger POSITION_SCALE = BigInteger.TEN.pow(6);
    private static final BigInteger BINLOG_FILE_SCALE = BigInteger.TEN.pow(10);

    static final long MAX_EPOCH = 99_999_999_999L;                      // 11 digits + 27 = 38
    static final BigInteger MAX_POSITION = BigInteger.TEN.pow(20).subtract(BigInteger.ONE);
    static final long MAX_ROW = 999_999L;
    static final long MAX_BINLOG_POS = 9_999_999_999L;                  // binlog files are at most 1 GiB
    static final long MAX_BINLOG_FILE = 999_999_999L;                   // keeps file * 10^10 + pos below 10^20

    private CdcVersion() {}

    /** A snapshot read (Debezium op 'r'). It has no log position. */
    public static BigInteger snapshot(long epoch) {
        return encode(epoch, 0, BigInteger.ZERO, 0);
    }

    /** A live Postgres change. The LSN is an unsigned 64-bit value. */
    public static BigInteger postgres(long epoch, long lsn) {
        return encode(epoch, 1, new BigInteger(Long.toUnsignedString(lsn)), 0);
    }

    /** A live MySQL change, e.g. file "mysql-bin.000123", position 4711, row 2 of that event. */
    public static BigInteger mysql(long epoch, String binlogFile, long binlogPos, long row) {
        long file = binlogFileNumber(binlogFile);
        if (file > MAX_BINLOG_FILE || binlogPos < 0 || binlogPos > MAX_BINLOG_POS) {
            throw new IllegalArgumentException("binlog coordinates out of range: " + binlogFile + ":" + binlogPos);
        }
        BigInteger position = BigInteger.valueOf(file).multiply(BINLOG_FILE_SCALE).add(BigInteger.valueOf(binlogPos));
        return encode(epoch, 1, position, row);
    }

    static long binlogFileNumber(String binlogFile) {
        int dot = binlogFile == null ? -1 : binlogFile.lastIndexOf('.');
        if (dot < 0 || dot == binlogFile.length() - 1) {
            throw new IllegalArgumentException("not a binlog file name: " + binlogFile);
        }
        return Long.parseLong(binlogFile.substring(dot + 1));      // NumberFormatException is an IllegalArgumentException
    }

    static BigInteger encode(long epoch, int phase, BigInteger position, long row) {
        if (epoch < 1 || epoch > MAX_EPOCH) {
            throw new IllegalArgumentException("epoch out of range: " + epoch);
        }
        if (phase != 0 && phase != 1) {
            throw new IllegalArgumentException("phase must be 0 or 1: " + phase);
        }
        if (position.signum() < 0 || position.compareTo(MAX_POSITION) > 0) {
            throw new IllegalArgumentException("position out of range: " + position);
        }
        if (row < 0 || row > MAX_ROW) {
            throw new IllegalArgumentException("row out of range: " + row);
        }
        return BigInteger.valueOf(epoch).multiply(EPOCH_SCALE)
                .add(BigInteger.valueOf(phase).multiply(PHASE_SCALE))
                .add(position.multiply(POSITION_SCALE))
                .add(BigInteger.valueOf(row));
    }
}
