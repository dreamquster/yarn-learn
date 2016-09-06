package org.dknight.app.kafka;

/**
 * Created by fanming.chen on 2016/9/5 0005.
 */
public final class KafkaConst {

    public static final short PRODUCE_REQUEST = 0;

    public static final long NO_OFFSET = 0L;

    public static final byte MAGIC_BYTES = 1;

    public static final byte COMPRESS_NONE = 0;

    public static final byte COMPRESS_BITS = 3;

    public static final byte CREATE_TIME = 0;

    public static final String CHARSET_UTF8 = "UTF-8";

    public static final int INT_BYTES = 4;

    public static final int SERVER_ACK = 1;


    private KafkaConst() {}
}
