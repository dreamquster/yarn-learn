package org.dknight.app;

/**
 * Created by fanming.chen on 2016/9/5 0005.
 */
public final class ByteUtils {
    private ByteUtils() {}

    public static byte[] toBytes(Integer value) {
        if (null == value) {
            return null;
        }

        return new byte[] {
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                value.byteValue()
        };
    }
}
