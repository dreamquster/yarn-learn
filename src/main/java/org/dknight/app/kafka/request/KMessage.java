package org.dknight.app.kafka.request;

import io.netty.buffer.ByteBuf;
import org.dknight.app.ByteUtils;
import org.dknight.app.kafka.KafkaConst;

import java.io.UnsupportedEncodingException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Created by fanming.chen on 2016/9/5 0005.
 */
public class KMessage {

    private static final int CRC_MAGIC_ATTR_BYTES = 4 + 1 + 1;

    private int crc;

    private byte magicByte = KafkaConst.MAGIC_BYTES;

    private byte attributes = KafkaConst.COMPRESS_NONE & (KafkaConst.CREATE_TIME<<KafkaConst.COMPRESS_BITS);

    private byte[] key;

    private byte[] value;

    private Checksum checksum = new CRC32();

    public KMessage(String key, String value) throws UnsupportedEncodingException {
        this.key = key.getBytes(KafkaConst.CHARSET_UTF8);
        this.value = value.getBytes(KafkaConst.CHARSET_UTF8);
    }

    public int sizeInBytes() {
        return CRC_MAGIC_ATTR_BYTES + KafkaConst.INT_BYTES + key.length + KafkaConst.INT_BYTES + value.length;
    }

    public void serializeTo(ByteBuf byteBuf) {
        byteBuf.writeInt(crc);

        byteBuf.writeByte(magicByte);
        byteBuf.writeByte(attributes);
        byteBuf.writeInt(key.length);
        byteBuf.writeBytes(key);
        byteBuf.writeInt(value.length);
        byteBuf.writeBytes(value);
    }

    private int calculateCRC() {
        checksum.reset();
        checksum.update(magicByte);
        checksum.update(attributes);
        checksum.update(ByteUtils.toBytes(key.length), 0, KafkaConst.INT_BYTES);
        checksum.update(key, 0, key.length);
        checksum.update(ByteUtils.toBytes(value.length), 0, KafkaConst.INT_BYTES);
        checksum.update(value, 0, value.length);
        return (int)checksum.getValue();
    }

}
