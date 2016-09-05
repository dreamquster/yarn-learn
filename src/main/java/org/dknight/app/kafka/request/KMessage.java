package org.dknight.app.kafka.request;

import io.netty.buffer.ByteBuf;
import org.dknight.app.kafka.KafkaConst;

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


    public int sizeInBytes() {
        return CRC_MAGIC_ATTR_BYTES + KafkaConst.BYTEARRAY_LEN + key.length + KafkaConst.BYTEARRAY_LEN + value.length;
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




}
