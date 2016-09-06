package org.dknight.app.kafka.request;

import io.netty.buffer.ByteBuf;
import org.dknight.app.kafka.KafkaConst;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by fanming.chen on 2016/9/5 0005.
 */
public class MessageSet {

    public static final int OFFSET_SIZE_LEN = 12;

    private long offset = KafkaConst.NO_OFFSET; //8byte

    private int messageSize; // 4byte

    private List<KMessage> messages = new LinkedList<KMessage>();

    public void addMessage(KMessage kMessage) {
        messages.add(kMessage);
    }

    public int sizeInBytes() {
        return OFFSET_SIZE_LEN + getMessageSize();
    }

    public void serializeTo(ByteBuf byteBuf) {
        byteBuf.writeLong(offset);
        getMessageSize();
        byteBuf.writeInt(messageSize);
        for (KMessage kMessage: messages) {
            kMessage.serializeTo(byteBuf);
        }
    }

    private int getMessageSize() {
        messageSize = 0;
        for (KMessage kMessage : messages) {
            messageSize += kMessage.sizeInBytes();
        }
        return messageSize;
    }
}
