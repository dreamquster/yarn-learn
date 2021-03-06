package org.dknight.app.kafka.request;

import io.netty.buffer.ByteBuf;
import org.dknight.app.kafka.KafkaConst;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

/**
 * Created by fanming.chen on 2016/9/5 0005.
 */
public class KafkaProduceRequest {

    private static final int REQUEST_HEADER_LEN = 4 + 2 + 2 + 4;

    private int size; // 4byte

    private short apiKey = KafkaConst.PRODUCE_REQUEST; // 2byte

    private short apiVersion = 0; //2byte

    private int correlationId; //4byte

    private String clientId;

    private static final int PRODUCE_HEADER_LEN = 2 + 4 + 4 + 4;

    private short requiredAcks = KafkaConst.SERVER_ACK;

    private int timeout = 5000;

    private int partition;

    private int messageSetSize;

    private MessageSet messageSet = new MessageSet();

    public void addMessage(KMessage kMessage) {
        messageSet.addMessage(kMessage);
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void serializeTo(ByteBuf byteBuf) throws UnsupportedEncodingException {
        size = REQUEST_HEADER_LEN + PRODUCE_HEADER_LEN + messageSet.sizeInBytes();
        if (null != clientId) {
            size += clientId.getBytes(KafkaConst.CHARSET_UTF8).length;
        }

        byteBuf.writeInt(size);
        byteBuf.writeShort(apiKey);
        byteBuf.writeShort(apiVersion);
        byteBuf.writeInt(correlationId);
        if (null != clientId) {
            byteBuf.writeInt(clientId.getBytes(KafkaConst.CHARSET_UTF8).length);
            byteBuf.writeBytes(clientId.getBytes(KafkaConst.CHARSET_UTF8));
        } else {
            byteBuf.writeInt(-1); // indicate null string
        }

        messageSet.serializeTo(byteBuf);
    }
}
