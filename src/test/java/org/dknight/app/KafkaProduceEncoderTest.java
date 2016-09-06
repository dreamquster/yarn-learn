package org.dknight.app;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.dknight.app.kafka.KafkaProduceEncoder;
import org.dknight.app.kafka.request.KMessage;
import org.dknight.app.kafka.request.KafkaProduceRequest;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

/**
 * Created by fanming.chen on 2016/9/5 0005.
 */
public class KafkaProduceEncoderTest {

    @Test
    public void encodeTest() throws UnsupportedEncodingException {
        KafkaProduceRequest produceRequest = new KafkaProduceRequest();
        produceRequest.addMessage(new KMessage(UUID.randomUUID().toString(), "hello world"));
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new KafkaProduceEncoder());
        embeddedChannel.writeOutbound(produceRequest);

        ByteBuf byteBuf = (ByteBuf) embeddedChannel.readOutbound();

    }
}
