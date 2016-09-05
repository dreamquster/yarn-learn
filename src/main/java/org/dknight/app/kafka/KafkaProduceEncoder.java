package org.dknight.app.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.dknight.app.kafka.request.KafkaProduceRequest;

/**
 * Created by fanming.chen on 2016/9/5 0005.
 */
public class KafkaProduceEncoder extends MessageToByteEncoder<KafkaProduceRequest> {
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, KafkaProduceRequest produceRequest, ByteBuf byteBuf) throws Exception {
        produceRequest.serializeTo(byteBuf);
    }
}
