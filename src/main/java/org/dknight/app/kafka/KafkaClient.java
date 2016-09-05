package org.dknight.app.kafka;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;


/**
 * Created by fanming.chen on 2016/9/5 0005.
 */
public class KafkaClient {

    private static final int port = 23017;

    public static void main(String[] args) throws InterruptedException {
        String host = args[0];
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new KafkaProduceHanlder());
                }
            });

            ChannelFuture f = bootstrap.connect(host, port).sync();

            f.channel().closeFuture().sync();
        } finally {

        }
    }
}
