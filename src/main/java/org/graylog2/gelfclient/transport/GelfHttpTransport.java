/*
 * Copyright 2014 TORCH GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graylog2.gelfclient.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.graylog2.gelfclient.GelfConfiguration;
import org.graylog2.gelfclient.encoder.GelfMessageJsonEncoder;
import org.graylog2.gelfclient.encoder.GelfTcpFrameDelimiterEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;

/**
 * A {@link GelfTransport} implementation that uses HTTP to send GELF messages.
 * <p>This class is thread-safe.</p>
 */
public class GelfHttpTransport extends AbstractGelfTransport {
    private static final Logger LOG = LoggerFactory.getLogger(GelfHttpTransport.class);

    /**
     * Creates a new HTTP GELF transport.
     *
     * @param config the GELF client configuration
     */
    public GelfHttpTransport(GelfConfiguration config) {
        super(config);
    }

    @Override
    protected void createBootstrap(final EventLoopGroup workerGroup) {
        final Bootstrap bootstrap = new Bootstrap();
        final GelfSenderThread senderThread = new GelfSenderThread(queue);

        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout())             
                .remoteAddress(config.getRemoteAddress())
                .handler(new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						
						// Use the Netty HTTP codec
						p.addLast(new HttpClientCodec());
						
						// Graylog doesn't return compressed responses so we are skipping the setup of a compression handler
						// At least according to the Graylog documents ;)
						
						p.addLast(new SimpleChannelInboundHandler<HttpObject>() {
							@Override
							public void channelRead0(ChannelHandlerContext ctxt, HttpObject message) {
								if(message instanceof HttpResponse) {
									//Cast it
									HttpResponse response = (HttpResponse) message;
									
									//The only thing we should get is a 202 Accepted.
									//This means that the message was accepted for processing
									if (response.getStatus() != HttpResponseStatus.ACCEPTED) {
										//TODO: Graceful error handling, i.e. a retry or something
										//Right now we do nothing
									}
								}
							}
						});
					}
                	
                });

        if (config.getSendBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_SNDBUF, config.getSendBufferSize());
        }

        bootstrap.connect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    LOG.debug("Connected!");
                } else {
                    LOG.error("Connection failed: {}", future.cause().getMessage());
                    scheduleReconnect(future.channel().eventLoop());
                }
            }
        });
    }
}
