/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.support.header.HeaderExchangeHandler;
import org.apache.dubbo.rpc.GracefulShutdown;
import org.apache.dubbo.rpc.ProtocolServer;

import java.nio.channels.ClosedChannelException;
import java.util.Collection;

import static org.apache.dubbo.common.constants.CommonConstants.READONLY_EVENT;
import static org.apache.dubbo.common.constants.CommonConstants.WRITEABLE_EVENT;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_CLOSE_STREAM;

public class DubboGracefulShutdown implements GracefulShutdown {
    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(DubboGracefulShutdown.class);
    private final DubboProtocol dubboProtocol;

    public DubboGracefulShutdown(DubboProtocol dubboProtocol) {
        this.dubboProtocol = dubboProtocol;
    }

    @Override
    public void readonly() {
        sendEvent(READONLY_EVENT);
    }

    @Override
    public void writeable() {
        sendEvent(WRITEABLE_EVENT);
    }

    private void sendEvent(String event) {
        try {
            // 遍历所有服务器
            for (ProtocolServer server : dubboProtocol.getServers()) {
                Collection<Channel> channels = server.getRemotingServer().getChannels();
                Request request = new Request();
                request.setEvent(event);
                // 单向通信，客户端发送请求后不等待响应，直接返回
                request.setTwoWay(false);
                request.setVersion(Version.getProtocolVersion());

                // 向每个channel发送事件
                for (Channel channel : channels) {
                    try {
                        if (channel.isConnected()) {
                            /**
                             * {@link HeaderExchangeChannel#send(Object, boolean)}
                             *
                             * 接收处理的是在 {@link HeaderExchangeHandler#handlerEvent(Channel, Request)}
                             */
                            channel.send(request, channel.getUrl().getParameter(Constants.CHANNEL_READONLYEVENT_SENT_KEY, true));
                        }
                    } catch (RemotingException e) {
                        if (e.getCause() instanceof ClosedChannelException) {
                            // ignore ClosedChannelException which means the connection has been closed.
                            continue;
                        }
                        logger.warn(TRANSPORT_FAILED_CLOSE_STREAM, "", "", "send cannot write message error.", e);
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn(TRANSPORT_FAILED_CLOSE_STREAM, "", "", "send cannot write message error.", e);
        }
    }
}
