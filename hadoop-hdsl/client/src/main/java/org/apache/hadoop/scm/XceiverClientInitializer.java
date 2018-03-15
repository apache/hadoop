/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.scm;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.util.concurrent.Semaphore;

/**
 * Setup the netty pipeline.
 */
public class XceiverClientInitializer extends
    ChannelInitializer<SocketChannel> {
  private final Pipeline pipeline;
  private final Semaphore semaphore;

  /**
   * Constructs an Initializer for the client pipeline.
   * @param pipeline  - Pipeline.
   */
  public XceiverClientInitializer(Pipeline pipeline, Semaphore semaphore) {
    this.pipeline = pipeline;
    this.semaphore = semaphore;
  }

  /**
   * This method will be called once when the Channel is registered. After
   * the method returns this instance will be removed from the
   * ChannelPipeline of the Channel.
   *
   * @param ch   Channel which was registered.
   * @throws Exception is thrown if an error occurs. In that case the
   *                   Channel will be closed.
   */
  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline p = ch.pipeline();

    p.addLast(new ProtobufVarint32FrameDecoder());
    p.addLast(new ProtobufDecoder(ContainerProtos
        .ContainerCommandResponseProto.getDefaultInstance()));

    p.addLast(new ProtobufVarint32LengthFieldPrepender());
    p.addLast(new ProtobufEncoder());

    p.addLast(new XceiverClientHandler(this.pipeline, this.semaphore));

  }
}
