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

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.Closeable;
import java.io.IOException;

/**
 * A Client for the storageContainer protocol.
 */
public interface XceiverClientSpi extends Closeable {
  /**
   * Connects to the leader in the pipeline.
   */
  void connect() throws Exception;

  @Override
  void close();

  /**
   * Returns the pipeline of machines that host the container used by this
   * client.
   *
   * @return pipeline of machines that host the container
   */
  Pipeline getPipeline();

  /**
   * Sends a given command to server and gets the reply back.
   * @param request Request
   * @return Response to the command
   * @throws IOException
   */
  ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request) throws IOException;
}
