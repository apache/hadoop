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

package org.apache.hadoop.ozone.container.common.transport.client;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;

/**
 * XceiverClientManager is responsible for the lifecycle of XceiverClient
 * instances.  Callers use this class to acquire an XceiverClient instance
 * connected to the desired container pipeline.  When done, the caller also uses
 * this class to release the previously acquired XceiverClient instance.
 *
 * This class may evolve to implement efficient lifecycle management policies by
 * caching container location information and pooling connected client instances
 * for reuse without needing to reestablish a socket connection.  The current
 * implementation simply allocates and closes a new instance every time.
 */
public class XceiverClientManager {

  private final OzoneConfiguration conf;

  /**
   * Creates a new XceiverClientManager.
   *
   * @param conf configuration
   */
  public XceiverClientManager(OzoneConfiguration conf) {
    Preconditions.checkNotNull(conf);
    this.conf = conf;
  }

  /**
   * Acquires a XceiverClient connected to a container capable of storing the
   * specified key.
   *
   * @param pipeline the container pipeline for the client connection
   * @return XceiverClient connected to a container
   * @throws IOException if an XceiverClient cannot be acquired
   */
  public XceiverClient acquireClient(Pipeline pipeline) throws IOException {
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkArgument(pipeline.getMachines() != null);
    Preconditions.checkArgument(!pipeline.getMachines().isEmpty());
    XceiverClient xceiverClient = new XceiverClient(pipeline, conf);
    try {
      xceiverClient.connect();
    } catch (Exception e) {
      throw new IOException("Exception connecting XceiverClient.", e);
    }
    return xceiverClient;
  }

  /**
   * Releases an XceiverClient after use.
   *
   * @param xceiverClient client to release
   */
  public void releaseClient(XceiverClient xceiverClient) {
    Preconditions.checkNotNull(xceiverClient);
    xceiverClient.close();
  }
}
