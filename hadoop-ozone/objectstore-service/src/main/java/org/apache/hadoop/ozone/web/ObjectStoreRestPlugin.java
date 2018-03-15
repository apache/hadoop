/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.web;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeServicePlugin;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.web.netty.ObjectStoreRestHttpServer;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataNode service plugin implementation to start ObjectStore rest server.
 */
public class ObjectStoreRestPlugin implements DataNodeServicePlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectStoreRestPlugin.class);

  private ObjectStoreHandler handler;

  private volatile int restServicePort = -1;

  private ObjectStoreRestHttpServer objectStoreRestHttpServer;

  public ObjectStoreRestPlugin() {
    OzoneConfiguration.activate();
  }

  @Override
  public void start(Object service) {
    DataNode dataNode = (DataNode) service;
    if (OzoneUtils.isOzoneEnabled(dataNode.getConf())) {
      try {
        handler = new ObjectStoreHandler(dataNode.getConf());
        ServerSocketChannel httpServerChannel =
            dataNode.getSecureResources() != null ?
                dataNode.getSecureResources().getHttpServerChannel() :
                null;

        objectStoreRestHttpServer =
            new ObjectStoreRestHttpServer(dataNode.getConf(), httpServerChannel,
                handler);

        objectStoreRestHttpServer.start();
      } catch (IOException e) {
        throw new RuntimeException("Can't start the Object Store Rest server",
            e);
      }
      synchronized (this) {
        try {
          restServicePort =
              objectStoreRestHttpServer.getHttpAddress().getPort();
        } finally {
          //in case fo waiting for the port information: we can continue.
          this.notify();
        }
      }
    }
  }

  @Override
  public void stop() {
    try {
      handler.close();
    } catch (Exception e) {
      throw new RuntimeException("Can't stop the Object Store Rest server", e);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(objectStoreRestHttpServer);
    IOUtils.closeQuietly(handler);
  }

  @Override
  public void onDatanodeIdCreation(DatanodeID dataNodeId) {
    synchronized (this) {
      if (restServicePort == -1) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          LOG.error("Wait for starting up http server is interrupted.");
        }
      }
    }
    dataNodeId.setOzoneRestPort(restServicePort);
  }
}
