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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.web.netty.ObjectStoreRestHttpServer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataNode service plugin implementation to start ObjectStore rest server.
 */
public class OzoneHddsDatanodeService implements ServicePlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneHddsDatanodeService.class);

  private Configuration conf;
  private ObjectStoreHandler handler;
  private ObjectStoreRestHttpServer objectStoreRestHttpServer;

  @Override
  public void start(Object service) {
    if (service instanceof HddsDatanodeService) {
      try {
        HddsDatanodeService hddsDatanodeService = (HddsDatanodeService) service;
        conf = hddsDatanodeService.getConf();
        handler = new ObjectStoreHandler(conf);
        objectStoreRestHttpServer = new ObjectStoreRestHttpServer(
            conf, null, handler);
        objectStoreRestHttpServer.start();
        DatanodeDetails.Port restPort = DatanodeDetails.newPort(
            DatanodeDetails.Port.Name.REST,
            objectStoreRestHttpServer.getHttpAddress().getPort());
        hddsDatanodeService.getDatanodeDetails().setPort(restPort);

      } catch (IOException e) {
        throw new RuntimeException("Can't start the Object Store Rest server",
            e);
      }
    } else {
      LOG.error("Not starting {}, as the plugin is not invoked through {}",
          OzoneHddsDatanodeService.class.getSimpleName(),
          HddsDatanodeService.class.getSimpleName());
    }
  }


  @Override
  public void stop() {
    try {
      if (handler != null) {
        handler.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Can't stop the Object Store Rest server", e);
    }
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(objectStoreRestHttpServer);
    IOUtils.closeQuietly(handler);
  }

}
