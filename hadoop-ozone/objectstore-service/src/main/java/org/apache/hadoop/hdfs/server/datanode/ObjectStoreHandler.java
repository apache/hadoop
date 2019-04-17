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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.web.ObjectStoreApplication;
import org.apache.hadoop.ozone.web.handlers.ServiceFilter;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.netty.ObjectStoreJerseyContainer;
import org.apache.hadoop.ozone.web.storage.DistributedStorageHandler;
import org.apache.hadoop.security.UserGroupInformation;

import com.sun.jersey.api.container.ContainerFactory;
import com.sun.jersey.api.core.ApplicationAdapter;
import static com.sun.jersey.api.core.ResourceConfig.FEATURE_TRACE;
import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.ozone.OmUtils.getOmAddress;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_TRACE_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_TRACE_ENABLED_KEY;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements object store handling within the DataNode process.  This class is
 * responsible for initializing and maintaining the RPC clients and servers and
 * the web application required for the object store implementation.
 */
public final class ObjectStoreHandler implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectStoreHandler.class);

  private final ObjectStoreJerseyContainer objectStoreJerseyContainer;
  private final OzoneManagerProtocol ozoneManagerClient;
  private final StorageContainerLocationProtocol
      storageContainerLocationClient;

  private final StorageHandler storageHandler;
  private ClientId clientId = ClientId.randomId();

  /**
   * Creates a new ObjectStoreHandler.
   *
   * @param conf configuration
   * @throws IOException if there is an I/O error
   */
  public ObjectStoreHandler(Configuration conf) throws IOException {
    boolean ozoneTrace = conf.getBoolean(OZONE_TRACE_ENABLED_KEY,
        OZONE_TRACE_ENABLED_DEFAULT);

    // Initialize Jersey container for object store web application.
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(StorageContainerLocationProtocolPB.class);

    InetSocketAddress scmAddress =
        getScmAddressForClients(conf);
    this.storageContainerLocationClient =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                RPC.getProxy(StorageContainerLocationProtocolPB.class,
                    scmVersion,
                    scmAddress, UserGroupInformation.getCurrentUser(), conf,
                    NetUtils.getDefaultSocketFactory(conf),
                    Client.getRpcTimeout(conf))),
            StorageContainerLocationProtocol.class, conf);

    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    long omVersion =
        RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    InetSocketAddress omAddress = getOmAddress(conf);
    this.ozoneManagerClient =
        TracingUtil.createProxy(
            new OzoneManagerProtocolClientSideTranslatorPB(
                RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
                    omAddress, UserGroupInformation.getCurrentUser(), conf,
                    NetUtils.getDefaultSocketFactory(conf),
                    Client.getRpcTimeout(conf)), clientId.toString()),
            OzoneManagerProtocol.class, conf);

    storageHandler = new DistributedStorageHandler(
        new OzoneConfiguration(conf),
        TracingUtil.createProxy(storageContainerLocationClient,
            StorageContainerLocationProtocol.class, conf),
        this.ozoneManagerClient);
    ApplicationAdapter aa =
        new ApplicationAdapter(new ObjectStoreApplication());
    Map<String, Object> settingsMap = new HashMap<>();
    settingsMap.put(PROPERTY_CONTAINER_REQUEST_FILTERS,
        ServiceFilter.class.getCanonicalName());
    settingsMap.put(FEATURE_TRACE, ozoneTrace);
    aa.setPropertiesAndFeatures(settingsMap);
    this.objectStoreJerseyContainer = ContainerFactory.createContainer(
        ObjectStoreJerseyContainer.class, aa);
    this.objectStoreJerseyContainer.setStorageHandler(storageHandler);
  }

  /**
   * Returns the initialized web application container.
   *
   * @return initialized web application container
   */
  public ObjectStoreJerseyContainer getObjectStoreJerseyContainer() {
    return this.objectStoreJerseyContainer;
  }

  /**
   * Returns the storage handler.
   *
   * @return returns the storage handler
   */
  public StorageHandler getStorageHandler() {
    return this.storageHandler;
  }

  @Override
  public void close() {
    LOG.info("Closing ObjectStoreHandler.");
    storageHandler.close();
    IOUtils.cleanupWithLogger(LOG, storageContainerLocationClient);
    IOUtils.cleanupWithLogger(LOG, ozoneManagerClient);
  }
}
