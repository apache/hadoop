/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdsl.HdslUtils.getScmAddressForBlockClients;
import static org.apache.hadoop.hdsl.HdslUtils.getScmAddressForClients;
import static org.apache.hadoop.ozone.KsmUtils.getKsmAddress;
import static org.apache.hadoop.ozone.OzoneConfigKeys.*;
import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS;
import static com.sun.jersey.api.core.ResourceConfig.FEATURE_TRACE;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.sun.jersey.api.container.ContainerFactory;
import com.sun.jersey.api.core.ApplicationAdapter;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.ksm.protocolPB
    .KeySpaceManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.ksm.protocolPB.KeySpaceManagerProtocolPB;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.ObjectStoreApplication;
import org.apache.hadoop.ozone.web.handlers.ServiceFilter;
import org.apache.hadoop.ozone.web.netty.ObjectStoreJerseyContainer;
import org.apache.hadoop.scm.protocolPB
    .ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.storage.DistributedStorageHandler;
import org.apache.hadoop.ozone.web.localstorage.LocalStorageHandler;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implements object store handling within the DataNode process.  This class is
 * responsible for initializing and maintaining the RPC clients and servers and
 * the web application required for the object store implementation.
 */
public final class ObjectStoreHandler implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectStoreHandler.class);

  private final ObjectStoreJerseyContainer objectStoreJerseyContainer;
  private final KeySpaceManagerProtocolClientSideTranslatorPB
      keySpaceManagerClient;
  private final StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private final ScmBlockLocationProtocolClientSideTranslatorPB
      scmBlockLocationClient;
  private final StorageHandler storageHandler;

  /**
   * Creates a new ObjectStoreHandler.
   *
   * @param conf configuration
   * @throws IOException if there is an I/O error
   */
  public ObjectStoreHandler(Configuration conf) throws IOException {
    String shType = conf.getTrimmed(OZONE_HANDLER_TYPE_KEY,
        OZONE_HANDLER_TYPE_DEFAULT);
    LOG.info("ObjectStoreHandler initializing with {}: {}",
        OZONE_HANDLER_TYPE_KEY, shType);
    boolean ozoneTrace = conf.getBoolean(OZONE_TRACE_ENABLED_KEY,
        OZONE_TRACE_ENABLED_DEFAULT);

    // Initialize Jersey container for object store web application.
    if (OzoneConsts.OZONE_HANDLER_DISTRIBUTED.equalsIgnoreCase(shType)) {
      RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
          ProtobufRpcEngine.class);
      long scmVersion =
          RPC.getProtocolVersion(StorageContainerLocationProtocolPB.class);

      InetSocketAddress scmAddress =
          getScmAddressForClients(conf);
      this.storageContainerLocationClient =
          new StorageContainerLocationProtocolClientSideTranslatorPB(
              RPC.getProxy(StorageContainerLocationProtocolPB.class, scmVersion,
              scmAddress, UserGroupInformation.getCurrentUser(), conf,
              NetUtils.getDefaultSocketFactory(conf),
              Client.getRpcTimeout(conf)));

      InetSocketAddress scmBlockAddress =
          getScmAddressForBlockClients(conf);
      this.scmBlockLocationClient =
          new ScmBlockLocationProtocolClientSideTranslatorPB(
              RPC.getProxy(ScmBlockLocationProtocolPB.class, scmVersion,
                  scmBlockAddress, UserGroupInformation.getCurrentUser(), conf,
                  NetUtils.getDefaultSocketFactory(conf),
                  Client.getRpcTimeout(conf)));

      RPC.setProtocolEngine(conf, KeySpaceManagerProtocolPB.class,
          ProtobufRpcEngine.class);
      long ksmVersion =
          RPC.getProtocolVersion(KeySpaceManagerProtocolPB.class);
      InetSocketAddress ksmAddress = getKsmAddress(conf);
      this.keySpaceManagerClient =
          new KeySpaceManagerProtocolClientSideTranslatorPB(
              RPC.getProxy(KeySpaceManagerProtocolPB.class, ksmVersion,
              ksmAddress, UserGroupInformation.getCurrentUser(), conf,
              NetUtils.getDefaultSocketFactory(conf),
              Client.getRpcTimeout(conf)));

      storageHandler = new DistributedStorageHandler(
          new OzoneConfiguration(conf),
          this.storageContainerLocationClient,
          this.keySpaceManagerClient);
    } else {
      if (OzoneConsts.OZONE_HANDLER_LOCAL.equalsIgnoreCase(shType)) {
        storageHandler = new LocalStorageHandler(conf);
        this.storageContainerLocationClient = null;
        this.scmBlockLocationClient = null;
        this.keySpaceManagerClient = null;
      } else {
        throw new IllegalArgumentException(
            String.format("Unrecognized value for %s: %s,"
                + " Allowed values are %s,%s",
                OZONE_HANDLER_TYPE_KEY, shType,
                OzoneConsts.OZONE_HANDLER_DISTRIBUTED,
                OzoneConsts.OZONE_HANDLER_LOCAL));
      }
    }
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
    IOUtils.cleanupWithLogger(LOG, scmBlockLocationClient);
    IOUtils.cleanupWithLogger(LOG, keySpaceManagerClient);
  }
}
