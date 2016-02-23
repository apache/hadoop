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

import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY;

import java.io.IOException;
import java.util.Collections;

import com.sun.jersey.api.container.ContainerFactory;
import com.sun.jersey.api.core.ApplicationAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.web.handlers.ServiceFilter;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.ObjectStoreApplication;
import org.apache.hadoop.ozone.web.netty.ObjectStoreJerseyContainer;
import org.apache.hadoop.ozone.web.storage.DistributedStorageHandler;
import org.apache.hadoop.ozone.web.localstorage.LocalStorageHandler;

/**
 * Implements object store handling within the DataNode process.  This class is
 * responsible for initializing and maintaining the RPC clients and servers and
 * the web application required for the object store implementation.
 */
public final class ObjectStoreHandler {

  private final ObjectStoreJerseyContainer objectStoreJerseyContainer;

  /**
   * Creates a new ObjectStoreHandler.
   *
   * @param conf configuration
   * @throws IOException if there is an I/O error
   */
  public ObjectStoreHandler(Configuration conf) throws IOException {
    String shType = conf.getTrimmed(DFS_STORAGE_HANDLER_TYPE_KEY,
        DFS_STORAGE_HANDLER_TYPE_DEFAULT);
    final StorageHandler storageHandler;
    if ("distributed".equalsIgnoreCase(shType)) {
      storageHandler = new DistributedStorageHandler();
    } else {
      if ("local".equalsIgnoreCase(shType)) {
        storageHandler = new LocalStorageHandler();
      } else {
        throw new IllegalArgumentException(
            String.format("Unrecognized value for %s: %s",
                DFS_STORAGE_HANDLER_TYPE_KEY, shType));
      }
    }
    ApplicationAdapter aa =
        new ApplicationAdapter(new ObjectStoreApplication());
    aa.setPropertiesAndFeatures(Collections.<String, Object>singletonMap(
        PROPERTY_CONTAINER_REQUEST_FILTERS,
        ServiceFilter.class.getCanonicalName()));
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
}
