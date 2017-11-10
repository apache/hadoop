/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.ksm.KeySpaceManager;
import org.apache.hadoop.ozone.scm.StorageContainerManager;
import org.apache.hadoop.ozone.web.client.OzoneRestClient;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Interface used for MiniOzoneClusters.
 */
public interface MiniOzoneCluster extends AutoCloseable, Closeable {
  void close();

  boolean restartDataNode(int i) throws IOException;

  boolean restartDataNode(int i, boolean keepPort) throws IOException;

  void shutdown();

  StorageContainerManager getStorageContainerManager();

  KeySpaceManager getKeySpaceManager();

  OzoneRestClient createOzoneRestClient() throws OzoneException;

  StorageContainerLocationProtocolClientSideTranslatorPB
  createStorageContainerLocationClient() throws IOException;

  void waitOzoneReady() throws TimeoutException, InterruptedException;

  void waitDatanodeOzoneReady(int dnIndex)
      throws TimeoutException, InterruptedException;

  void waitTobeOutOfChillMode() throws TimeoutException,
      InterruptedException;

  void waitForHeartbeatProcessed() throws TimeoutException,
      InterruptedException;
}