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

package org.apache.hadoop.ozone.client;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;

import java.io.IOException;

/**
 * ObjectStore class is responsible for the client operations that can be
 * performed on Ozone Object Store.
 */
public class ObjectStore {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  private final ClientProtocol proxy;

  /**
   * Creates an instance of ObjectStore with the proxy.
   * @param proxy ClientProtocol proxy
   */
  public ObjectStore(ClientProtocol proxy) {
    this.proxy = proxy;
  }

  /**
   * Creates the volume with default values.
   * @param volumeName Name of the volume to be created.
   * @throws IOException
   */
  public void createVolume(String volumeName) throws IOException {
    Preconditions.checkNotNull(volumeName);
    proxy.createVolume(volumeName);
  }

  /**
   * Creates the volume.
   * @param volumeName Name of the volume to be created.
   * @param volumeArgs Volume properties.
   * @throws IOException
   */
  public void createVolume(String volumeName, VolumeArgs volumeArgs)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(volumeArgs);
    proxy.createVolume(volumeName, volumeArgs);
  }

  /**
   * Returns the volume information.
   * @param volumeName Name of the volume.
   * @return OzoneVolume
   * @throws IOException
   */
  public OzoneVolume getVolume(String volumeName) throws IOException {
    Preconditions.checkNotNull(volumeName);
    OzoneVolume volume = proxy.getVolumeDetails(volumeName);
    volume.setClientProxy(proxy);
    return volume;
  }

  /**
   * Deletes the volume.
   * @param volumeName Name of the volume.
   * @throws IOException
   */
  public void deleteVolume(String volumeName) throws IOException {
    Preconditions.checkNotNull(volumeName);
    proxy.deleteVolume(volumeName);
  }
}
