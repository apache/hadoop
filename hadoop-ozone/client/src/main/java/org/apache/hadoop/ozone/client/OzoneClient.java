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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

/**
 * OzoneClient connects to Ozone Cluster and
 * perform basic operations.
 */
public class OzoneClient implements Closeable {

  /*
   * OzoneClient connects to Ozone Cluster and
   * perform basic operations.
   *
   * +-------------+     +---+   +-------------------------------------+
   * | OzoneClient | --> | C |   | Object Store                        |
   * |_____________|     | l |   |  +-------------------------------+  |
   *                     | i |   |  | Volume(s)                     |  |
   *                     | e |   |  |   +------------------------+  |  |
   *                     | n |   |  |   | Bucket(s)              |  |  |
   *                     | t |   |  |   |   +------------------+ |  |  |
   *                     |   |   |  |   |   | Key -> Value (s) | |  |  |
   *                     | P |-->|  |   |   |                  | |  |  |
   *                     | r |   |  |   |   |__________________| |  |  |
   *                     | o |   |  |   |                        |  |  |
   *                     | t |   |  |   |________________________|  |  |
   *                     | o |   |  |                               |  |
   *                     | c |   |  |_______________________________|  |
   *                     | o |   |                                     |
   *                     | l |   |_____________________________________|
   *                     |___|
   * Example:
   * ObjectStore store = client.getObjectStore();
   * store.createVolume(“volume one”, VolumeArgs);
   * volume.setQuota(“10 GB”);
   * OzoneVolume volume = store.getVolume(“volume one”);
   * volume.createBucket(“bucket one”, BucketArgs);
   * bucket.setVersioning(true);
   * OzoneOutputStream os = bucket.createKey(“key one”, 1024);
   * os.write(byte[]);
   * os.close();
   * OzoneInputStream is = bucket.readKey(“key one”);
   * is.read();
   * is.close();
   * bucket.deleteKey(“key one”);
   * volume.deleteBucket(“bucket one”);
   * store.deleteVolume(“volume one”);
   * client.close();
   */

  private final ClientProtocol proxy;
  private final ObjectStore objectStore;

  /**
   * Creates a new OzoneClient object, generally constructed
   * using {@link OzoneClientFactory}.
   * @param conf Configuration object
   * @param proxy ClientProtocol proxy instance
   */
  public OzoneClient(Configuration conf, ClientProtocol proxy) {
    this.proxy = proxy;
    this.objectStore = new ObjectStore(conf, this.proxy);
  }

  @VisibleForTesting
  protected OzoneClient(ObjectStore objectStore) {
    this.objectStore = objectStore;
    this.proxy = null;
  }
  /**
   * Returns the object store associated with the Ozone Cluster.
   * @return ObjectStore
   */
  public ObjectStore getObjectStore() {
    return objectStore;
  }

  /**
   * Closes the client and all the underlying resources.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    proxy.close();
  }
}
