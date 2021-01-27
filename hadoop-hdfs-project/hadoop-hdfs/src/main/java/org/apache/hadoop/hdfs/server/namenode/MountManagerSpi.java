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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MountMode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An interface for mount manager.
 */
public interface MountManagerSpi {

  /**
   * Check whether a mount mode is supported by a given mount manager.
   * @return
   */
  boolean isSupervisedMode(MountMode mountMode);

  /**
   * Prepare adding a new mount.
   * @param tempMountPath temp mount path, will be renamed to user-specified
   *                      mount path at last.
   * @param remotePath  remote path of provided storage.
   * @param mergedConf  config with remote config merged.
   * @throws IOException
   */
  void prepareMount(String tempMountPath, String remotePath,
      Configuration mergedConf) throws IOException;

  /**
   * Do some operations after successfully mount.
   * @param volInfo
   * @param isMounted the mount has been created before if true.
   * @throws IOException
   */
  void finishMount(ProvidedVolumeInfo volInfo, boolean isMounted)
      throws IOException;

  /**
   * Do some cleanup work after removing a mount.
   * @param volInfo
   * @throws IOException
   */
  void removeMount(ProvidedVolumeInfo volInfo) throws IOException;

  /**
   * Start cache service.
   */
  void startService() throws IOException;

  /**
   * Stop cache service.
   */
  void stopService();

  /**
   * Get provided storage metrics.
   */
  String getMetrics(ProvidedVolumeInfo volumeInfo);

  /**
   * This method is used to verify the remote connection, thus we can avoid
   * mount operation blocked too long when remote storage is not accessible.
   */
  default void verifyConnection(String remotePath, Configuration conf)
      throws IOException {
    ExecutorService es = Executors.newSingleThreadExecutor();
    Future<Boolean> future = es.submit(() -> {
      FileSystem fs = FileSystem.get(new URI(remotePath), conf);
      return fs.exists(new Path(remotePath));
    });
    try {
      boolean exists = future.get(10, TimeUnit.SECONDS);
      if (!exists) {
        throw new IOException("The given remote storage path doesn't exist!");
      }
    } catch (TimeoutException e) {
      throw new IOException("Time out in verifying remote " +
          "connection for " + remotePath);
    } catch (Exception e) {
      throw new IOException("Failed to verify the " +
          "connection due to " + e.getMessage());
    } finally {
      future.cancel(true);
    }
  }
}
