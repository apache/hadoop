/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SyncMount;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class RemoteSyncURICreator {

  public static URI createRemotePath(SyncMount syncMount, String localPath) {

    if (".".equals(localPath) || "".equals(localPath)) {
      return UriBuilder.fromUri(syncMount.getRemoteLocation())
          .path(Path.SEPARATOR)
          .build();
    } else {
      return UriBuilder.fromUri(syncMount.getRemoteLocation())
          .path(localPath)
          .build();
    }
  }

  /**
   * Strip off the basePath and append the localPath to the
   * SyncMount's  Remote URL.
   *
   * @param syncMount
   * @param absolutePath
   * @return
   * @throws IllegalArgumentException When the absolute path does not refer to a
   *                                  location within the SyncMount's local path
   */
  public static URI createRemotePathFromAbsolutePath(SyncMount syncMount,
      String absolutePath) {
    String localBackupPath = syncMount.getLocalPath().toString();
    if (!absolutePath.startsWith(localBackupPath)) {
      throw new IllegalArgumentException("The given absolute path must be " +
          "prefixed by the local path of the SyncMount.");
    }
    String trimmedPath = absolutePath.substring(localBackupPath.length(),
        absolutePath.length());
    return UriBuilder.fromUri(syncMount.getRemoteLocation())
        .path(trimmedPath)
        .build();
  }
}
