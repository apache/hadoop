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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * A SyncMount represents immutable information about a PROVIDED Storage
 * sync mount * binding (local -> remote).
 *
 * TODO: ehiggs - Name should not be here since that is something we use to
 * look up * the binding but not anything about the actual binding itself. (cf.
 * INodes don't know their filenames). - but we want `hdfs syncservice -list`
 * to also have the names.
 *
 * @See SyncMountStatus
 */
public class SyncMount {
  private final String name;
  private final Path localPath;
  private final URI remoteLocation;

  public SyncMount(String name, Path localPath, URI remoteLocation) {
    this.name = name;
    this.localPath = localPath;
    this.remoteLocation = remoteLocation;
  }

  public String getName() {
    return name;
  }

  public Path getLocalPath() {
    return localPath;
  }

  public URI getRemoteLocation() {
    return remoteLocation;
  }

  @Override
  public String toString() {
    return "SyncMount{" +
        "name='" + name + '\'' +
        ", localPath=" + localPath +
        ", remoteLocation=" + remoteLocation +
        '}';
  }
}
