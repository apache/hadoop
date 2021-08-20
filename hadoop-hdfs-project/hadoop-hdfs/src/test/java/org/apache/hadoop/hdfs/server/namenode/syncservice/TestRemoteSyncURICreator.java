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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.fs.MountMode;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.junit.Test;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.UUID;

import static org.apache.hadoop.hdfs.server.namenode.syncservice.RemoteSyncURICreator.createRemotePath;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.RemoteSyncURICreator.createRemotePathFromAbsolutePath;
import static org.junit.Assert.assertEquals;

/**
 * Test RemoteSyncURICreator.
 */
public class TestRemoteSyncURICreator {

  @Test
  public void testFileInRoot() {
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(UUID.randomUUID(),
        "/irrelavant/local/path/to/backup/dir",
        "hdfs://127.0.0.1:0/remote-path/", MountMode.WRITEBACK);
    URI remotePath = createRemotePath(syncMount, "file.bin");
    assertEquals(UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/file.bin")
            .build(), remotePath);
  }


  @Test
  public void testDirectoriesAndFile() {
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(UUID.randomUUID(),
        "/irrelavant/local/path/to/backup/dir",
        "hdfs://127.0.0.1:0/remote-path/", MountMode.WRITEBACK);
    URI remotePath = createRemotePath(syncMount, "a/b/c/file.bin");
    assertEquals(UriBuilder.fromUri(
        "hdfs://127.0.0.1:0/remote-path/a/b/c/file.bin").build(),
        remotePath);
  }

  @Test
  public void testAbsoluteCaseFileInRoot() {
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(UUID.randomUUID(),
        "/local/backup-dir-path/",
        "hdfs://127.0.0.1:0/remote-path/", MountMode.WRITEBACK);
    URI remotePath = createRemotePathFromAbsolutePath(syncMount,
        "/local/backup-dir-path/file.bin");
    assertEquals(UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/file.bin")
            .build(), remotePath);
  }

  @Test
  public void testAbsoluteCaseFileDirectoriesAndFile() {
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(UUID.randomUUID(),
        "/local/backup-dir-path/",
        "hdfs://127.0.0.1:0/remote-path/", MountMode.WRITEBACK);
    URI remotePath = createRemotePathFromAbsolutePath(syncMount,
        "/local/backup-dir-path/a/b/c/file.bin");
    assertEquals(UriBuilder.fromUri(
        "hdfs://127.0.0.1:0/remote-path/a/b/c/file.bin").build(), remotePath);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testAbsoluteCaseNoSharedPrefixThrowsException() {
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(UUID.randomUUID(),
        "/local/backup-dir-path/",
        "hdfs://127.0.0.1:0/remote-path/", MountMode.WRITEBACK);
    URI remotePath = createRemotePathFromAbsolutePath(syncMount,
        "/local/alternative-backup-dir-path/a/b/c/file.bin");

  }
}