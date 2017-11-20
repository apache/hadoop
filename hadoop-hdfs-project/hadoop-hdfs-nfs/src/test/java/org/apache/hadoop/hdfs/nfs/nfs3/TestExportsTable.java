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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.FileSystemException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.nfs.mount.Mountd;
import org.apache.hadoop.hdfs.nfs.mount.RpcProgramMountd;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestExportsTable {

  @Rule
  public ExpectedException exception = ExpectedException.none();
 
  @Test
  public void testHdfsExportPoint() throws IOException {
    NfsConfiguration config = new NfsConfiguration();
    MiniDFSCluster cluster = null;

    // Use emphral port in case tests are running in parallel
    config.setInt("nfs3.mountd.port", 0);
    config.setInt("nfs3.server.port", 0);
    config.set("nfs.http.address", "0.0.0.0:0");

    try {
      cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).build();
      cluster.waitActive();

      // Start nfs
      final Nfs3 nfsServer = new Nfs3(config);
      nfsServer.startServiceInternal(false);

      Mountd mountd = nfsServer.getMountd();
      RpcProgramMountd rpcMount = (RpcProgramMountd) mountd.getRpcProgram();
      assertTrue(rpcMount.getExports().size() == 1);

      String exportInMountd = rpcMount.getExports().get(0);
      assertTrue(exportInMountd.equals("/"));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testViewFsMultipleExportPoint() throws IOException {
    NfsConfiguration config = new NfsConfiguration();
    MiniDFSCluster cluster = null;
    String clusterName = RandomStringUtils.randomAlphabetic(10);

    String exportPoint = "/hdfs1,/hdfs2";
    config.setStrings(NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY, exportPoint);
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        FsConstants.VIEWFS_SCHEME + "://" + clusterName);
    // Use emphral port in case tests are running in parallel
    config.setInt("nfs3.mountd.port", 0);
    config.setInt("nfs3.server.port", 0);
    config.set("nfs.http.address", "0.0.0.0:0");

    try {
      cluster =
          new MiniDFSCluster.Builder(config).nnTopology(
              MiniDFSNNTopology.simpleFederatedTopology(2))
              .numDataNodes(2)
              .build();
      cluster.waitActive();
      DistributedFileSystem hdfs1 = cluster.getFileSystem(0);
      DistributedFileSystem hdfs2 = cluster.getFileSystem(1);
      cluster.waitActive();
      Path base1 = new Path("/user1");
      Path base2 = new Path("/user2");
      hdfs1.delete(base1, true);
      hdfs2.delete(base2, true);
      hdfs1.mkdirs(base1);
      hdfs2.mkdirs(base2);
      ConfigUtil.addLink(config, clusterName, "/hdfs1",
          hdfs1.makeQualified(base1).toUri());
      ConfigUtil.addLink(config, clusterName, "/hdfs2",
          hdfs2.makeQualified(base2).toUri());

      // Start nfs
      final Nfs3 nfsServer = new Nfs3(config);
      nfsServer.startServiceInternal(false);

      Mountd mountd = nfsServer.getMountd();
      RpcProgramMountd rpcMount = (RpcProgramMountd) mountd.getRpcProgram();
      assertTrue(rpcMount.getExports().size() == 2);

      String exportInMountd1 = rpcMount.getExports().get(0);
      assertTrue(exportInMountd1.equals("/hdfs1"));

      String exportInMountd2 = rpcMount.getExports().get(1);
      assertTrue(exportInMountd2.equals("/hdfs2"));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testViewFsInternalExportPoint() throws IOException {
    NfsConfiguration config = new NfsConfiguration();
    MiniDFSCluster cluster = null;
    String clusterName = RandomStringUtils.randomAlphabetic(10);

    String exportPoint = "/hdfs1/subpath";
    config.setStrings(NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY, exportPoint);
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        FsConstants.VIEWFS_SCHEME + "://" + clusterName);
    // Use emphral port in case tests are running in parallel
    config.setInt("nfs3.mountd.port", 0);
    config.setInt("nfs3.server.port", 0);
    config.set("nfs.http.address", "0.0.0.0:0");

    try {
      cluster =
          new MiniDFSCluster.Builder(config).nnTopology(
              MiniDFSNNTopology.simpleFederatedTopology(2))
              .numDataNodes(2)
              .build();
      cluster.waitActive();
      DistributedFileSystem hdfs1 = cluster.getFileSystem(0);
      DistributedFileSystem hdfs2 = cluster.getFileSystem(1);
      cluster.waitActive();
      Path base1 = new Path("/user1");
      Path base2 = new Path("/user2");
      hdfs1.delete(base1, true);
      hdfs2.delete(base2, true);
      hdfs1.mkdirs(base1);
      hdfs2.mkdirs(base2);
      ConfigUtil.addLink(config, clusterName, "/hdfs1",
          hdfs1.makeQualified(base1).toUri());
      ConfigUtil.addLink(config, clusterName, "/hdfs2",
          hdfs2.makeQualified(base2).toUri());
      Path subPath = new Path(base1, "subpath");
      hdfs1.delete(subPath, true);
      hdfs1.mkdirs(subPath);

      // Start nfs
      final Nfs3 nfsServer = new Nfs3(config);
      nfsServer.startServiceInternal(false);

      Mountd mountd = nfsServer.getMountd();
      RpcProgramMountd rpcMount = (RpcProgramMountd) mountd.getRpcProgram();
      assertTrue(rpcMount.getExports().size() == 1);

      String exportInMountd = rpcMount.getExports().get(0);
      assertTrue(exportInMountd.equals(exportPoint));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testViewFsRootExportPoint() throws IOException {
    NfsConfiguration config = new NfsConfiguration();
    MiniDFSCluster cluster = null;
    String clusterName = RandomStringUtils.randomAlphabetic(10);

    String exportPoint = "/";
    config.setStrings(NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY, exportPoint);
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        FsConstants.VIEWFS_SCHEME + "://" + clusterName);
    // Use emphral port in case tests are running in parallel
    config.setInt("nfs3.mountd.port", 0);
    config.setInt("nfs3.server.port", 0);
    config.set("nfs.http.address", "0.0.0.0:0");

    try {
      cluster =
          new MiniDFSCluster.Builder(config).nnTopology(
              MiniDFSNNTopology.simpleFederatedTopology(2))
              .numDataNodes(2)
              .build();
      cluster.waitActive();
      DistributedFileSystem hdfs1 = cluster.getFileSystem(0);
      DistributedFileSystem hdfs2 = cluster.getFileSystem(1);
      cluster.waitActive();
      Path base1 = new Path("/user1");
      Path base2 = new Path("/user2");
      hdfs1.delete(base1, true);
      hdfs2.delete(base2, true);
      hdfs1.mkdirs(base1);
      hdfs2.mkdirs(base2);
      ConfigUtil.addLink(config, clusterName, "/hdfs1",
          hdfs1.makeQualified(base1).toUri());
      ConfigUtil.addLink(config, clusterName, "/hdfs2",
          hdfs2.makeQualified(base2).toUri());

      exception.expect(FileSystemException.class);
      exception.
          expectMessage("Only HDFS is supported as underlyingFileSystem, "
              + "fs scheme:viewfs");
      // Start nfs
      final Nfs3 nfsServer = new Nfs3(config);
      nfsServer.startServiceInternal(false);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testHdfsInternalExportPoint() throws IOException {
    NfsConfiguration config = new NfsConfiguration();
    MiniDFSCluster cluster = null;

    String exportPoint = "/myexport1";
    config.setStrings(NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY, exportPoint);
    // Use emphral port in case tests are running in parallel
    config.setInt("nfs3.mountd.port", 0);
    config.setInt("nfs3.server.port", 0);
    config.set("nfs.http.address", "0.0.0.0:0");
    Path base = new Path(exportPoint);

    try {
      cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem(0);
      hdfs.delete(base, true);
      hdfs.mkdirs(base);

      // Start nfs
      final Nfs3 nfsServer = new Nfs3(config);
      nfsServer.startServiceInternal(false);

      Mountd mountd = nfsServer.getMountd();
      RpcProgramMountd rpcMount = (RpcProgramMountd) mountd.getRpcProgram();
      assertTrue(rpcMount.getExports().size() == 1);

      String exportInMountd = rpcMount.getExports().get(0);
      assertTrue(exportInMountd.equals(exportPoint));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testInvalidFsExport() throws IOException {
    NfsConfiguration config = new NfsConfiguration();
    MiniDFSCluster cluster = null;

    // Use emphral port in case tests are running in parallel
    config.setInt("nfs3.mountd.port", 0);
    config.setInt("nfs3.server.port", 0);
    config.set("nfs.http.address", "0.0.0.0:0");

    try {
      cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).build();
      cluster.waitActive();
      config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
          FsConstants.LOCAL_FS_URI.toString());

      exception.expect(FileSystemException.class);
      exception.
          expectMessage("Only HDFS is supported as underlyingFileSystem, "
              + "fs scheme:file");
      // Start nfs
      final Nfs3 nfsServer = new Nfs3(config);
      nfsServer.startServiceInternal(false);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
