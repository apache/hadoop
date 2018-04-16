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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Test class for DataNodeVolumeMetrics.
 */
public class TestDataNodeVolumeMetrics {
  private static final Log LOG =
      LogFactory.getLog(TestDataNodeVolumeMetrics.class);

  private static final int BLOCK_SIZE = 1024;
  private static final short REPL = 1;
  private static final int NUM_DATANODES = 1;

  @Rule
  public Timeout timeout = new Timeout(300000);

  @Test
  public void testVolumeMetrics() throws Exception {
    MiniDFSCluster cluster = setupClusterForVolumeMetrics();
    try {
      FileSystem fs = cluster.getFileSystem();
      final Path fileName = new Path("/test.dat");
      final long fileLen = Integer.MAX_VALUE + 1L;
      DFSTestUtil.createFile(fs, fileName, false, BLOCK_SIZE, fileLen,
          fs.getDefaultBlockSize(fileName),
          REPL, 1L, true);

      try (FSDataOutputStream out = fs.append(fileName)) {
        out.writeBytes("hello world");
        ((DFSOutputStream) out.getWrappedStream()).hsync();
      }

      verifyDataNodeVolumeMetrics(fs, cluster, fileName);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testVolumeMetricsWithVolumeDepartureArrival() throws Exception {
    MiniDFSCluster cluster = setupClusterForVolumeMetrics();
    try {
      FileSystem fs = cluster.getFileSystem();
      final Path fileName = new Path("/test.dat");
      final long fileLen = Integer.MAX_VALUE + 1L;
      DFSTestUtil.createFile(fs, fileName, false, BLOCK_SIZE, fileLen,
          fs.getDefaultBlockSize(fileName),
          REPL, 1L, true);

      try (FSDataOutputStream out = fs.append(fileName)) {
        out.writeBytes("hello world");
        ((DFSOutputStream) out.getWrappedStream()).hsync();
      }

      ArrayList<DataNode> dns = cluster.getDataNodes();
      assertTrue("DN1 should be up", dns.get(0).isDatanodeUp());
      final File dn1Vol2 = cluster.getInstanceStorageDir(0, 1);

      DataNodeTestUtils.injectDataDirFailure(dn1Vol2);
      verifyDataNodeVolumeMetrics(fs, cluster, fileName);

      DataNodeTestUtils.restoreDataDirFromFailure(dn1Vol2);
      DataNodeTestUtils.reconfigureDataNode(dns.get(0), dn1Vol2);
      verifyDataNodeVolumeMetrics(fs, cluster, fileName);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private MiniDFSCluster setupClusterForVolumeMetrics() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys
        .DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY, 100);
    SimulatedFSDataset.setFactory(conf);
    return new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .storageTypes(new StorageType[]{StorageType.RAM_DISK, StorageType.DISK})
        .storagesPerDatanode(2)
        .build();
  }

  private void verifyDataNodeVolumeMetrics(final FileSystem fs,
      final MiniDFSCluster cluster, final Path fileName) throws IOException {
    List<DataNode> datanodes = cluster.getDataNodes();
    DataNode datanode = datanodes.get(0);

    final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, fileName);
    final FsVolumeSpi volume = datanode.getFSDataset().getVolume(block);
    DataNodeVolumeMetrics metrics = volume.getMetrics();

    MetricsRecordBuilder rb = getMetrics(volume.getMetrics().name());
    assertCounter("TotalDataFileIos", metrics.getTotalDataFileIos(), rb);

    LOG.info("TotalMetadataOperations : " +
        metrics.getTotalMetadataOperations());
    LOG.info("TotalDataFileIos : " + metrics.getTotalDataFileIos());
    LOG.info("TotalFileIoErrors : " + metrics.getTotalFileIoErrors());

    LOG.info("MetadataOperationSampleCount : " +
        metrics.getMetadataOperationSampleCount());
    LOG.info("MetadataOperationMean : " + metrics.getMetadataOperationMean());
    LOG.info("MetadataFileIoStdDev : " +
        metrics.getMetadataOperationStdDev());

    LOG.info("DataFileIoSampleCount : " + metrics.getDataFileIoSampleCount());
    LOG.info("DataFileIoMean : " + metrics.getDataFileIoMean());
    LOG.info("DataFileIoStdDev : " + metrics.getDataFileIoStdDev());

    LOG.info("flushIoSampleCount : " + metrics.getFlushIoSampleCount());
    LOG.info("flushIoMean : " + metrics.getFlushIoMean());
    LOG.info("flushIoStdDev : " + metrics.getFlushIoStdDev());

    LOG.info("syncIoSampleCount : " + metrics.getSyncIoSampleCount());
    LOG.info("syncIoMean : " + metrics.getSyncIoMean());
    LOG.info("syncIoStdDev : " + metrics.getSyncIoStdDev());

    LOG.info("readIoSampleCount : " + metrics.getReadIoMean());
    LOG.info("readIoMean : " + metrics.getReadIoMean());
    LOG.info("readIoStdDev : " + metrics.getReadIoStdDev());

    LOG.info("writeIoSampleCount : " + metrics.getWriteIoSampleCount());
    LOG.info("writeIoMean : " + metrics.getWriteIoMean());
    LOG.info("writeIoStdDev : " + metrics.getWriteIoStdDev());

    LOG.info("fileIoErrorSampleCount : "
        + metrics.getFileIoErrorSampleCount());
    LOG.info("fileIoErrorMean : " + metrics.getFileIoErrorMean());
    LOG.info("fileIoErrorStdDev : " + metrics.getFileIoErrorStdDev());
  }
}
