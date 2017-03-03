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

package org.apache.hadoop.hdfs.server.datanode.extdataset;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

public class ExternalVolumeImpl implements FsVolumeSpi {
  @Override
  public FsVolumeReference obtainReference() throws ClosedChannelException {
    return null;
  }

  @Override
  public String[] getBlockPoolList() {
    return null;
  }

  @Override
  public long getAvailable() throws IOException {
    return 0;
  }

  @Override
  public String getStorageID() {
    return null;
  }

  @Override
  public StorageType getStorageType() {
    return StorageType.DEFAULT;
  }

  @Override
  public boolean isTransientStorage() {
    return false;
  }

  @Override
  public void reserveSpaceForReplica(long bytesToReserve) {
  }

  @Override
  public void releaseReservedSpace(long bytesToRelease) {
  }

  @Override
  public void releaseLockedMemory(long bytesToRelease) {
  }

  @Override
  public BlockIterator newBlockIterator(String bpid, String name) {
    return null;
  }

  @Override
  public BlockIterator loadBlockIterator(String bpid, String name)
      throws IOException {
    return null;
  }

  @Override
  public FsDatasetSpi getDataset() {
    return null;
  }

  @Override
  public StorageLocation getStorageLocation() {
    return null;
  }

  @Override
  public URI getBaseURI() {
    return null;
  }

  @Override
  public DF getUsageStats(Configuration conf) {
    return null;
  }

  @Override
  public byte[] loadLastPartialChunkChecksum(
      File blockFile, File metaFile) throws IOException {
    return null;
  }

  @Override
  public LinkedList<ScanInfo> compileReport(String bpid,
      LinkedList<ScanInfo> report, ReportCompiler reportCompiler)
      throws InterruptedException, IOException {
    return null;
  }

  @Override
  public FileIoProvider getFileIoProvider() {
    return null;
  }

  @Override
  public DataNodeVolumeMetrics getMetrics() {
    return null;
  }

  @Override
  public VolumeCheckResult check(VolumeCheckContext context)
      throws Exception {
    return VolumeCheckResult.HEALTHY;
  }
}
