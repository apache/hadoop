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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.AvailableSpaceVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReadThroughInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A synchronous implementation of the {@link ReadThroughInputStream}. Any data
 * read from the PROVIDED storage is written to a local volume first and then
 * returned to the caller. The local (cached) replica is finalized only when
 * this stream is closed.
 */
public class SynchronousReadThroughInputStream extends ReadThroughInputStream {

  static final Log LOG =
      LogFactory.getLog(SynchronousReadThroughInputStream.class);

  private ExtendedBlock blockToCache;
  // DISK type is used.
  private StorageType targetStorageType;

  /**
   * InputStream for data in provided storage.
   */
  private FSDataInputStream remoteIn;

  /**
   * the bounded stream that is used to read data.
   */
  private BoundedInputStream inputStream;

  private long fileOffset;
  private long seekOffset;
  private long maxBytesReadable;

  private Configuration conf;
  private FsDatasetImpl dataset;

  /**
   * local replica that is the cached-copy.
   */
  private ReplicaInPipeline localReplicaInfo;

  /**
   * OutputStream to local replica.
   */
  private OutputStream localReplicaOut;

  /**
   * reference to the volume to cache data locally.
   */
  private FsVolumeReference localVolumeReference;

  private long totalBytesRead;

  /**
   * flag to indicate if the data read, can be cached locally.
   */
  private boolean cacheDataLocally = false;

  @Override
  public void init(ExtendedBlock blockToCache, Configuration conf,
      FsDatasetSpi dataset, FSDataInputStream in, long fileOffset,
      long seekOffset, long maxBytesReadable) throws IOException {
    this.blockToCache = blockToCache;
    StorageType cacheStorageType =
        conf.getEnum(DFSConfigKeys.DFS_PROVIDED_READ_CACHE_STORAGE_TYPE_KEY,
        DFSConfigKeys.DFS_PROVIDED_READ_CACHE_STORAGE_TYPE_DEFAULT);
    if (cacheStorageType != StorageType.ARCHIVE &&
        cacheStorageType != StorageType.SSD) {
      cacheStorageType = StorageType.DISK;
    }
    this.targetStorageType = cacheStorageType;
    this.conf = conf;
    this.dataset = (FsDatasetImpl) dataset;
    this.remoteIn = in;
    this.inputStream =
        new BoundedInputStream(in, maxBytesReadable - seekOffset);
    this.fileOffset = fileOffset;
    this.seekOffset = seekOffset;
    this.maxBytesReadable = maxBytesReadable;
    // cache only if offset is 0
    // TODO cache otherwise as well.
    this.cacheDataLocally = (seekOffset == 0);
    this.totalBytesRead = 0;
    if (cacheDataLocally) {
      try {
        localVolumeReference = chooseLocalVolume();
        createLocalReplica((FsVolumeImpl) localVolumeReference.getVolume());
        LOG.debug("SynchronousReadThroughInputStream.init: blockToCache "
            + blockToCache + " localReplicaInfo is " + localReplicaInfo);
      } catch (IOException e) {
        LOG.warn("IOException " + e + " when creating a local replica in "
            + "SynchronousReadThroughInputStream for block " + blockToCache
            + "; Cleaning up local state;"
            + "will proceed without caching replica");
        cacheDataLocally = false;
        cleanUp();
      }
    }
  }

  private FsVolumeReference chooseLocalVolume() throws IOException {

    Class<? extends VolumeChoosingPolicy> volumeChoserClass = conf.getClass(
        DFSConfigKeys.DFS_DATANODE_PROVIDED_VOLUME_CHOOSING_POLICY,
        AvailableSpaceVolumeChoosingPolicy.class, VolumeChoosingPolicy.class);
    VolumeChoosingPolicy<FsVolumeSpi> volumeChooser =
        ReflectionUtils.newInstance(volumeChoserClass, conf);

    // find volumes of the target type.
    List<FsVolumeSpi> list = new ArrayList<>();
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      for (FsVolumeSpi vol : volumes) {
        if (vol.getStorageType() == targetStorageType) {
          list.add(vol);
        }
      }
    }

    try {
      FsVolumeSpi chosenVolume =
          volumeChooser.chooseVolume(list, blockToCache.getNumBytes(), null);
      return chosenVolume.obtainReference();
    } catch (IOException e) {
      throw new IOException("Unable to find suitable volume to cache block "
          + blockToCache + " Exception: " + e);
    }
  }

  private void createLocalReplica(FsVolumeImpl localVolume) throws IOException {
    localReplicaInfo = localVolume.createTemporary(blockToCache);
    if (localReplicaInfo == null) {
      throw new IOException("Caching block " + blockToCache.getBlockId()
          + " failed as temporary replicaInfo cannot be created on volume "
          + localVolume);
    }
    ReplicaOutputStreams streams = localReplicaInfo.createStreams(true, null);
    localReplicaOut = streams.getDataOut();
    localReplicaInfo.setNumBytes(0);
  }

  private void finalizeCachedReplica() throws IOException {
    if (!cacheDataLocally || localReplicaInfo == null) {
      // data wasn't cached locally, so nothing to clean up.
      return;
    }

    LOG.debug(
        "Finalizing read-through data for block " + blockToCache.getBlockId());
    try {
      if (seekOffset > 0) {
        throw new IOException(
            "Caching unsupported for seekOffset > 0 for replica "
                + localReplicaInfo.getReplicaInfo());
      }
      if (totalBytesRead < maxBytesReadable - seekOffset) {
        copyRemainingData();
      }
      // create metadata for the block
      ReplicaInfo remoteReplica = dataset.getReplicaInfo(blockToCache);
      InputStream metaIn = remoteReplica.getMetadataInputStream(0);
      OutputStream metaOut = new FileOutputStream(
          new File(localReplicaInfo.getReplicaInfo().getMetadataURI()), false);
      // streams closed by the copyBytes.
      IOUtils.copyBytes(metaIn, metaOut, remoteReplica.getMetadataLength(),
          true);
      inputStream.close();
      remoteIn.close();
      localReplicaOut.close();
      localReplicaInfo.setNumBytes(maxBytesReadable);
      // finalize the block and notify NN.
      dataset.finalizeReplica(blockToCache.getBlockPoolId(),
          localReplicaInfo.getReplicaInfo());
      dataset.notifyNamenodeForNewReplica(localReplicaInfo.getReplicaInfo(),
          blockToCache.getBlockPoolId());
    } catch (IOException e) {
      LOG.warn(
          "Cleaning up cached data for block " + blockToCache + "; Got " + e);
      cleanUp();
      throw e;
    }
  }

  /**
   * Copies the data that belong to the PROVIDED replica but was not read by the
   * reader.
   * 
   * @throws IOException
   */
  private void copyRemainingData() throws IOException {
    LOG.debug("Only partial data was read for block " + blockToCache
        + "; reading rest of the data.");
    // read remaining data and cache
    remoteIn.seek(fileOffset + seekOffset + totalBytesRead);
    IOUtils.copyBytes(remoteIn, localReplicaOut,
        maxBytesReadable - seekOffset - totalBytesRead, false);
    localReplicaInfo.setNumBytes(blockToCache.getNumBytes());
  }

  /**
   * Cleans up any state that was created on the locally chosen volume.
   */
  private void cleanUp() {
    // release bytes reserved and delete the block data.
    if (localReplicaInfo != null) {
      LOG.debug("cleaning up cached data for replica " + localReplicaInfo);
      localReplicaInfo.releaseAllBytesReserved();
      dataset.delBlockFromDisk(localReplicaInfo.getReplicaInfo());
      localReplicaInfo = null;
      try {
        localReplicaOut.close();
      } catch (IOException e) {
        LOG.warn("Exception in closing stream to the local replica: " + e);
      } finally {
        localReplicaOut = null;
      }
    }
  }

  /**
   * closes all the relevant streams.
   */
  private void closeStreams() {
    IOUtils.closeStream(localVolumeReference);
    IOUtils.closeStream(inputStream);
    IOUtils.closeStream(remoteIn);
    IOUtils.closeStream(localReplicaOut);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void run() {
    // do nothing!! copying is done synchronously!
  }

  @Override
  public int read() throws IOException {
    byte[] dataByte = new byte[1];
    int ret = this.read(dataByte, 0, 1);
    if (ret > 0) {
      return dataByte[0];
    }
    return ret;
  }

  @Override
  public int read(byte[] buf) throws IOException {
    return this.read(buf, 0, buf.length);
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    try {
      int bytesRead = inputStream.read(buf, off, len);
      if (cacheDataLocally && bytesRead > 0 && localReplicaInfo != null) {
        // the data is written out sequentially as it is read sequentially.
        localReplicaOut.write(buf, off, bytesRead);
        totalBytesRead += bytesRead;
      }
      return bytesRead;
    } catch (IOException e) {
      LOG.error("Exception in reading data for " + blockToCache
          + ". Cleaning up cached data");
      cleanUp();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    // set the flag; the stream will be closed by
    // the thread executing this Runnable.
    try {
      finalizeCachedReplica();
    } finally {
      closeStreams();
    }
  }

  @Override
  public String toString() {
    if (blockToCache != null) {
      return "SynchronousReadThroughInputStream: Caching block: "
          + blockToCache.getBlockId() + " bpid: "
          + blockToCache.getBlockPoolId();
    } else {
      return "SynchronousReadThroughInputStream";
    }
  }
}