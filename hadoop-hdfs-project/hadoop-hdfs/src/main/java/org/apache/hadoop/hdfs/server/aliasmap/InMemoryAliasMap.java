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
package org.apache.hadoop.hdfs.server.aliasmap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ProvidedStorageLocationProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.namenode.ImageServlet;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

/**
 * InMemoryAliasMap is an implementation of the InMemoryAliasMapProtocol for
 * use with LevelDB.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class InMemoryAliasMap implements InMemoryAliasMapProtocol,
    Configurable {

  private static final Logger LOG = LoggerFactory
      .getLogger(InMemoryAliasMap.class);

  private static final String SNAPSHOT_COPY_DIR = "aliasmap_snapshot";
  private static final String TAR_NAME = "aliasmap.tar.gz";
  private final URI aliasMapURI;
  private final DB levelDb;
  private Configuration conf;
  private String blockPoolID;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  public static @Nonnull InMemoryAliasMap init(Configuration conf,
      String blockPoolID) throws IOException {
    Options options = new Options();
    options.createIfMissing(true);
    String directory =
        conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR);
    if (directory == null) {
      throw new IOException("InMemoryAliasMap location is null");
    }
    File levelDBpath;
    if (blockPoolID != null) {
      levelDBpath = new File(directory, blockPoolID);
    } else {
      levelDBpath = new File(directory);
    }
    if (!levelDBpath.exists()) {
      LOG.warn("InMemoryAliasMap location {} is missing. Creating it.",
          levelDBpath);
      if(!levelDBpath.mkdirs()) {
        throw new IOException(
            "Unable to create missing aliasmap location: " + levelDBpath);
      }
    }
    DB levelDb = JniDBFactory.factory.open(levelDBpath, options);
    InMemoryAliasMap aliasMap =  new InMemoryAliasMap(levelDBpath.toURI(),
        levelDb, blockPoolID);
    aliasMap.setConf(conf);
    return aliasMap;
  }

  @VisibleForTesting
  InMemoryAliasMap(URI aliasMapURI, DB levelDb, String blockPoolID) {
    this.aliasMapURI = aliasMapURI;
    this.levelDb = levelDb;
    this.blockPoolID = blockPoolID;
  }

  @Override
  public IterationResult list(Optional<Block> marker) throws IOException {
    try (DBIterator iterator = levelDb.iterator()) {
      Integer batchSize =
          conf.getInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_BATCH_SIZE,
              DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_BATCH_SIZE_DEFAULT);
      if (marker.isPresent()) {
        iterator.seek(toProtoBufBytes(marker.get()));
      } else {
        iterator.seekToFirst();
      }
      int i = 0;
      ArrayList<FileRegion> batch =
          Lists.newArrayListWithExpectedSize(batchSize);
      while (iterator.hasNext() && i < batchSize) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        Block block = fromBlockBytes(entry.getKey());
        ProvidedStorageLocation providedStorageLocation =
            fromProvidedStorageLocationBytes(entry.getValue());
        batch.add(new FileRegion(block, providedStorageLocation));
        ++i;
      }
      if (iterator.hasNext()) {
        Block nextMarker = fromBlockBytes(iterator.next().getKey());
        return new IterationResult(batch, Optional.of(nextMarker));
      } else {
        return new IterationResult(batch, Optional.empty());
      }
    }
  }

  public @Nonnull Optional<ProvidedStorageLocation> read(@Nonnull Block block)
      throws IOException {

    byte[] extendedBlockDbFormat = toProtoBufBytes(block);
    byte[] providedStorageLocationDbFormat = levelDb.get(extendedBlockDbFormat);
    if (providedStorageLocationDbFormat == null) {
      return Optional.empty();
    } else {
      ProvidedStorageLocation providedStorageLocation =
          fromProvidedStorageLocationBytes(providedStorageLocationDbFormat);
      return Optional.of(providedStorageLocation);
    }
  }

  public void write(@Nonnull Block block,
      @Nonnull ProvidedStorageLocation providedStorageLocation)
      throws IOException {
    byte[] extendedBlockDbFormat = toProtoBufBytes(block);
    byte[] providedStorageLocationDbFormat =
        toProtoBufBytes(providedStorageLocation);
    levelDb.put(extendedBlockDbFormat, providedStorageLocationDbFormat);
  }

  @Override
  public String getBlockPoolId() {
    return blockPoolID;
  }

  public void close() throws IOException {
    levelDb.close();
  }

  @Nonnull
  public static ProvidedStorageLocation fromProvidedStorageLocationBytes(
      @Nonnull byte[] providedStorageLocationDbFormat)
      throws InvalidProtocolBufferException {
    ProvidedStorageLocationProto providedStorageLocationProto =
        ProvidedStorageLocationProto
            .parseFrom(providedStorageLocationDbFormat);
    return PBHelperClient.convert(providedStorageLocationProto);
  }

  @Nonnull
  public static Block fromBlockBytes(@Nonnull byte[] blockDbFormat)
      throws InvalidProtocolBufferException {
    BlockProto blockProto = BlockProto.parseFrom(blockDbFormat);
    return PBHelperClient.convert(blockProto);
  }

  public static byte[] toProtoBufBytes(@Nonnull ProvidedStorageLocation
      providedStorageLocation) throws IOException {
    ProvidedStorageLocationProto providedStorageLocationProto =
        PBHelperClient.convert(providedStorageLocation);
    ByteArrayOutputStream providedStorageLocationOutputStream =
        new ByteArrayOutputStream();
    providedStorageLocationProto.writeTo(providedStorageLocationOutputStream);
    return providedStorageLocationOutputStream.toByteArray();
  }

  public static byte[] toProtoBufBytes(@Nonnull Block block)
      throws IOException {
    BlockProto blockProto =
        PBHelperClient.convert(block);
    ByteArrayOutputStream blockOutputStream = new ByteArrayOutputStream();
    blockProto.writeTo(blockOutputStream);
    return blockOutputStream.toByteArray();
  }

  /**
   * Transfer this aliasmap for bootstrapping standby Namenodes. The map is
   * transferred as a tar.gz archive. This archive needs to be extracted on the
   * standby Namenode.
   *
   * @param response http response.
   * @param conf configuration to use.
   * @param aliasMap aliasmap to transfer.
   * @throws IOException
   */
  public static void transferForBootstrap(HttpServletResponse response,
      Configuration conf, InMemoryAliasMap aliasMap) throws IOException {
    File aliasMapSnapshot = null;
    File compressedAliasMap = null;
    try {
      aliasMapSnapshot = createSnapshot(aliasMap);
      // compress the snapshot that is associated with the
      // block pool id of the aliasmap.
      compressedAliasMap = getCompressedAliasMap(
          new File(aliasMapSnapshot, aliasMap.blockPoolID));
      try (FileInputStream fis = new FileInputStream(compressedAliasMap)) {
        ImageServlet.setVerificationHeadersForGet(response, compressedAliasMap);
        ImageServlet.setFileNameHeaders(response, compressedAliasMap);
        // send file
        DataTransferThrottler throttler =
            ImageServlet.getThrottlerForBootstrapStandby(conf);
        TransferFsImage.copyFileToStream(response.getOutputStream(),
            compressedAliasMap, fis, throttler);
      }
    } finally {
      // cleanup the temporary snapshot and compressed files.
      StringBuilder errMessage = new StringBuilder();
      if (compressedAliasMap != null
          && !FileUtil.fullyDelete(compressedAliasMap)) {
        errMessage.append("Failed to fully delete compressed aliasmap ")
            .append(compressedAliasMap.getAbsolutePath()).append("\n");
      }
      if (aliasMapSnapshot != null && !FileUtil.fullyDelete(aliasMapSnapshot)) {
        errMessage.append("Failed to fully delete the aliasmap snapshot ")
            .append(aliasMapSnapshot.getAbsolutePath()).append("\n");
      }
      if (errMessage.length() > 0) {
        throw new IOException(errMessage.toString());
      }
    }
  }

  /**
   * Create a new LevelDB store which is a snapshot copy of the original
   * aliasmap.
   *
   * @param aliasMap original aliasmap.
   * @return the {@link File} where the snapshot is created.
   * @throws IOException
   */
  static File createSnapshot(InMemoryAliasMap aliasMap) throws IOException {
    File originalAliasMapDir = new File(aliasMap.aliasMapURI);
    String bpid = originalAliasMapDir.getName();
    File snapshotDir =
        new File(originalAliasMapDir.getParent(), SNAPSHOT_COPY_DIR);
    File newLevelDBDir = new File(snapshotDir, bpid);
    if (!newLevelDBDir.mkdirs()) {
      throw new IOException(
          "Unable to create aliasmap snapshot directory " + newLevelDBDir);
    }
    // get a snapshot for the original DB.
    DB originalDB = aliasMap.levelDb;
    try (Snapshot snapshot = originalDB.getSnapshot()) {
      // create a new DB for the snapshot and copy all K,V pairs.
      Options options = new Options();
      options.createIfMissing(true);
      try (DB snapshotDB = JniDBFactory.factory.open(newLevelDBDir, options)) {
        try (DBIterator iterator =
            originalDB.iterator(new ReadOptions().snapshot(snapshot))) {
          iterator.seekToFirst();
          while (iterator.hasNext()) {
            Map.Entry<byte[], byte[]> entry = iterator.next();
            snapshotDB.put(entry.getKey(), entry.getValue());
          }
        }
      }
    }

    return snapshotDir;
  }

  /**
   * Compress the given aliasmap directory as tar.gz.
   *
   * @return a reference to the compressed aliasmap.
   * @throws IOException
   */
  private static File getCompressedAliasMap(File aliasMapDir)
      throws IOException {
    File outCompressedFile = new File(aliasMapDir.getParent(), TAR_NAME);
    BufferedOutputStream bOut = null;
    GzipCompressorOutputStream gzOut = null;
    TarArchiveOutputStream tOut = null;
    try {
      bOut = new BufferedOutputStream(new FileOutputStream(outCompressedFile));
      gzOut = new GzipCompressorOutputStream(bOut);
      tOut = new TarArchiveOutputStream(gzOut);
      addFileToTarGzRecursively(tOut, aliasMapDir, "", new Configuration());
    } finally {
      if (tOut != null) {
        tOut.finish();
      }
      IOUtils.cleanupWithLogger(null, tOut, gzOut, bOut);
    }
    return outCompressedFile;
  }

  /**
   * Add all contents of the given file to the archive.
   *
   * @param tOut archive to use.
   * @param file file to archive.
   * @param prefix path prefix.
   * @throws IOException
   */
  private static void addFileToTarGzRecursively(TarArchiveOutputStream tOut,
      File file, String prefix, Configuration conf) throws IOException {
    String entryName = prefix + file.getName();
    TarArchiveEntry tarEntry = new TarArchiveEntry(file, entryName);
    tOut.putArchiveEntry(tarEntry);

    LOG.debug("Adding entry {} to alias map archive", entryName);
    if (file.isFile()) {
      try (FileInputStream in = new FileInputStream(file)) {
        IOUtils.copyBytes(in, tOut, conf, false);
      }
      tOut.closeArchiveEntry();
    } else {
      tOut.closeArchiveEntry();
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          // skip the LOCK file
          if (!child.getName().equals("LOCK")) {
            addFileToTarGzRecursively(tOut, child, entryName + "/", conf);
          }
        }
      }
    }
  }

  /**
   * Extract the aliasmap archive to complete the bootstrap process. This method
   * has to be called after the aliasmap archive is transfered from the primary
   * Namenode.
   *
   * @param aliasMap location of the aliasmap.
   * @throws IOException
   */
  public static void completeBootstrapTransfer(File aliasMap)
      throws IOException {
    File tarname = new File(aliasMap, TAR_NAME);
    if (!tarname.exists()) {
      throw new IOException(
          "Aliasmap archive (" + tarname + ") does not exist");
    }
    try {
      FileUtil.unTar(tarname, aliasMap);
    } finally {
      // delete the archive.
      if(!FileUtil.fullyDelete(tarname)) {
        LOG.warn("Failed to fully delete aliasmap archive: " + tarname);
      }
    }
  }

  /**
   * CheckedFunction is akin to {@link java.util.function.Function} but
   * specifies an IOException.
   * @param <T1> First argument type.
   * @param <T2> Second argument type.
   * @param <R> Return type.
   */
  @FunctionalInterface
  public interface CheckedFunction2<T1, T2, R> {
    R apply(T1 t1, T2 t2) throws IOException;
  }
}
