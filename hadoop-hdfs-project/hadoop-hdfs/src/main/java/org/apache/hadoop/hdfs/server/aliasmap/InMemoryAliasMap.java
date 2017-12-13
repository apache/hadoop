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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ProvidedStorageLocationProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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

  @VisibleForTesting
  static String createPathErrorMessage(String directory) {
    return new StringBuilder()
        .append("Configured directory '")
        .append(directory)
        .append("' doesn't exist")
        .toString();
  }

  public static @Nonnull InMemoryAliasMap init(Configuration conf,
      String blockPoolID) throws IOException {
    Options options = new Options();
    options.createIfMissing(true);
    String directory =
        conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR);
    LOG.info("Attempting to load InMemoryAliasMap from \"{}\"", directory);
    File levelDBpath;
    if (blockPoolID != null) {
      levelDBpath = new File(directory, blockPoolID);
    } else {
      levelDBpath = new File(directory);
    }
    if (!levelDBpath.exists()) {
      String error = createPathErrorMessage(directory);
      throw new IOException(error);
    }
    DB levelDb = JniDBFactory.factory.open(levelDBpath, options);
    InMemoryAliasMap aliasMap = new InMemoryAliasMap(levelDb, blockPoolID);
    aliasMap.setConf(conf);
    return aliasMap;
  }

  @VisibleForTesting
  InMemoryAliasMap(DB levelDb, String blockPoolID) {
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
