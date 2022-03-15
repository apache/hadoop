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
package org.apache.hadoop.hdfs.server.common.blockaliasmap.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocolPB.InMemoryAliasMapProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * InMemoryLevelDBAliasMapClient is the client for the InMemoryAliasMapServer.
 * This is used by the Datanode and fs2img to store and retrieve FileRegions
 * based on the given Block.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryLevelDBAliasMapClient extends BlockAliasMap<FileRegion>
    implements Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(InMemoryLevelDBAliasMapClient.class);
  private Configuration conf;
  private Collection<InMemoryAliasMapProtocol> aliasMaps;

  @Override
  public void close() {
    if (aliasMaps != null) {
      for (InMemoryAliasMapProtocol aliasMap : aliasMaps) {
        RPC.stopProxy(aliasMap);
      }
    }
  }

  class LevelDbReader extends BlockAliasMap.Reader<FileRegion> {

    private InMemoryAliasMapProtocol aliasMap;

    LevelDbReader(InMemoryAliasMapProtocol aliasMap) {
      this.aliasMap = aliasMap;
    }

    @Override
    public Optional<FileRegion> resolve(Block block) throws IOException {
      Optional<ProvidedStorageLocation> read = aliasMap.read(block);
      return read.map(psl -> new FileRegion(block, psl));
    }

    @Override
    public void close() throws IOException {
    }

    private class LevelDbIterator
        extends BlockAliasMap<FileRegion>.ImmutableIterator {

      private Iterator<FileRegion> iterator;
      private Optional<Block> nextMarker;

      LevelDbIterator()  {
        batch(Optional.empty());
      }

      private void batch(Optional<Block> newNextMarker) {
        try {
          InMemoryAliasMap.IterationResult iterationResult =
              aliasMap.list(newNextMarker);
          List<FileRegion> fileRegions = iterationResult.getFileRegions();
          this.iterator = fileRegions.iterator();
          this.nextMarker = iterationResult.getNextBlock();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext() || nextMarker.isPresent();
      }

      @Override
      public FileRegion next() {
        if (iterator.hasNext()) {
          return iterator.next();
        } else {
          if (nextMarker.isPresent()) {
            batch(nextMarker);
            return next();
          } else {
            throw new NoSuchElementException();
          }
        }
      }
    }

    @Override
    public Iterator<FileRegion> iterator() {
      return new LevelDbIterator();
    }
  }

  static class LevelDbWriter extends BlockAliasMap.Writer<FileRegion> {

    private InMemoryAliasMapProtocol aliasMap;

    LevelDbWriter(InMemoryAliasMapProtocol aliasMap) {
      this.aliasMap = aliasMap;
    }

    @Override
    public void store(FileRegion fileRegion) throws IOException {
      aliasMap.write(fileRegion.getBlock(),
          fileRegion.getProvidedStorageLocation());
    }

    @Override
    public void close() throws IOException {
    }
  }

  InMemoryLevelDBAliasMapClient() {
    aliasMaps = new ArrayList<>();
  }

  private InMemoryAliasMapProtocol getAliasMap(String blockPoolID)
      throws IOException {
    if (blockPoolID == null) {
      throw new IOException("Block pool id required to get aliasmap reader");
    }
    // if a block pool id has been supplied, and doesn't match the associated
    // block pool ids, return null.
    for (InMemoryAliasMapProtocol aliasMap : aliasMaps) {
      try {
        String aliasMapBlockPoolId = aliasMap.getBlockPoolId();
        if (aliasMapBlockPoolId != null &&
            aliasMapBlockPoolId.equals(blockPoolID)) {
          return aliasMap;
        }
      } catch (IOException e) {
        LOG.error("Exception in retrieving block pool id {}", e);
      }
    }
    throw new IOException(
        "Unable to retrieve InMemoryAliasMap for block pool id " + blockPoolID);
  }

  @Override
  public Reader<FileRegion> getReader(Reader.Options opts, String blockPoolID)
      throws IOException {
    InMemoryAliasMapProtocol aliasMap = getAliasMap(blockPoolID);
    LOG.info("Loading InMemoryAliasMapReader for block pool id {}",
        blockPoolID);
    return new LevelDbReader(aliasMap);
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts, String blockPoolID)
      throws IOException {
    InMemoryAliasMapProtocol aliasMap = getAliasMap(blockPoolID);
    LOG.info("Loading InMemoryAliasMapWriter for block pool id {}",
        blockPoolID);
    return new LevelDbWriter(aliasMap);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    aliasMaps = InMemoryAliasMapProtocolClientSideTranslatorPB.init(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void refresh() throws IOException {
  }
}
