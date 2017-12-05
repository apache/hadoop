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
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
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

  private Configuration conf;
  private InMemoryAliasMapProtocolClientSideTranslatorPB aliasMap;
  private String blockPoolID;

  @Override
  public void close() {
    aliasMap.stop();
  }

  class LevelDbReader extends BlockAliasMap.Reader<FileRegion> {

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

  class LevelDbWriter extends BlockAliasMap.Writer<FileRegion> {
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
    if (UserGroupInformation.isSecurityEnabled()) {
      throw new UnsupportedOperationException("Unable to start "
          + "InMemoryLevelDBAliasMapClient as security is enabled");
    }
  }


  @Override
  public Reader<FileRegion> getReader(Reader.Options opts, String blockPoolID)
      throws IOException {
    if (this.blockPoolID == null) {
      this.blockPoolID = aliasMap.getBlockPoolId();
    }
    // if a block pool id has been supplied, and doesn't match the associated
    // block pool id, return null.
    if (blockPoolID != null && this.blockPoolID != null
        && !this.blockPoolID.equals(blockPoolID)) {
      return null;
    }
    return new LevelDbReader();
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts, String blockPoolID)
      throws IOException {
    if (this.blockPoolID == null) {
      this.blockPoolID = aliasMap.getBlockPoolId();
    }
    if (blockPoolID != null && !this.blockPoolID.equals(blockPoolID)) {
      return null;
    }
    return new LevelDbWriter();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.aliasMap = new InMemoryAliasMapProtocolClientSideTranslatorPB(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void refresh() throws IOException {
  }

}
