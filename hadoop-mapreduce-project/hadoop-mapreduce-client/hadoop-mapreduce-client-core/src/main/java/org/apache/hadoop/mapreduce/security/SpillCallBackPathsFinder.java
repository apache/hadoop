/*
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
package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoStreamUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.CryptoUtils;

/**
 * An implementation class that keeps track of the spilled files.
 */
public class SpillCallBackPathsFinder extends SpillCallBackInjector {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpillCallBackPathsFinder.class);
  /**
   * Encrypted spilled files.
   */
  private final Map<Path, Set<Long>> encryptedSpillFiles =
      Collections.synchronizedMap(new ConcurrentHashMap<>());
  /**
   * Non-Encrypted spilled files.
   */
  private final Map<Path, Set<Long>> spillFiles =
      Collections.synchronizedMap(new ConcurrentHashMap<>());
  /**
   * Invalid position access.
   */
  private final Map<Path, Set<Long>> invalidAccessMap =
      Collections.synchronizedMap(new ConcurrentHashMap<>());
  /**
   * Index spill files.
   */
  private final Set<Path> indexSpillFiles = ConcurrentHashMap.newKeySet();
  /**
   * Paths that were not found in the maps.
   */
  private final Set<Path> negativeCache = ConcurrentHashMap.newKeySet();

  protected Map<Path, Set<Long>> getFilesMap(Configuration config) {
    if (CryptoUtils.isEncryptedSpillEnabled(config)) {
      return encryptedSpillFiles;
    }
    return spillFiles;
  }

  @Override
  public void writeSpillFileCB(Path path, FSDataOutputStream out,
      Configuration conf) {
    long outPos = out.getPos();
    getFilesMap(conf)
        .computeIfAbsent(path, p -> ConcurrentHashMap.newKeySet())
        .add(outPos);
    LOG.debug("writeSpillFileCB.. path:{}; pos:{}", path, outPos);
  }

  @Override
  public void getSpillFileCB(Path path, InputStream is, Configuration conf) {
    if (path == null) {
      return;
    }
    Set<Long> pathEntries = getFilesMap(conf).get(path);
    if (pathEntries != null) {
      try {
        long isPos = CryptoStreamUtils.getInputStreamOffset(is);
        if (pathEntries.contains(isPos)) {
          LOG.debug("getSpillFileCB... Path {}; Pos: {}", path, isPos);
          return;
        }
        invalidAccessMap
            .computeIfAbsent(path, p -> ConcurrentHashMap.newKeySet())
            .add(isPos);
        LOG.debug("getSpillFileCB... access incorrect position.. "
            + "Path {}; Pos: {}", path, isPos);
      } catch (IOException e) {
        LOG.error("Could not get inputStream position.. Path {}", path, e);
        // do nothing
      }
      return;
    }
    negativeCache.add(path);
    LOG.warn("getSpillFileCB.. Could not find spilled file .. Path: {}", path);
  }

  @Override
  public String getSpilledFileReport() {
    StringBuilder strBuilder =
        new StringBuilder("\n++++++++ Spill Report ++++++++")
            .append(dumpMapEntries("Encrypted Spilled Files",
                encryptedSpillFiles))
            .append(dumpMapEntries("Non-Encrypted Spilled Files",
                spillFiles))
            .append(dumpMapEntries("Invalid Spill Access",
                invalidAccessMap))
            .append("\n ----- Spilled Index Files ----- ")
            .append(indexSpillFiles.size());
    for (Path p : indexSpillFiles) {
      strBuilder.append("\n\t index-path: ").append(p.toString());
    }
    strBuilder.append("\n ----- Negative Cache files ----- ")
        .append(negativeCache.size());
    for (Path p : negativeCache) {
      strBuilder.append("\n\t path: ").append(p.toString());
    }
    return strBuilder.toString();
  }

  @Override
  public void addSpillIndexFileCB(Path path, Configuration conf) {
    if (path == null) {
      return;
    }
    indexSpillFiles.add(path);
    LOG.debug("addSpillIndexFileCB... Path: {}", path);
  }

  @Override
  public void validateSpillIndexFileCB(Path path, Configuration conf) {
    if (path == null) {
      return;
    }
    if (indexSpillFiles.contains(path)) {
      LOG.debug("validateSpillIndexFileCB.. Path: {}", path);
      return;
    }
    LOG.warn("validateSpillIndexFileCB.. could not retrieve indexFile.. "
        + "Path: {}", path);
    negativeCache.add(path);
  }

  public Set<Path> getEncryptedSpilledFiles() {
    return Collections.unmodifiableSet(encryptedSpillFiles.keySet());
  }

  /**
   * Gets the set of path:pos of the entries that were accessed incorrectly.
   * @return a set of string in the format of {@literal Path[Pos]}
   */
  public Set<String> getInvalidSpillEntries() {
    Set<String> result = new LinkedHashSet<>();
    for (Entry<Path, Set<Long>> spillMapEntry: invalidAccessMap.entrySet()) {
      for (Long singleEntry : spillMapEntry.getValue()) {
        result.add(String.format("%s[%d]",
            spillMapEntry.getKey(), singleEntry));
      }
    }
    return result;
  }

  private String dumpMapEntries(String label,
      Map<Path, Set<Long>> entriesMap) {
    StringBuilder strBuilder =
        new StringBuilder(String.format("%n ----- %s ----- %d", label,
            entriesMap.size()));
    for (Entry<Path, Set<Long>> encryptedSpillEntry
        : entriesMap.entrySet()) {
      strBuilder.append(String.format("%n\t\tpath: %s",
          encryptedSpillEntry.getKey()));
      for (Long singlePos : encryptedSpillEntry.getValue()) {
        strBuilder.append(String.format("%n\t\t\tentry: %d", singlePos));
      }
    }
    return strBuilder.toString();
  }
}
