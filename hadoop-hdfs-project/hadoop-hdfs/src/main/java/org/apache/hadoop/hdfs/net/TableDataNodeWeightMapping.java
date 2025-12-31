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
package org.apache.hadoop.hdfs.net;

import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NET_TOPOLOGY_WEIGHT_TABLE_FILE_NAME_KEY;

/**
 * Simple {@link DNSToWeightMapping} implementation that reads a 2 column text
 * file. The columns are separated by whitespace. The first column is a DNS or
 * IP address and the second column specifies the weight where the address maps.
 * <p>
 * This class uses the configuration parameter {@code
 *  dfs.net.topology.weight.table.file.name} to locate the mapping file.
 * </p>
 * <p>
 * If no entry corresponding to the address is found, the default weight
 * {@code 1} is returned.
 * </p>
 */
public class TableDataNodeWeightMapping extends AbstractDataNodeWeightMapping {

  private static final Logger LOG = LoggerFactory.getLogger(TableDataNodeWeightMapping.class);
  private static final char COMMENT_CHARACTER = '#';

  private Map<String, Integer> weightMap;

  private Map<String, Integer> loadFromFile() {
    Map<String, Integer> loadMap = new HashMap<>();

    String filename = getConf().get(DFS_NET_TOPOLOGY_WEIGHT_TABLE_FILE_NAME_KEY, null);
    if (StringUtils.isBlank(filename)) {
      LOG.warn(DFS_NET_TOPOLOGY_WEIGHT_TABLE_FILE_NAME_KEY + " not configured.");
      return null;
    }

    try (LineIterator it =
             FileUtils.lineIterator(new File(filename), StandardCharsets.UTF_8.name())) {
      while (it.hasNext()) {
        String line = it.next().trim();
        if (line.isEmpty() || line.charAt(0) == COMMENT_CHARACTER) {
          continue;
        }
        String[] columns = line.split("\\s+");
        if (columns.length == 2) {
          Integer weight = Ints.tryParse(columns[1]);
          if (weight != null && weight > 0) {
            loadMap.put(columns[0], weight);
          } else {
            LOG.warn("Invalid weight value. Ignoring. {}", line);
          }
        } else {
          LOG.warn("Line does not have two columns. Ignoring. {}", line);
        }
      }
    } catch (Exception e) {
      LOG.warn("{} cannot be read.", filename, e);
      return null;
    }
    return loadMap;
  }

  @Override
  public synchronized int resolve(String ipAddress, String hostName) {
    loadIfNeeded();
    // use ip address to resolve weight, hostName is ignored
    return weightMap.getOrDefault(ipAddress, DEFAULT_WEIGHT);
  }

  private void loadIfNeeded() {
    if (weightMap == null) {
      weightMap = loadFromFile();
      if (weightMap == null) {
        LOG.warn("Failed to read topology table. " +
            NetworkTopology.DEFAULT_RACK + " will be used for all nodes.");
        weightMap = Collections.emptyMap();
      }
    }
  }

  @Override
  public void reload() {
    Map<String, Integer> newMap = loadFromFile();
    if (newMap == null) {
      LOG.error("Failed to reload the weight table. " +
          "The cached mappings will not be cleared.");
    } else {
      synchronized (this) {
        weightMap = newMap;
      }
    }
  }

}
