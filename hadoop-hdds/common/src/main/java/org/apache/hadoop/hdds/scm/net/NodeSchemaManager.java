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
package org.apache.hadoop.hdds.scm.net;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.net.NodeSchemaLoader.NodeSchemaLoadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** The class manages all network topology schemas. */

public final class NodeSchemaManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      NodeSchemaManager.class);

  // All schema saved and sorted from ROOT to LEAF node
  private List<NodeSchema> allSchema;
  // enforcePrefix only applies to INNER_NODE
  private boolean enforcePrefix;
  // max level, includes ROOT level
  private int maxLevel = -1;

  private volatile static NodeSchemaManager instance = null;

  private NodeSchemaManager() {
  }

  public static NodeSchemaManager getInstance() {
    if (instance == null) {
      instance = new NodeSchemaManager();
    }
    return instance;
  }

  public void init(Configuration conf) {
    /**
     * Load schemas from network topology schema configuration file
     */
    String schemaFileType = conf.get(
            ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_TYPE);

    String schemaFile = conf.get(
        ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE,
        ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_DEFAULT);

    NodeSchemaLoadResult result;
    try {
      if (schemaFileType.toLowerCase().compareTo("yaml") == 0) {
        result = NodeSchemaLoader.getInstance().loadSchemaFromYaml(schemaFile);
      } else {
        result = NodeSchemaLoader.getInstance().loadSchemaFromXml(schemaFile);
      }
      allSchema = result.getSchemaList();
      enforcePrefix = result.isEnforePrefix();
      maxLevel = allSchema.size();
    } catch (Throwable e) {
      String msg = "Fail to load schema file:" + schemaFile
          + ", error:" + e.getMessage();
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }

  @VisibleForTesting
  public void init(NodeSchema[] schemas, boolean enforce) {
    allSchema = new ArrayList<>();
    allSchema.addAll(Arrays.asList(schemas));
    enforcePrefix = enforce;
    maxLevel = schemas.length;
  }

  public int getMaxLevel() {
    return maxLevel;
  }

  public int getCost(int level) {
    Preconditions.checkArgument(level <= maxLevel &&
        level >= (NetConstants.ROOT_LEVEL));
    return allSchema.get(level - NetConstants.ROOT_LEVEL).getCost();
  }

  /**
   * Given a incomplete network path, return its complete network path if
   * possible. E.g. input is 'node1', output is '/rack-default/node1' if this
   * schema manages ROOT, RACK and LEAF, with prefix defined and enforce prefix
   * enabled.
   *
   * @param path the incomplete input path
   * @return complete path, null if cannot carry out complete action or action
   * failed
   */
  public String complete(String path) {
    if (!enforcePrefix) {
      return null;
    }
    String normalizedPath = NetUtils.normalize(path);
    String[] subPath = normalizedPath.split(NetConstants.PATH_SEPARATOR_STR);
    if ((subPath.length) == maxLevel) {
      return path;
    }
    StringBuffer newPath = new StringBuffer(NetConstants.ROOT);
    // skip the ROOT and LEAF layer
    int i, j;
    for (i = 1, j = 1; i < subPath.length && j < (allSchema.size() - 1);) {
      if (allSchema.get(j).matchPrefix(subPath[i])) {
        newPath.append(NetConstants.PATH_SEPARATOR_STR + subPath[i]);
        i++;
        j++;
      } else {
        newPath.append(allSchema.get(j).getDefaultName());
        j++;
      }
    }
    if (i == (subPath.length - 1)) {
      newPath.append(NetConstants.PATH_SEPARATOR_STR + subPath[i]);
      return newPath.toString();
    }
    return null;
  }
}
