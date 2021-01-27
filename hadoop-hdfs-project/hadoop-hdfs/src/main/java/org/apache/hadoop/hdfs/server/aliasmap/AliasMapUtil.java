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
package org.apache.hadoop.hdfs.server.aliasmap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * Utility class for AliasMap.
 */
public final class AliasMapUtil {

  private AliasMapUtil() {
  }

  public static BlockAliasMap.Writer<FileRegion> createAliasMapWriter(
      String blockPoolId, Configuration conf) {
    // load block writer into storage
    Class<? extends BlockAliasMap> aliasMapClass = conf.getClass(
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TextFileRegionAliasMap.class, BlockAliasMap.class);
    final BlockAliasMap<FileRegion> aliasMap = ReflectionUtils.newInstance(
        aliasMapClass, conf);
    try {
      return aliasMap.getWriter(null, blockPoolId);
    } catch (IOException e) {
      throw new RuntimeException("Could not load AliasMap Writer: "
          + e.getMessage());
    }
  }

  public static BlockAliasMap.Reader<FileRegion> createAliasMapReader(
      String blockPoolId, Configuration conf) {
    // load block reader into storage
    Class<? extends BlockAliasMap> aliasMapClass = conf.getClass(
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TextFileRegionAliasMap.class, BlockAliasMap.class);
    final BlockAliasMap<FileRegion> aliasMap = ReflectionUtils.newInstance(
        aliasMapClass, conf);
    try {
      return aliasMap.getReader(null, blockPoolId);
    } catch (IOException e) {
      throw new RuntimeException("Could not load AliasMap Reader: "
          + e.getMessage());
    }
  }
}
