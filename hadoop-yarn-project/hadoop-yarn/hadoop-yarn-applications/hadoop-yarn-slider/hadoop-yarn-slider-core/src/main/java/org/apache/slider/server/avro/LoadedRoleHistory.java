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

package org.apache.slider.server.avro;

import org.apache.hadoop.fs.Path;
import org.apache.slider.common.tools.SliderUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The role history
 */
public class LoadedRoleHistory {

  private RoleHistoryHeader header;

  private Path path;

  public final Map<String, Integer> roleMap = new HashMap<>();

  public final List<NodeEntryRecord> records = new ArrayList<>();

  /**
   * Add a record
   * @param record
   */
  public void add(NodeEntryRecord record) {
    records.add(record);
  }

  /**
   * Number of loaded records
   * @return
   */
  public int size() {
    return records.size();
  }

  public RoleHistoryHeader getHeader() {
    return header;
  }

  public void setHeader(RoleHistoryHeader header) {
    this.header = header;
  }

  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public void buildMapping(Map<CharSequence, Integer> source) {
    roleMap.clear();
    for (Map.Entry<CharSequence, Integer> entry : source.entrySet()) {
      roleMap.put(SliderUtils.sequenceToString(entry.getKey()),
          entry.getValue());
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
      "LoadedRoleHistory{");
    sb.append("path=").append(path);
    sb.append("; number of roles=").append(roleMap.size());
    sb.append("; size=").append(size());
    sb.append('}');
    return sb.toString();
  }
}
