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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Information that describes a journal
 */
@InterfaceAudience.Private
public class JournalInfo {
  private final int layoutVersion;
  private final String clusterId;
  private final int namespaceId;

  public JournalInfo(int lv, String clusterId, int nsId) {
    this.layoutVersion = lv;
    this.clusterId = clusterId;
    this.namespaceId = nsId;
  }

  public int getLayoutVersion() {
    return layoutVersion;
  }

  public String getClusterId() {
    return clusterId;
  }

  public int getNamespaceId() {
    return namespaceId;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("lv=").append(layoutVersion).append(";cid=").append(clusterId)
    .append(";nsid=").append(namespaceId);
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    JournalInfo jInfo;
    if (!(o instanceof JournalInfo)) {
      return false;
    }
    jInfo = (JournalInfo) o;
    return ((jInfo.clusterId.equals(this.clusterId))
        && (jInfo.namespaceId == this.namespaceId)
        && (jInfo.layoutVersion == this.layoutVersion));
  }
  
  @Override
  public int hashCode() {
    return (namespaceId ^ layoutVersion ^ clusterId.hashCode());
  }
}
