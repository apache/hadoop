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

package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BatchedRemoteIterator;

/**
 * Used on the client-side to iterate over the list of encryption zones
 * stored on the namenode.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EncryptionZoneWithIdIterator
    extends BatchedRemoteIterator<Long, EncryptionZoneWithId> {

  private final ClientProtocol namenode;

  EncryptionZoneWithIdIterator(ClientProtocol namenode) {
    super(Long.valueOf(0));
    this.namenode = namenode;
  }

  @Override
  public BatchedEntries<EncryptionZoneWithId> makeRequest(Long prevId)
      throws IOException {
    return namenode.listEncryptionZones(prevId);
  }

  @Override
  public Long elementToPrevKey(EncryptionZoneWithId entry) {
    return entry.getId();
  }
}
