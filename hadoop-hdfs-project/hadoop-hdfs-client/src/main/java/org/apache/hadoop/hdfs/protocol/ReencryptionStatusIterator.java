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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.io.IOException;

/**
 * ReencryptionStatusIterator is a remote iterator that iterates over the
 * reencryption status of encryption zones.
 * It supports retrying in case of namenode failover.
 */
@InterfaceAudience.Private
public class ReencryptionStatusIterator
    extends BatchedRemoteIterator<Long, ZoneReencryptionStatus> {

  private final ClientProtocol namenode;
  private final Tracer tracer;

  public ReencryptionStatusIterator(ClientProtocol namenode, Tracer tracer) {
    super((long) 0);
    this.namenode = namenode;
    this.tracer = tracer;
  }

  @Override
  public BatchedEntries<ZoneReencryptionStatus> makeRequest(Long prevId)
      throws IOException {
    try (TraceScope ignored = tracer.newScope("listReencryptionStatus")) {
      return namenode.listReencryptionStatus(prevId);
    }
  }

  @Override
  public Long elementToPrevKey(ZoneReencryptionStatus entry) {
    return entry.getId();
  }
}
