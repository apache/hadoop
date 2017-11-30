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
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

/**
 * OpenFilesIterator is a remote iterator that iterates over the open files list
 * managed by the NameNode. Since the list is retrieved in batches, it does not
 * represent a consistent view of all open files.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OpenFilesIterator extends
    BatchedRemoteIterator<Long, OpenFileEntry> {
  private final ClientProtocol namenode;
  private final Tracer tracer;

  public OpenFilesIterator(ClientProtocol namenode, Tracer tracer) {
    super(HdfsConstants.GRANDFATHER_INODE_ID);
    this.namenode = namenode;
    this.tracer = tracer;
  }

  @Override
  public BatchedEntries<OpenFileEntry> makeRequest(Long prevId)
      throws IOException {
    try (TraceScope ignored = tracer.newScope("listOpenFiles")) {
      return namenode.listOpenFiles(prevId);
    }
  }

  @Override
  public Long elementToPrevKey(OpenFileEntry entry) {
    return entry.getId();
  }
}
