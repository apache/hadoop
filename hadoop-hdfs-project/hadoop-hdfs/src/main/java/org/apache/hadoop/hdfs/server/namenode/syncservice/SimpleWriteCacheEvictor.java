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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evict blocks on HDFS once data is synced to external storage, applicable to
 * provided storage write back mount.
 */
public class SimpleWriteCacheEvictor extends WriteCacheEvictor {
  private static final Logger LOG =
      LoggerFactory.getLogger(SimpleWriteCacheEvictor.class);

  public SimpleWriteCacheEvictor(Configuration conf,
      FSNamesystem fsNamesystem) {
    super(conf, fsNamesystem);
  }

  @Override
  public void evict() {
    while (!evictQueue.isEmpty()) {
      Long blockCollectionId = evictQueue.poll();
      INodeFile file = fsNamesystem.getBlockCollection(blockCollectionId);
      LOG.info("Trying to remove local blocks for " + file.getFullPathName());
      removeLocalBlocks(file.getBlocks());
    }
  }
}
