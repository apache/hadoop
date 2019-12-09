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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

/**
 * Resolver mapping all files to a configurable, uniform blocksize
 * and replication.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FixedBlockMultiReplicaResolver extends FixedBlockResolver {

  public static final String REPLICATION =
      "hdfs.image.writer.resolver.fixed.block.replication";

  private int replication;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    replication = conf.getInt(REPLICATION, 1);
  }

  public int getReplication(FileStatus s) {
    return replication;
  }

}
