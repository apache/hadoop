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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.raid.protocol.PolicyInfo;

/**
 * Implementation of {@link RaidNode} that performs raiding locally.
 */
public class LocalRaidNode extends RaidNode {

  public static final Log LOG = LogFactory.getLog(LocalRaidNode.class);

  public LocalRaidNode(Configuration conf) throws IOException {
    super(conf);

    LOG.info("created");
  }

  /**
   * {@inheritDocs}
   */
  @Override
  void raidFiles(PolicyInfo info, List<FileStatus> paths) throws IOException {
    doRaid(conf, info, paths);
  }

  /**
   * {@inheritDocs}
   */
  @Override
  int getRunningJobsForPolicy(String policyName) {
    return 0;
  }
}