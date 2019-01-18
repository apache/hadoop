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

package org.apache.hadoop.fs.s3a.commit.terasort;

import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.mapred.JobConf;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_COMMITTER_ENABLED;

public class ITestMagicTerasort extends AbstractCommitTerasortIT {

  @Override
  protected String committerName() {
    return MagicS3GuardCommitter.NAME;
  }

  /**
   * Turn on the magic commit support for the FS, else nothing will work.
   * @param conf configuration
   */
  @Override
  protected void applyCustomConfigOptions(JobConf conf) {
    conf.setBoolean(MAGIC_COMMITTER_ENABLED, true);
  }

}
