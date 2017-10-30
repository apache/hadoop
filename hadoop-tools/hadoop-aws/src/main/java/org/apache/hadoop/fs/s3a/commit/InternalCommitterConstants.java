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

package org.apache.hadoop.fs.s3a.commit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.PartitionedStagingCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterFactory;

/**
 * These are internal constants not intended for public use.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InternalCommitterConstants {
  /**
   * This is the staging committer base class; only used for testing.
   */
  @InterfaceStability.Unstable
  @InterfaceAudience.Private
  public static final String COMMITTER_NAME_STAGING = "staging";
  /**
   * A unique identifier to use for this work: {@value}.
   */
  public static final String FS_S3A_COMMITTER_STAGING_UUID =
      "fs.s3a.committer.staging.uuid";

  /**
   * Directory committer factory: {@value}.
   */
  public static final String STAGING_COMMITTER_FACTORY =
      StagingCommitterFactory.CLASSNAME;

  /**
   * Directory committer factory: {@value}.
   */
  public static final String DIRECTORY_COMMITTER_FACTORY =
      DirectoryStagingCommitterFactory.CLASSNAME;

  /**
   * Partitioned committer factory: {@value}.
   */
  public static final String PARTITION_COMMITTER_FACTORY =
      PartitionedStagingCommitterFactory.CLASSNAME;

  /**
   * Magic committer factory: {@value}.
   */
  public static final String MAGIC_COMMITTER_FACTORY =
      MagicS3GuardCommitterFactory.CLASSNAME;
}
