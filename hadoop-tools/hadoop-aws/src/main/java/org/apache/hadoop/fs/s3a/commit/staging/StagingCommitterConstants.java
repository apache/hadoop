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

package org.apache.hadoop.fs.s3a.commit.staging;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Internal staging committer constants.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class StagingCommitterConstants {

  private StagingCommitterConstants() {
  }

  /**
   * The temporary path for staging data, if not explicitly set.
   * By using an unqualified path, this will be qualified to be relative
   * to the users' home directory, so protectec from access for others.
   */
  public static final String FILESYSTEM_TEMP_PATH = "tmp/staging";

  /** Name of the root partition :{@value}. */
  public static final String TABLE_ROOT = "table_root";

  /**
   * Filename used under {@code ~/${UUID}} for the staging files.
   */
  public static final String STAGING_UPLOADS = "staging-uploads";

  // Spark configuration keys

  /**
   * The UUID for jobs: {@value}.
   */
  public static final String SPARK_WRITE_UUID =
      "spark.sql.sources.writeJobUUID";

  /**
   * The App ID for jobs.
   */

  public static final String SPARK_APP_ID = "spark.app.id";

  public static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
}
