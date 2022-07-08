/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.runS3GuardCommand;

/**
 * An extension of the contract test base set up for S3A tests.
 */
public class AbstractAuditToolTest extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractAuditToolTest.class);

  /**
   * Take a configuration, copy it and disable FS Caching on
   * the new one.
   *
   * @param conf source config
   * @return a new, patched, config
   */
  protected Configuration uncachedFSConfig(final Configuration conf) {
    Configuration c = new Configuration(conf);
    disableFilesystemCaching(c);
    return c;
  }

  /**
   * Run a S3GuardTool command from a varags list and the
   * configuration returned by {@code getConfiguration()}.
   *
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(Object... args) throws Exception {
    return runS3GuardCommand(uncachedFSConfig(getConfiguration()), args);
  }
}
