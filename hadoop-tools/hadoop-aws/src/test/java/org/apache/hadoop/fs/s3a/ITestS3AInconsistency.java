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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.concurrent.Callable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.InconsistentAmazonS3Client.*;

/**
 * Tests S3A behavior under forced inconsistency via {@link
 * InconsistentAmazonS3Client}.
 *
 * These tests are for validating expected behavior *without* S3Guard, but
 * may also run with S3Guard enabled.  For tests that validate S3Guard's
 * consistency features, see {@link ITestS3GuardListConsistency}.
 */
public class ITestS3AInconsistency extends AbstractS3ATestBase {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    conf.setClass(S3_CLIENT_FACTORY_IMPL, InconsistentS3ClientFactory.class,
        S3ClientFactory.class);
    conf.set(FAIL_INJECT_INCONSISTENCY_KEY, DEFAULT_DELAY_KEY_SUBSTRING);
    conf.setFloat(FAIL_INJECT_INCONSISTENCY_PROBABILITY, 1.0f);
    conf.setLong(FAIL_INJECT_INCONSISTENCY_MSEC, DEFAULT_DELAY_KEY_MSEC);
    return new S3AContract(conf);
  }

  @Test
  public void testGetFileStatus() throws Exception {
    S3AFileSystem fs = getFileSystem();

    // 1. Make sure no ancestor dirs exist
    Path dir = path("ancestor");
    fs.delete(dir, true);
    waitUntilDeleted(dir);

    // 2. Create a descendant file, which implicitly creates ancestors
    // This file has delayed visibility.
    touch(getFileSystem(),
        path("ancestor/file-" + DEFAULT_DELAY_KEY_SUBSTRING));

    // 3. Assert expected behavior.  If S3Guard is enabled, we should be able
    // to get status for ancestor.  If S3Guard is *not* enabled, S3A will
    // fail to infer the existence of the ancestor since visibility of the
    // child file is delayed, and its key prefix search will return nothing.
    try {
      FileStatus status = fs.getFileStatus(dir);
      if (fs.hasMetadataStore()) {
        assertTrue("Ancestor is dir", status.isDirectory());
      } else {
        fail("getFileStatus should fail due to delayed visibility.");
      }
    } catch (FileNotFoundException e) {
      if (fs.hasMetadataStore()) {
        fail("S3Guard failed to list parent of inconsistent child.");
      }
      LOG.info("File not found, as expected.");
    }
  }

  private void waitUntilDeleted(final Path p) throws Exception {
    LambdaTestUtils.eventually(30 * 1000, 1000,
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            assertPathDoesNotExist("Dir should be deleted", p);
            return null;
          }
        }
    );
  }
}
