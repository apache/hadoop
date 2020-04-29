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

package org.apache.hadoop.fs.contract.s3a;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;

/**
 * root dir operations against an S3 bucket.
 */
public class ITestS3AContractRootDir extends
    AbstractContractRootDirectoryTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AContractRootDir.class);

  /**
   * Create a configuration, possibly patching in S3Guard options.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  public S3AFileSystem getFileSystem() {
    return (S3AFileSystem) super.getFileSystem();
  }

  @Override
  @Ignore("S3 always return false when non-recursively remove root dir")
  public void testRmNonEmptyRootDirNonRecursive() throws Throwable {
  }

  /**
   * This is overridden to allow for eventual consistency on listings,
   * but only if the store does not have S3Guard protecting it.
   */
  @Override
  public void testListEmptyRootDirectory() throws IOException {
    int maxAttempts = 10;
    describe("Listing root directory; for consistency allowing "
        + maxAttempts + " attempts");
    for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
      try {
        super.testListEmptyRootDirectory();
        break;
      } catch (AssertionError | FileNotFoundException e) {
        if (attempt < maxAttempts) {
          LOG.info("Attempt {} of {} for empty root directory test failed.  "
              + "This is likely caused by eventual consistency of S3 "
              + "listings.  Attempting retry.", attempt, maxAttempts,
              e);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
            fail("Test interrupted.");
            break;
          }
        } else {
          LOG.error(
              "Empty root directory test failed {} attempts.  Failing test.",
              maxAttempts);
          throw e;
        }
      }
    }
  }
}
