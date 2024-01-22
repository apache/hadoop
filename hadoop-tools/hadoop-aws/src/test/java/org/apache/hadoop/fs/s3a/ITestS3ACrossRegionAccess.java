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

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Test to verify cross region bucket access.
 */
public class ITestS3ACrossRegionAccess extends AbstractS3ATestBase {

  @Test
  public void testCentralEndpointCrossRegionAccess() throws Throwable {
    describe("Create bucket on different region and access it using central endpoint");
    Configuration conf = getConfiguration();
    removeBaseAndBucketOverrides(conf, ENDPOINT, AWS_REGION);

    Configuration newConf = new Configuration(conf);

    newConf.set(ENDPOINT, CENTRAL_ENDPOINT);

    try (S3AFileSystem newFs = new S3AFileSystem()) {
      newFs.initialize(getFileSystem().getUri(), newConf);

      final String file = getMethodName();
      Path basePath = new Path("basePath-" + getMethodName());
      final Path srcDir = new Path(basePath, "srcdir");
      newFs.mkdirs(srcDir);
      Path src = new Path(srcDir, file);

      try (FSDataOutputStream out = newFs.create(src)) {
        out.write(new byte[] {1, 2, 3, 4, 5});
      }
      ContractTestUtils.assertIsFile(getFileSystem(), new Path(srcDir, file));
    }
  }

}
