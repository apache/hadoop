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

package org.apache.hadoop.fs.s3a.auth;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_SIGNERS;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_CLASS_NAME;
import static org.apache.hadoop.fs.s3a.Constants.SIGNING_ALGORITHM_S3;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.createMagicFile;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Test the HTTP signer SPI.
 * Two different UGIs are created; ths simplifies cleanup.
 */
public class ITestHttpSigner extends AbstractS3ATestBase {
  private static final Logger LOG = LoggerFactory
      .getLogger(ITestHttpSigner.class);

  private static final String TEST_ID_KEY = "TEST_ID_KEY";
  private static final String TEST_REGION_KEY = "TEST_REGION_KEY";

  private final UserGroupInformation ugi1 = UserGroupInformation.createRemoteUser("user1");

  private final UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("user2");

  private String regionName;

  private String endpoint;

  @Override
  public void setup() throws Exception {
    super.setup();
    final S3AFileSystem fs = getFileSystem();
    final Configuration conf = fs.getConf();
    // determine the endpoint -skipping the test.
    endpoint = conf.getTrimmed(Constants.ENDPOINT, Constants.CENTRAL_ENDPOINT);
    LOG.debug("Test endpoint is {}", endpoint);
    regionName = conf.getTrimmed(Constants.AWS_REGION, "");
    if (regionName.isEmpty()) {
      regionName = determineRegion(fs.getBucket());
    }
    LOG.debug("Determined region name to be [{}] for bucket [{}]", regionName,
        fs.getBucket());
  }

  private String determineRegion(String bucketName) throws IOException {
    return getS3AInternals().getBucketLocation(bucketName);
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    FileSystem.closeAllForUGI(ugi1);
    FileSystem.closeAllForUGI(ugi2);
  }

  private Configuration createTestConfig(String identifier) {
    Configuration conf = createConfiguration();

    removeBaseAndBucketOverrides(conf,
        CUSTOM_SIGNERS,
        SIGNING_ALGORITHM_S3);

    conf.setBoolean(HTTP_SIGNER_ENABLED, true);
    conf.set(HTTP_SIGNER_CLASS_NAME, CustomHttpSigner.class.getName());

    conf.set(TEST_ID_KEY, identifier);
    conf.set(TEST_REGION_KEY, regionName);

    // make absolutely sure there is no caching.
    disableFilesystemCaching(conf);

    return conf;
  }

  @Test
  public void testCustomSignerAndInitializer()
      throws IOException, InterruptedException {

    final Path basePath = path(getMethodName());
    FileSystem fs1 = runStoreOperationsAndVerify(ugi1,
        new Path(basePath, "customsignerpath1"), "id1");

    FileSystem fs2 = runStoreOperationsAndVerify(ugi2,
        new Path(basePath, "customsignerpath2"), "id2");
  }

  private S3AFileSystem runStoreOperationsAndVerify(UserGroupInformation ugi,
      Path finalPath, String identifier)
      throws IOException, InterruptedException {
    Configuration conf = createTestConfig(identifier);
    return ugi.doAs((PrivilegedExceptionAction<S3AFileSystem>) () -> {
      S3AFileSystem fs = (S3AFileSystem)finalPath.getFileSystem(conf);

      fs.mkdirs(finalPath);

      // now do some more operations to make sure all is good.
      final Path subdir = new Path(finalPath, "year=1970/month=1/day=1");
      fs.mkdirs(subdir);

      final Path file1 = new Path(subdir, "file1");
      ContractTestUtils.touch(fs, new Path(subdir, "file1"));
      fs.listStatus(subdir);
      fs.delete(file1, false);
      ContractTestUtils.touch(fs, new Path(subdir, "file1"));

      // create a magic file.
      createMagicFile(fs, subdir);
      ContentSummary summary = fs.getContentSummary(finalPath);
      fs.getS3AInternals().abortMultipartUploads(subdir);
      fs.rename(subdir, new Path(finalPath, "renamed"));
      fs.delete(finalPath, true);
      return fs;
    });
  }
}
