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

package org.apache.hadoop.fs.s3a.tools;

import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSBadRequestException;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeNotS3ExpressFileSystem;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeS3ExpressFileSystem;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeStoreAwsHosted;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.expectErrorCode;
import static org.apache.hadoop.fs.s3a.impl.S3ExpressStorage.STORE_CAPABILITY_S3_EXPRESS_STORAGE;
import static org.apache.hadoop.fs.s3a.tools.BucketTool.CREATE;
import static org.apache.hadoop.fs.s3a.tools.BucketTool.NO_ZONE_SUPPLIED;
import static org.apache.hadoop.fs.s3a.tools.BucketTool.OPT_ENDPOINT;
import static org.apache.hadoop.fs.s3a.tools.BucketTool.OPT_REGION;
import static org.apache.hadoop.fs.s3a.tools.BucketTool.OPT_ZONE;
import static org.apache.hadoop.fs.s3a.tools.BucketTool.UNSUPPORTED_ZONE_ARG;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test {@link BucketTool}.
 * This is a tricky little test as it doesn't want to create any new bucket,
 * including on third party stores.
 */
public class ITestBucketTool extends AbstractS3ATestBase {

  public static final String USWEST_2 = "us-west-2";

  private static final Logger LOG = LoggerFactory.getLogger(ITestBucketTool.class);

  /**
   * This is yours.
   */
  public static final String OWNED = "BucketAlreadyOwnedByYouException";

  /**
   * This is not yours. Not *yet* used in any tests.
   */
  public static final String EXISTS = "BucketAlreadyExists";

  /**
   * Sample S3 Express bucket name. {@value}.
   */
  private static final String S3EXPRESS_BUCKET = "bucket--usw2-az2--x-s3";

  /**
   * Sample S3 Express bucket URI. {@value}.
   */
  public static final String S3A_S3EXPRESS = "s3a://" + S3EXPRESS_BUCKET;

  /**
   * US west az. {@value}
   */
  public static final String USWEST_AZ_2 = "usw2-az2";

  public static final String INVALID_LOCATION =
      "Invalid Name value for Location configuration";

  private String bucketName;

  private boolean s3ExpressStore;

  private BucketTool bucketTool;

  private String fsURI;

  private String region;

  private S3AFileSystem fs;

  @Override
  public void setup() throws Exception {
    super.setup();
    fs = getFileSystem();
    final Configuration conf = fs.getConf();
    bucketName = S3ATestUtils.getTestBucketName(conf);
    fsURI = fs.getUri().toString();
    s3ExpressStore = fs.hasPathCapability(new Path("/"), STORE_CAPABILITY_S3_EXPRESS_STORAGE);
    bucketTool = new BucketTool(conf);
    region = conf.get(AWS_REGION, "");
  }

  @Test
  public void testRecreateTestBucketS3Express() throws Throwable {
    assumeS3ExpressFileSystem(getFileSystem());
    final IOException ex = intercept(IOException.class,
        () -> bucketTool.exec("bucket", d(CREATE), d(OPT_ZONE), USWEST_AZ_2, d(OPT_REGION), region,
            fsURI));
    if (ex instanceof AWSBadRequestException) {
      // owned error
      if (!ex.getMessage().contains(OWNED)
          && !ex.getMessage().contains(INVALID_LOCATION)) {
        throw ex;
      }
    } else if (ex instanceof UnknownHostException) {
      // endpoint region stuff, expect the error to include the s3express endpoint
      // name
      assertExceptionContains("s3express-control", ex);
    }
  }

  /**
   * Try and recreate the test bucket.
   * Third party stores may allow this, so skip test on them.
   */
  @Test
  public void testRecreateTestBucketNonS3Express() throws Throwable {
    assumeNotS3ExpressFileSystem(fs);
    assumeStoreAwsHosted(fs);
    intercept(AWSBadRequestException.class, OWNED,
        () -> bucketTool.exec("bucket", d(CREATE),
            d(OPT_REGION), region,
            fsURI));
  }

  @Test
  public void testSimpleBucketWithZoneParam() throws Throwable {
    expectErrorCode(EXIT_USAGE,
        intercept(ExitUtil.ExitException.class, UNSUPPORTED_ZONE_ARG, () ->
            bucketTool.exec("bucket", d(CREATE),
                d(OPT_ZONE), USWEST_AZ_2,
                "s3a://simple/")));
  }

  @Test
  public void testS3ExpressBucketWithoutZoneParam() throws Throwable {
    assumeStoreAwsHosted(getFileSystem());
    expectErrorCode(EXIT_USAGE,
        intercept(ExitUtil.ExitException.class, NO_ZONE_SUPPLIED, () ->
            bucketTool.exec("bucket", d(CREATE),
                S3A_S3EXPRESS)));
  }

  /**
   * To avoid accidentally trying to create S3 Express stores on third party
   * endpoints, the tool rejects such attempts.
   */
  @Test
  public void testS3ExpressStorageOnlyOnAwsEndpoint() throws Throwable {
    describe("The tool only supports S3 Express bucket names on aws endpoints");
    expectErrorCode(EXIT_NOT_ACCEPTABLE,
        intercept(ExitUtil.ExitException.class, () ->
            bucketTool.exec("bucket", d(CREATE),
                d(OPT_ENDPOINT), "s3.example.org",
                S3A_S3EXPRESS)));
  }

  /**
   * Add a dash to a string.
   * @param s source.
   * @return "-s"
   */
  private String d(String s) {
    return "-" + s;
  }

}
