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

package org.apache.hadoop.fs.s3a.list;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3ListRequest;
import org.apache.hadoop.fs.s3a.api.S3ObjectStorageClassFilter;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.s3a.Constants.READ_RESTORED_GLACIER_OBJECTS;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS_DEEP_ARCHIVE;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS_GLACIER;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfStorageClassTestsDisabled;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;


/**
 * Tests of various cases related to Glacier/Deep Archive Storage class.
 */
@RunWith(Parameterized.class)
public class ITestS3AReadRestoredGlacierObjects extends AbstractS3ATestBase {

  enum Type { GLACIER_AND_DEEP_ARCHIVE, GLACIER }

  @Parameterized.Parameters(name = "storage-class-{1}")
  public static Collection<Object[]> data(){
    return Arrays.asList(new Object[][] {
        {STORAGE_CLASS_GLACIER}, {STORAGE_CLASS_DEEP_ARCHIVE},
    });
  }

  private static final int MAX_RETRIES = 100;
  private static final int RETRY_DELAY_MS = 5000;
  private final String glacierClass;

  public ITestS3AReadRestoredGlacierObjects(String glacierClass) {
    this.glacierClass = glacierClass;
  }

  private FileSystem createFiles(String s3ObjectStorageClassFilter) throws Throwable {
    FileSystem fs = createFileSystem(s3ObjectStorageClassFilter);
    Path path = new Path(methodPath(), "glaciated");
    ContractTestUtils.touch(fs, path);
    return fs;
  }

  private FileSystem createFileSystem(String s3ObjectStorageClassFilter) throws Throwable {
    Configuration conf = createConfiguration();
    conf.set(READ_RESTORED_GLACIER_OBJECTS, s3ObjectStorageClassFilter);
    // Create Glacier objects:Storage Class:DEEP_ARCHIVE/GLACIER
    conf.set(STORAGE_CLASS, glacierClass);
    FileSystem fs = new S3AFileSystem();
    fs.initialize(getFileSystem().getUri(), conf);
    return fs;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = super.createConfiguration();
    skipIfStorageClassTestsDisabled(newConf);
    disableFilesystemCaching(newConf);
    removeBaseAndBucketOverrides(newConf, STORAGE_CLASS, READ_RESTORED_GLACIER_OBJECTS);
    return newConf;
  }

  @Test
  public void testAllGlacierAndDeepArchiveCases() throws Throwable {

    try (FileSystem fs = createFiles(S3ObjectStorageClassFilter.SKIP_ALL_GLACIER.name())) {

      // testIgnoreGlacierObject
      describe("Running testIgnoreGlacierObject");
      Assertions.assertThat(
          fs.listStatus(methodPath()))
        .describedAs("FileStatus List of %s", methodPath())
        .isEmpty();

      // testReadAllObjects
      describe("Running testReadAllObjects");
      try (FileSystem fsTest = createFileSystem(S3ObjectStorageClassFilter.READ_ALL.name())) {
        Assertions.assertThat(
                fsTest.listStatus(methodPath()))
            .describedAs("FileStatus List of %s", methodPath())
            .isNotEmpty();
      }

      // testIgnoreRestoringGlacierObject
      describe("Running testIgnoreRestoringGlacierObject");
      try (FileSystem fsTest = createFileSystem(S3ObjectStorageClassFilter.READ_RESTORED_GLACIER_OBJECTS.name())) {
        Assertions.assertThat(
                fsTest.listStatus(methodPath()))
            .describedAs("FileStatus List of %s", methodPath())
            .isEmpty();
      }

      // testConfigWithInvalidValue
      describe("Running testConfigWithInvalidValue");
      String invalidValue = "ABCDE";
      try (FileSystem fsTest = createFiles(invalidValue)) {
        Assertions.assertThat(
                fsTest.listStatus(methodPath()))
            .describedAs("FileStatus List of %s", methodPath())
            .isNotEmpty();
      }


      // testRestoredGlacierObject - run only for Glacier Storage Class
      if ( glacierClass == STORAGE_CLASS_GLACIER ) {
        describe("Running testRestoredGlacierObject");
        try (FileSystem fsTest = createFileSystem(S3ObjectStorageClassFilter.READ_RESTORED_GLACIER_OBJECTS.name())) {
          restoreGlacierObject(getFilePrefixForListObjects(), 2);
          Assertions.assertThat(
                  fsTest.listStatus(methodPath()))
              .describedAs("FileStatus List of %s", methodPath())
              .isNotEmpty();
        }

      }

    }
  }


  private void restoreGlacierObject(String glacierObjectKey, int expirationDays) throws Exception {
    describe("Initiate restore of the Glacier object");
    try (AuditSpan auditSpan = getSpanSource().createSpan(OBJECT_LIST_REQUEST, "", "").activate()) {

      S3Client s3Client = getFileSystem().getS3AInternals().getAmazonS3Client("test");

      // Create a restore object request
      RestoreObjectRequest requestRestore = getFileSystem().getRequestFactory()
          .newRestoreObjectRequestBuilder(glacierObjectKey, Tier.EXPEDITED, expirationDays).build();

      s3Client.restoreObject(requestRestore);

      // fetch the glacier object
      S3ListRequest s3ListRequest = getFileSystem().createListObjectsRequest(
          getFilePrefixForListObjects(), "/");

      describe("Wait till restore of the object is complete");
      LambdaTestUtils.await(MAX_RETRIES * RETRY_DELAY_MS, RETRY_DELAY_MS,
          () -> !getS3GlacierObject(s3Client, s3ListRequest).restoreStatus().isRestoreInProgress());
    }
  }


  private String getFilePrefixForListObjects() throws IOException {
    return getFileSystem().pathToKey(new Path(methodPath(), "glaciated"));
  }

  private S3Object getS3GlacierObject(S3Client s3Client, S3ListRequest s3ListRequest) {
    return s3Client.listObjectsV2(s3ListRequest.getV2()).contents()
        .stream()
        .findFirst().orElse(null);
  }
}
