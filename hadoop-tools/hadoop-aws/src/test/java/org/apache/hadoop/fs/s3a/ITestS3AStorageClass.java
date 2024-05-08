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

import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;

import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_DISK;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS_GLACIER;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS_REDUCED_REDUNDANCY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfStorageClassTestsDisabled;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.XA_STORAGE_CLASS;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.decodeBytes;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests of storage class.
 */
@RunWith(Parameterized.class)
public class ITestS3AStorageClass extends AbstractS3ATestBase {

  /**
   * HADOOP-18339. Parameterized the test for different fast upload buffer types
   * to ensure the storage class configuration works with all of them.
   */
  @Parameterized.Parameters(name = "fast-upload-buffer-{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {FAST_UPLOAD_BUFFER_DISK},
        {FAST_UPLOAD_BUFFER_ARRAY}
    });
  }

  private final String fastUploadBufferType;

  public ITestS3AStorageClass(String fastUploadBufferType) {
    this.fastUploadBufferType = fastUploadBufferType;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    skipIfStorageClassTestsDisabled(conf);
    disableFilesystemCaching(conf);
    removeBaseAndBucketOverrides(conf, STORAGE_CLASS, FAST_UPLOAD_BUFFER);
    conf.set(FAST_UPLOAD_BUFFER, fastUploadBufferType);

    return conf;
  }

  /*
   * This test ensures the default storage class configuration (no config or null)
   * works well with create and copy operations
   */
  @Test
  public void testCreateAndCopyObjectWithStorageClassDefault() throws Throwable {
    Configuration conf = this.createConfiguration();
    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();

    FileSystem fs = contract.getTestFileSystem();
    Path dir = methodPath();
    fs.mkdirs(dir);
    assertObjectHasNoStorageClass(dir);
    Path path = new Path(dir, "file1");
    ContractTestUtils.touch(fs, path);
    assertObjectHasNoStorageClass(path);
    Path path2 = new Path(dir, "file1");
    fs.rename(path, path2);
    assertObjectHasNoStorageClass(path2);
  }

  /*
   * Verify object can be created and copied correctly
   * with specified storage class
   */
  @Test
  public void testCreateAndCopyObjectWithStorageClassReducedRedundancy() throws Throwable {
    Configuration conf = this.createConfiguration();
    conf.set(STORAGE_CLASS, STORAGE_CLASS_REDUCED_REDUNDANCY);
    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();

    FileSystem fs = contract.getTestFileSystem();
    Path dir = methodPath();
    fs.mkdirs(dir);
    // even with storage class specified
    // directories do not have storage class
    assertObjectHasNoStorageClass(dir);
    Path path = new Path(dir, "file1");
    ContractTestUtils.touch(fs, path);
    assertObjectHasStorageClass(path, STORAGE_CLASS_REDUCED_REDUNDANCY);
    Path path2 = new Path(dir, "file1");
    fs.rename(path, path2);
    assertObjectHasStorageClass(path2, STORAGE_CLASS_REDUCED_REDUNDANCY);
  }

  /*
   * Archive storage classes have different behavior
   * from general storage classes
   */
  @Test
  public void testCreateAndCopyObjectWithStorageClassGlacier() throws Throwable {
    Configuration conf = this.createConfiguration();
    conf.set(STORAGE_CLASS, STORAGE_CLASS_GLACIER);
    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();

    FileSystem fs = contract.getTestFileSystem();
    Path dir = methodPath();
    fs.mkdirs(dir);
    // even with storage class specified
    // directories do not have storage class
    assertObjectHasNoStorageClass(dir);
    Path path = new Path(dir, "file1");
    ContractTestUtils.touch(fs, path);
    assertObjectHasStorageClass(path, STORAGE_CLASS_GLACIER);
    Path path2 = new Path(dir, "file2");

    // this is the current behavior
    // object with archive storage class can't be read directly
    // when trying to read it, AccessDeniedException will be thrown
    // with message InvalidObjectState
    intercept(AccessDeniedException.class, "InvalidObjectState", () -> fs.rename(path, path2));
  }

  /*
   * Verify object can be created and copied correctly
   * with completely invalid storage class
   */
  @Test
  public void testCreateAndCopyObjectWithStorageClassInvalid() throws Throwable {
    Configuration conf = this.createConfiguration();
    conf.set(STORAGE_CLASS, "testing");
    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();

    FileSystem fs = contract.getTestFileSystem();
    Path dir = methodPath();
    fs.mkdirs(dir);
    // even with storage class specified
    // directories do not have storage class
    assertObjectHasNoStorageClass(dir);
    Path path = new Path(dir, "file1");
    ContractTestUtils.touch(fs, path);
    assertObjectHasNoStorageClass(path);
    Path path2 = new Path(dir, "file1");
    fs.rename(path, path2);
    assertObjectHasNoStorageClass(path2);
  }

  /*
   * Verify object can be created and copied correctly
   * with empty string configuration
   */
  @Test
  public void testCreateAndCopyObjectWithStorageClassEmpty() throws Throwable {
    Configuration conf = this.createConfiguration();
    conf.set(STORAGE_CLASS, "");
    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();

    FileSystem fs = contract.getTestFileSystem();
    Path dir = methodPath();
    fs.mkdirs(dir);
    // even with storage class specified
    // directories do not have storage class
    assertObjectHasNoStorageClass(dir);
    Path path = new Path(dir, "file1");
    ContractTestUtils.touch(fs, path);
    assertObjectHasNoStorageClass(path);
    Path path2 = new Path(dir, "file1");
    fs.rename(path, path2);
    assertObjectHasNoStorageClass(path2);
  }

  /**
   * Assert that a given object has no storage class specified.
   *
   * @param path path
   */
  protected void assertObjectHasNoStorageClass(Path path) throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Map<String, byte[]> xAttrs = fs.getXAttrs(path);
    String storageClass = decodeBytes(xAttrs.get(XA_STORAGE_CLASS));

    Assertions.assertThat(storageClass).describedAs("Storage class of object %s", path).isNull();
  }

  /**
   * Assert that a given object has the given storage class specified.
   *
   * @param path                 path
   * @param expectedStorageClass expected storage class for the object
   */
  protected void assertObjectHasStorageClass(Path path, String expectedStorageClass)
      throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Map<String, byte[]> xAttrs = fs.getXAttrs(path);
    String actualStorageClass = decodeBytes(xAttrs.get(XA_STORAGE_CLASS));

    Assertions.assertThat(actualStorageClass).describedAs("Storage class of object %s", path)
        .isEqualToIgnoringCase(expectedStorageClass);
  }
}
