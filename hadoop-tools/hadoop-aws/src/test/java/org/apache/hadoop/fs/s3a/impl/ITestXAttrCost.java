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

package org.apache.hadoop.fs.s3a.impl;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_OP_XATTR_LIST;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_XATTR_GET_MAP;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_XATTR_GET_NAMED;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.CONTENT_TYPE_OCTET_STREAM;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.CONTENT_TYPE_APPLICATION_XML;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.XA_CONTENT_LENGTH;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.XA_CONTENT_TYPE;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.XA_STANDARD_HEADERS;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.decodeBytes;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.CREATE_FILE_OVERWRITE;

/**
 * Invoke XAttr API calls against objects in S3 and validate header
 * extraction.
 */
public class ITestXAttrCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestXAttrCost.class);

  private static final int GET_METADATA_ON_OBJECT = 1;
  private static final int GET_METADATA_ON_DIR = GET_METADATA_ON_OBJECT * 2;

  public ITestXAttrCost() {
    // no parameterization here
    super(false, true, false);
  }

  @Test
  public void testXAttrRoot() throws Throwable {
    describe("Test xattr on root");
    Path root = new Path("/");
    S3AFileSystem fs = getFileSystem();
    Map<String, byte[]> xAttrs = verifyMetrics(
        () -> fs.getXAttrs(root),
        with(INVOCATION_XATTR_GET_MAP, GET_METADATA_ON_OBJECT));
    logXAttrs(xAttrs);
    List<String> headerList = verifyMetrics(() ->
            fs.listXAttrs(root),
        with(INVOCATION_OP_XATTR_LIST, GET_METADATA_ON_OBJECT));

    // verify this contains all the standard markers,
    // but not the magic marker header
    Assertions.assertThat(headerList)
        .describedAs("Headers on root object")
        .containsOnly(
            XA_CONTENT_LENGTH,
            XA_CONTENT_TYPE);
    assertHeaderEntry(xAttrs, XA_CONTENT_TYPE)
        .isEqualTo(CONTENT_TYPE_APPLICATION_XML);
  }

  /**
   * Log the attributes as strings.
   * @param xAttrs map of attributes
   */
  private void logXAttrs(final Map<String, byte[]> xAttrs) {
    xAttrs.forEach((k, v) ->
        LOG.info("{} has bytes[{}] => \"{}\"",
            k, v.length, decodeBytes(v)));
  }

  @Test
  public void testXAttrFile() throws Throwable {
    describe("Test xattr on a file");
    Path testFile = methodPath();
    create(testFile, true, CREATE_FILE_OVERWRITE);
    S3AFileSystem fs = getFileSystem();
    Map<String, byte[]> xAttrs = verifyMetrics(() ->
            fs.getXAttrs(testFile),
        with(INVOCATION_XATTR_GET_MAP, GET_METADATA_ON_OBJECT));
    logXAttrs(xAttrs);
    assertHeaderEntry(xAttrs, XA_CONTENT_LENGTH)
        .isEqualTo("0");

    // get the list of supported headers
    List<String> headerList = verifyMetrics(
        () -> fs.listXAttrs(testFile),
        with(INVOCATION_OP_XATTR_LIST, GET_METADATA_ON_OBJECT));
    // verify this contains all the standard markers,
    // but not the magic marker header
    Assertions.assertThat(headerList)
        .describedAs("Supported headers")
        .containsAnyElementsOf(Arrays.asList(XA_STANDARD_HEADERS));

    // ask for one header and validate its value
    byte[] bytes = verifyMetrics(() ->
            fs.getXAttr(testFile, XA_CONTENT_LENGTH),
        with(INVOCATION_XATTR_GET_NAMED, GET_METADATA_ON_OBJECT));
    assertHeader(XA_CONTENT_LENGTH, bytes)
        .isEqualTo("0");
    assertHeaderEntry(xAttrs, XA_CONTENT_TYPE)
        .isEqualTo(CONTENT_TYPE_OCTET_STREAM);
  }

  /**
   * Directory attributes can be retrieved, but they take two HEAD requests.
   * @throws Throwable
   */
  @Test
  public void testXAttrDir() throws Throwable {
    describe("Test xattr on a dir");

    S3AFileSystem fs = getFileSystem();
    Path dir = methodPath();
    fs.mkdirs(dir);
    Map<String, byte[]> xAttrs = verifyMetrics(() ->
            fs.getXAttrs(dir),
        with(INVOCATION_XATTR_GET_MAP, GET_METADATA_ON_DIR));
    logXAttrs(xAttrs);
    assertHeaderEntry(xAttrs, XA_CONTENT_LENGTH)
        .isEqualTo("0");

    // get the list of supported headers
    List<String> headerList = verifyMetrics(
        () -> fs.listXAttrs(dir),
        with(INVOCATION_OP_XATTR_LIST, GET_METADATA_ON_DIR));
    // verify this contains all the standard markers,
    // but not the magic marker header
    Assertions.assertThat(headerList)
        .describedAs("Supported headers")
        .containsAnyElementsOf(Arrays.asList(XA_STANDARD_HEADERS));

    // ask for one header and validate its value
    byte[] bytes = verifyMetrics(() ->
            fs.getXAttr(dir, XA_CONTENT_LENGTH),
        with(INVOCATION_XATTR_GET_NAMED, GET_METADATA_ON_DIR));
    assertHeader(XA_CONTENT_LENGTH, bytes)
        .isEqualTo("0");
    assertHeaderEntry(xAttrs, XA_CONTENT_TYPE)
        .isEqualTo(CONTENT_TYPE_OCTET_STREAM);
  }

  /**
   * When the operations are called on a missing path, FNFE is
   * raised and only one attempt is made to retry the operation.
   */
  @Test
  public void testXAttrMissingFile() throws Throwable {
    describe("Test xattr on a missing path");
    Path testFile = methodPath();
    S3AFileSystem fs = getFileSystem();
    int getMetadataOnMissingFile = GET_METADATA_ON_DIR;
    verifyMetricsIntercepting(FileNotFoundException.class, "", () ->
            fs.getXAttrs(testFile),
        with(INVOCATION_XATTR_GET_MAP, getMetadataOnMissingFile));
    verifyMetricsIntercepting(FileNotFoundException.class, "", () ->
            fs.getXAttr(testFile, XA_CONTENT_LENGTH),
        with(INVOCATION_XATTR_GET_NAMED, getMetadataOnMissingFile));
    verifyMetricsIntercepting(FileNotFoundException.class, "", () ->
            fs.listXAttrs(testFile),
        with(INVOCATION_OP_XATTR_LIST, getMetadataOnMissingFile));
  }

  /**
   * Generate an assert on a named header in the map.
   * @param xAttrs attribute map
   * @param key header key
   * @return the assertion
   */
  private AbstractStringAssert<?> assertHeaderEntry(
      Map<String, byte[]> xAttrs, String key) {

    return assertHeader(key, xAttrs.get(key));
  }

  /**
   * Create an assertion on the header; check for the bytes
   * being non-null/empty and then returns the decoded values
   * as a string assert.
   * @param key header key (for error)
   * @param bytes value
   * @return the assertion
   */
  private AbstractStringAssert<?> assertHeader(final String key,
      final byte[] bytes) {

    String decoded = decodeBytes(bytes);
    return Assertions.assertThat(decoded)
        .describedAs("xattr %s decoded to: %s", key, decoded)
        .isNotNull()
        .isNotEmpty();
  }
}
