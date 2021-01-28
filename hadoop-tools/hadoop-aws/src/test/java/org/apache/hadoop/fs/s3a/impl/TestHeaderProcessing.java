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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.test.OperationTrackingStore;
import org.apache.hadoop.test.HadoopTestBase;

import static java.lang.System.currentTimeMillis;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.XA_MAGIC_MARKER;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.X_HEADER_MAGIC_MARKER;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.XA_LAST_MODIFIED;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.XA_CONTENT_LENGTH;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.decodeBytes;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.encodeBytes;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.extractXAttrLongValue;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit tests of header processing logic in {@link HeaderProcessing}.
 * Builds up a context accessor where the path
 * defined in {@link #MAGIC_PATH} exists and returns object metadata.
 *
 */
public class TestHeaderProcessing extends HadoopTestBase {

  private static final XAttrContextAccessor CONTEXT_ACCESSORS
      = new XAttrContextAccessor();

  public static final String VALUE = "abcdeFGHIJ123!@##&82;";

  public static final long FILE_LENGTH = 1024;

  private static final String FINAL_FILE = "s3a://bucket/dest/output.csv";

  private StoreContext context;

  private HeaderProcessing headerProcessing;

  private static final String MAGIC_KEY
      = "dest/__magic/job1/ta1/__base/output.csv";
  private static final String MAGIC_FILE
      = "s3a://bucket/" + MAGIC_KEY;

  private static final Path MAGIC_PATH =
      new Path(MAGIC_FILE);

  public static final long MAGIC_LEN = 4096L;

  /**
   * All the XAttrs which are built up.
   */
  private static final String[] RETRIEVED_XATTRS = {
      XA_MAGIC_MARKER,
      XA_CONTENT_LENGTH,
      XA_LAST_MODIFIED
  };

  @Before
  public void setup() throws Exception {
    CONTEXT_ACCESSORS.len = FILE_LENGTH;
    CONTEXT_ACCESSORS.userHeaders.put(
        X_HEADER_MAGIC_MARKER,
        Long.toString(MAGIC_LEN));
    context = S3ATestUtils.createMockStoreContext(true,
        new OperationTrackingStore(), CONTEXT_ACCESSORS);
    headerProcessing = new HeaderProcessing(context);
  }

  @Test
  public void testByteRoundTrip() throws Throwable {
    Assertions.assertThat(decodeBytes(encodeBytes(VALUE)))
        .describedAs("encoding of " + VALUE)
        .isEqualTo(VALUE);
  }

  @Test
  public void testGetMarkerXAttr() throws Throwable {
    assertAttributeHasValue(XA_MAGIC_MARKER, MAGIC_LEN);
  }

  @Test
  public void testGetLengthXAttr() throws Throwable {
    assertAttributeHasValue(XA_CONTENT_LENGTH, FILE_LENGTH);
  }

  /**
   * Last modified makes it through.
   */
  @Test
  public void testGetDateXAttr() throws Throwable {
    Assertions.assertThat(
        decodeBytes(headerProcessing.getXAttr(MAGIC_PATH,
            XA_LAST_MODIFIED)))
        .describedAs("XAttribute " + XA_LAST_MODIFIED)
        .isEqualTo(CONTEXT_ACCESSORS.date.toString());
  }

  /**
   * The API calls on unknown paths raise 404s.
   */
  @Test
  public void test404() throws Throwable {
    intercept(FileNotFoundException.class, () ->
        headerProcessing.getXAttr(new Path(FINAL_FILE), XA_MAGIC_MARKER));
  }

  /**
   * This call returns all the attributes which aren't null, including
   * all the standard HTTP headers.
   */
  @Test
  public void testGetAllXAttrs() throws Throwable {
    Map<String, byte[]> xAttrs = headerProcessing.getXAttrs(MAGIC_PATH);
    Assertions.assertThat(xAttrs.keySet())
        .describedAs("Attribute keys")
        .contains(RETRIEVED_XATTRS);
  }

  /**
   * This call returns all the attributes which aren't null, including
   * all the standard HTTP headers.
   */
  @Test
  public void testListXAttrKeys() throws Throwable {
    List<String> xAttrs = headerProcessing.listXAttrs(MAGIC_PATH);
    Assertions.assertThat(xAttrs)
        .describedAs("Attribute keys")
        .contains(RETRIEVED_XATTRS);
  }

  /**
   * Filtering is on attribute key, not header.
   */
  @Test
  public void testGetFilteredXAttrs() throws Throwable {
    Map<String, byte[]> xAttrs = headerProcessing.getXAttrs(MAGIC_PATH,
        Lists.list(XA_MAGIC_MARKER, XA_CONTENT_LENGTH, "unknown"));
    Assertions.assertThat(xAttrs.keySet())
        .describedAs("Attribute keys")
        .containsExactlyInAnyOrder(XA_MAGIC_MARKER, XA_CONTENT_LENGTH);
    // and the values are good
    assertLongAttributeValue(
        XA_MAGIC_MARKER,
        xAttrs.get(XA_MAGIC_MARKER),
        MAGIC_LEN);
    assertLongAttributeValue(
        XA_CONTENT_LENGTH,
        xAttrs.get(XA_CONTENT_LENGTH),
        FILE_LENGTH);
  }

  /**
   * An empty list of keys results in empty results.
   */
  @Test
  public void testFilterEmptyXAttrs() throws Throwable {
    Map<String, byte[]> xAttrs = headerProcessing.getXAttrs(MAGIC_PATH,
        Lists.list());
    Assertions.assertThat(xAttrs.keySet())
        .describedAs("Attribute keys")
        .isEmpty();
  }

  /**
   * Add two headers to the metadata, then verify that
   * the magic marker header is copied, but not the other header.
   */
  @Test
  public void testMetadataCopySkipsMagicAttribute() throws Throwable {

    final String owner = "x-header-owner";
    final String root = "root";
    CONTEXT_ACCESSORS.userHeaders.put(owner, root);
    final ObjectMetadata source = context.getContextAccessors()
        .getObjectMetadata(MAGIC_KEY);
    final Map<String, String> sourceUserMD = source.getUserMetadata();
    Assertions.assertThat(sourceUserMD.get(owner))
        .describedAs("owner header in copied MD")
        .isEqualTo(root);

    ObjectMetadata dest = new ObjectMetadata();
    headerProcessing.cloneObjectMetadata(source, dest);

    Assertions.assertThat(dest.getUserMetadata().get(X_HEADER_MAGIC_MARKER))
        .describedAs("Magic marker header in copied MD")
        .isNull();
    Assertions.assertThat(dest.getUserMetadata().get(owner))
        .describedAs("owner header in copied MD")
        .isEqualTo(root);
  }

  /**
   * Assert that an XAttr has a specific long value.
   * @param key attribute key
   * @param bytes bytes of the attribute.
   * @param expected expected numeric value.
   */
  private void assertLongAttributeValue(
      final String key,
      final byte[] bytes,
      final long expected) {
    Assertions.assertThat(extractXAttrLongValue(bytes))
        .describedAs("XAttribute " + key)
        .isNotEmpty()
        .hasValue(expected);
  }

  /**
   * Assert that a retrieved XAttr has a specific long value.
   * @param key attribute key
   * @param expected expected numeric value.
   */
  protected void assertAttributeHasValue(final String key,
      final long expected)
      throws IOException {
    assertLongAttributeValue(
        key,
        headerProcessing.getXAttr(MAGIC_PATH, key),
        expected);
  }

  /**
   * Context accessor with XAttrs returned for the {@link #MAGIC_PATH}
   * path.
   */
  private static final class XAttrContextAccessor
      implements ContextAccessors {

    private final Map<String, String> userHeaders = new HashMap<>();

    private long len;
    private Date date = new Date(currentTimeMillis());

    @Override
    public Path keyToPath(final String key) {
      return new Path("s3a://bucket/" + key);
    }

    @Override
    public String pathToKey(final Path path) {
      // key is path with leading / stripped.
      String key = path.toUri().getPath();
      return key.length() > 1 ? key.substring(1) : key;
    }

    @Override
    public File createTempFile(final String prefix, final long size)
        throws IOException {
      throw new UnsupportedOperationException("unsppported");
    }

    @Override
    public String getBucketLocation() throws IOException {
      return null;
    }

    @Override
    public Path makeQualified(final Path path) {
      return path;
    }

    @Override
    public ObjectMetadata getObjectMetadata(final String key)
        throws IOException {
      if (MAGIC_KEY.equals(key)) {
        ObjectMetadata omd = new ObjectMetadata();
        omd.setUserMetadata(userHeaders);
        omd.setContentLength(len);
        omd.setLastModified(date);
        return omd;
      } else {
        throw new FileNotFoundException(key);
      }
    }

    public void setHeader(String key, String val) {
      userHeaders.put(key, val);
    }
  }

}
