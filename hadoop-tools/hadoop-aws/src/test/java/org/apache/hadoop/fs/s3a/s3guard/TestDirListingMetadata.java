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

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.*;

/**
 * Unit tests of {@link DirListingMetadata}.
 */
public class TestDirListingMetadata {

  private static final String TEST_OWNER = "hadoop";
  public static final String TEST_ETAG = "abc";
  public static final String TEST_VERSION_ID = "def";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testNullPath() {
    exception.expect(NullPointerException.class);
    exception.expectMessage(notNullValue(String.class));
    new DirListingMetadata(null, null, false);
  }

  @Test
  public void testNullListing() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    assertEquals(path, meta.getPath());
    assertNotNull(meta.getListing());
    assertTrue(meta.getListing().isEmpty());
    assertFalse(meta.isAuthoritative());
  }

  @Test
  public void testEmptyListing() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path,
        new ArrayList<PathMetadata>(0),
        false);
    assertEquals(path, meta.getPath());
    assertNotNull(meta.getListing());
    assertTrue(meta.getListing().isEmpty());
    assertFalse(meta.isAuthoritative());
  }

  @Test
  public void testListing() {
    Path path = new Path("/path");
    PathMetadata pathMeta1 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir1"), TEST_OWNER));
    PathMetadata pathMeta2 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir2"), TEST_OWNER));
    PathMetadata pathMeta3 = new PathMetadata(
        new S3AFileStatus(123, 456, new Path(path, "file1"), 8192, TEST_OWNER,
            TEST_ETAG, TEST_VERSION_ID));
    List<PathMetadata> listing = Arrays.asList(pathMeta1, pathMeta2, pathMeta3);
    DirListingMetadata meta = new DirListingMetadata(path, listing, false);
    assertEquals(path, meta.getPath());
    assertNotNull(meta.getListing());
    assertFalse(meta.getListing().isEmpty());
    assertTrue(meta.getListing().contains(pathMeta1));
    assertTrue(meta.getListing().contains(pathMeta2));
    assertTrue(meta.getListing().contains(pathMeta3));
    assertFalse(meta.isAuthoritative());
  }

  @Test
  public void testListingUnmodifiable() {
    Path path = new Path("/path");
    DirListingMetadata meta = makeTwoDirsOneFile(path);
    assertNotNull(meta.getListing());
    exception.expect(UnsupportedOperationException.class);
    meta.getListing().clear();
  }

  @Test
  public void testAuthoritative() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, true);
    assertEquals(path, meta.getPath());
    assertNotNull(meta.getListing());
    assertTrue(meta.getListing().isEmpty());
    assertTrue(meta.isAuthoritative());
  }

  @Test
  public void testSetAuthoritative() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    assertEquals(path, meta.getPath());
    assertNotNull(meta.getListing());
    assertTrue(meta.getListing().isEmpty());
    assertFalse(meta.isAuthoritative());
    meta.setAuthoritative(true);
    assertTrue(meta.isAuthoritative());
  }

  @Test
  public void testGet() {
    Path path = new Path("/path");
    PathMetadata pathMeta1 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir1"), TEST_OWNER));
    PathMetadata pathMeta2 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir2"), TEST_OWNER));
    PathMetadata pathMeta3 = new PathMetadata(
        new S3AFileStatus(123, 456, new Path(path, "file1"), 8192, TEST_OWNER,
            TEST_ETAG, TEST_VERSION_ID));
    List<PathMetadata> listing = Arrays.asList(pathMeta1, pathMeta2, pathMeta3);
    DirListingMetadata meta = new DirListingMetadata(path, listing, false);
    assertEquals(path, meta.getPath());
    assertNotNull(meta.getListing());
    assertFalse(meta.getListing().isEmpty());
    assertTrue(meta.getListing().contains(pathMeta1));
    assertTrue(meta.getListing().contains(pathMeta2));
    assertTrue(meta.getListing().contains(pathMeta3));
    assertFalse(meta.isAuthoritative());
    assertEquals(pathMeta1, meta.get(pathMeta1.getFileStatus().getPath()));
    assertEquals(pathMeta2, meta.get(pathMeta2.getFileStatus().getPath()));
    assertEquals(pathMeta3, meta.get(pathMeta3.getFileStatus().getPath()));
    assertNull(meta.get(new Path(path, "notfound")));
  }

  @Test
  public void testGetNull() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(NullPointerException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.get(null);
  }

  @Test
  public void testGetRoot() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.get(new Path("/"));
  }

  @Test
  public void testGetNotChild() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.get(new Path("/different/ancestor"));
  }

  @Test
  public void testPut() {
    Path path = new Path("/path");
    PathMetadata pathMeta1 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir1"), TEST_OWNER));
    PathMetadata pathMeta2 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir2"), TEST_OWNER));
    PathMetadata pathMeta3 = new PathMetadata(
        new S3AFileStatus(123, 456, new Path(path, "file1"), 8192, TEST_OWNER,
            TEST_ETAG, TEST_VERSION_ID));
    List<PathMetadata> listing = Arrays.asList(pathMeta1, pathMeta2, pathMeta3);
    DirListingMetadata meta = new DirListingMetadata(path, listing, false);
    assertEquals(path, meta.getPath());
    assertNotNull(meta.getListing());
    assertFalse(meta.getListing().isEmpty());
    assertTrue(meta.getListing().contains(pathMeta1));
    assertTrue(meta.getListing().contains(pathMeta2));
    assertTrue(meta.getListing().contains(pathMeta3));
    assertFalse(meta.isAuthoritative());
    PathMetadata pathMeta4 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir3"), TEST_OWNER));
    meta.put(pathMeta4);
    assertTrue(meta.getListing().contains(pathMeta4));
    assertEquals(pathMeta4, meta.get(pathMeta4.getFileStatus().getPath()));
  }

  @Test
  public void testPutNull() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(NullPointerException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.put(null);
  }

  @Test
  public void testPutNullPath() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(NullPointerException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.put(new PathMetadata(new S3AFileStatus(true, null, TEST_OWNER)));
  }

  @Test
  public void testPutRoot() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.put(new PathMetadata(new S3AFileStatus(true, new Path("/"),
        TEST_OWNER)));
  }

  @Test
  public void testPutNotChild() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.put(new PathMetadata(
        new S3AFileStatus(true, new Path("/different/ancestor"), TEST_OWNER)));
  }

  @Test
  public void testRemove() {
    Path path = new Path("/path");
    PathMetadata pathMeta1 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir1"), TEST_OWNER));
    PathMetadata pathMeta2 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir2"), TEST_OWNER));
    PathMetadata pathMeta3 = new PathMetadata(
        new S3AFileStatus(123, 456, new Path(path, "file1"), 8192, TEST_OWNER,
            TEST_ETAG, TEST_VERSION_ID));
    List<PathMetadata> listing = Arrays.asList(pathMeta1, pathMeta2, pathMeta3);
    DirListingMetadata meta = new DirListingMetadata(path, listing, false);
    assertEquals(path, meta.getPath());
    assertNotNull(meta.getListing());
    assertFalse(meta.getListing().isEmpty());
    assertTrue(meta.getListing().contains(pathMeta1));
    assertTrue(meta.getListing().contains(pathMeta2));
    assertTrue(meta.getListing().contains(pathMeta3));
    assertFalse(meta.isAuthoritative());
    meta.remove(pathMeta1.getFileStatus().getPath());
    assertFalse(meta.getListing().contains(pathMeta1));
    assertNull(meta.get(pathMeta1.getFileStatus().getPath()));
  }

  @Test
  public void testRemoveNull() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(NullPointerException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.remove(null);
  }

  @Test
  public void testRemoveRoot() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.remove(new Path("/"));
  }

  @Test
  public void testRemoveNotChild() {
    Path path = new Path("/path");
    DirListingMetadata meta = new DirListingMetadata(path, null, false);
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(notNullValue(String.class));
    meta.remove(new Path("/different/ancestor"));
  }


  @Test
  public void testRemoveExpiredEntriesFromListing() {
    long ttl = 9;
    long oldTime = 100;
    long newTime = 110;
    long now = 110;

    Path path = new Path("/path");
    PathMetadata pathMeta1 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir1"), TEST_OWNER));
    PathMetadata pathMeta2 = new PathMetadata(
        new S3AFileStatus(true, new Path(path, "dir2"), TEST_OWNER));
    PathMetadata pathMeta3 = new PathMetadata(
        new S3AFileStatus(123, 456, new Path(path, "file1"), 8192, TEST_OWNER,
            TEST_ETAG, TEST_VERSION_ID));
    pathMeta1.setLastUpdated(oldTime);
    pathMeta2.setLastUpdated(0);
    pathMeta3.setLastUpdated(newTime);

    List<PathMetadata> listing = Arrays.asList(pathMeta1, pathMeta2, pathMeta3);
    DirListingMetadata meta = new DirListingMetadata(path, listing, false);

    List<PathMetadata> expired = meta.removeExpiredEntriesFromListing(ttl,
        now);

    Assertions.assertThat(meta.getListing())
        .describedAs("Metadata listing for %s", path)
        .doesNotContain(pathMeta1)
        .contains(pathMeta2)
        .contains(pathMeta3);
    Assertions.assertThat(expired)
        .describedAs("Expire entries underr %s", path)
        .doesNotContain(pathMeta2)
        .contains(pathMeta1);
  }

  /**
   * Create DirListingMetadata with two dirs and one file living in directory
   * 'parent'.
   */
  private static DirListingMetadata makeTwoDirsOneFile(Path parent) {
    PathMetadata pathMeta1 = new PathMetadata(
        new S3AFileStatus(true, new Path(parent, "dir1"), TEST_OWNER));
    PathMetadata pathMeta2 = new PathMetadata(
        new S3AFileStatus(true, new Path(parent, "dir2"), TEST_OWNER));
    PathMetadata pathMeta3 = new PathMetadata(
        new S3AFileStatus(123, 456, new Path(parent, "file1"), 8192,
            TEST_OWNER, TEST_ETAG, TEST_VERSION_ID));
    List<PathMetadata> listing = Arrays.asList(pathMeta1, pathMeta2, pathMeta3);
    return new DirListingMetadata(parent, listing, false);
  }
}
