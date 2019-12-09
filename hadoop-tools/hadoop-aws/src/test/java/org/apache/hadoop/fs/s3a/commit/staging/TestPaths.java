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

package org.apache.hadoop.fs.s3a.commit.staging;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.commit.staging.Paths.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test {@link org.apache.hadoop.fs.s3a.commit.staging.Paths}.
 */
public class TestPaths extends HadoopTestBase {

  @Test
  public void testUUIDPart() {
    assertUUIDAdded("/part-0000", "/part-0000-UUID");
  }

  @Test
  public void testUUIDPartSuffix() {
    assertUUIDAdded("/part-0000.gz.csv", "/part-0000-UUID.gz.csv");
  }

  @Test
  public void testUUIDDottedPath() {
    assertUUIDAdded("/parent.dir/part-0000", "/parent.dir/part-0000-UUID");
  }

  @Test
  public void testUUIDPartUUID() {
    assertUUIDAdded("/part-0000-UUID.gz.csv", "/part-0000-UUID.gz.csv");
  }

  @Test
  public void testUUIDParentUUID() {
    assertUUIDAdded("/UUID/part-0000.gz.csv", "/UUID/part-0000.gz.csv");
  }

  @Test
  public void testUUIDDir() throws Throwable {
    intercept(IllegalStateException.class,
        () -> addUUID("/dest/", "UUID"));
  }

  @Test
  public void testUUIDEmptyDir() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> addUUID("", "UUID"));
  }

  @Test
  public void testEmptyUUID() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> addUUID("part-0000.gz", ""));
  }

  private void assertUUIDAdded(String path, String expected) {
    assertEquals("from " + path, expected, addUUID(path, "UUID"));
  }

  private static final String DATA = "s3a://landsat-pds/data/";
  private static final Path BASE = new Path(DATA);

  @Test
  public void testRelativizeOneLevel() {
    String suffix = "year=2017";
    Path path = new Path(DATA + suffix);
    assertEquals(suffix, getRelativePath(BASE, path));
  }

  @Test
  public void testRelativizeTwoLevel() {
    String suffix = "year=2017/month=10";
    Path path = path(BASE, suffix);
    assertEquals(suffix, getRelativePath(BASE, path));
  }

  @Test
  public void testRelativizeSelf() {
    assertEquals("", getRelativePath(BASE, BASE));
  }

  @Test
  public void testRelativizeParent() {
    // goes up to the parent if one is above the other
    assertEquals("/", getRelativePath(BASE, BASE.getParent()));
  }

  @Test
  public void testGetPartition() {
    assertEquals("year=2017/month=10",
        getPartition("year=2017/month=10/part-0000.avro"));
  }

  @Test
  public void testMPUCommitDir() throws Throwable {
    Configuration conf = new Configuration();
    LocalFileSystem localFS = FileSystem.getLocal(conf);
    Path dir = getMultipartUploadCommitsDirectory(localFS, conf, "UUID");
    assertTrue(dir.toString().endsWith("UUID/"
        + StagingCommitterConstants.STAGING_UPLOADS));
  }

}
