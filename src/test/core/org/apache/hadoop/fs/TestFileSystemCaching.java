/**
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

package org.apache.hadoop.fs;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNotSame;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestFileSystemCaching {

  @Test
  public void testCacheEnabled() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    FileSystem fs1 = FileSystem.get(new URI("cachedfile://a"), conf);
    FileSystem fs2 = FileSystem.get(new URI("cachedfile://a"), conf);
    assertSame(fs1, fs2);
  }

  @Test
  public void testCacheDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.uncachedfile.impl", conf.get("fs.file.impl"));
    conf.setBoolean("fs.uncachedfile.impl.disable.cache", true);
    FileSystem fs1 = FileSystem.get(new URI("uncachedfile://a"), conf);
    FileSystem fs2 = FileSystem.get(new URI("uncachedfile://a"), conf);
    assertNotSame(fs1, fs2);
  }

  @Test
  public void testGetLocal() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT);
    FileSystem fs1 = FileSystem.get(conf);
    assertTrue(fs1 instanceof LocalFileSystem);

    FileSystem fs2 = FileSystem.get(LocalFileSystem.NAME, conf);
    assertTrue(fs2 instanceof LocalFileSystem);
  }

  @Test
  public void testGetDefaultFs() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    assertNotNull(fs);
  }

  private void checkDefaultFsParam(Configuration conf, String uri)
      throws Exception {
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, uri);
    assertNotNull(FileSystem.get(conf));
  }

  @Test
  public void testInvalidDefaultFsParam() throws Exception {
    Configuration conf = new Configuration();

    // All of the following set the default filesystem config
    // to an invalid URI. Subsequent requests for a FileSystem
    // should return the internal default fs.

    checkDefaultFsParam(conf, "this-is-an-/invalid_uri");
    checkDefaultFsParam(conf, "");
    checkDefaultFsParam(conf, "/foo");
    checkDefaultFsParam(conf, "/foo/bar");
    checkDefaultFsParam(conf, "foo");
    checkDefaultFsParam(conf, "foo:8020");
    checkDefaultFsParam(conf, "foo/bar");
    checkDefaultFsParam(conf, "foo:8020/bar");
    checkDefaultFsParam(conf, "hdfs://");
    checkDefaultFsParam(conf, "local");
  }

  @Test
  public void testGetInvalidFs() throws Exception {
    Configuration conf = new Configuration();

    // None of these are valid FileSystem URIs. The default FS
    // should be returned in all cases.

    assertNotNull(FileSystem.get(URI.create("/foo"), conf));
    assertNotNull(FileSystem.get(URI.create("/foo/bar"), conf));
    assertNotNull(FileSystem.get(URI.create("foo"), conf));
    assertNotNull(FileSystem.get(URI.create("foo/bar"), conf));
    assertNotNull(FileSystem.get(URI.create("local"), conf));
  }

  private void checkBadUri(Configuration conf, String uri) {
    try {
      FileSystem.setDefaultUri(conf, uri);
      fail("Expected invalid URI: " + uri);
    } catch (Exception e) {
      // Got expected exception; ok.
    }
  }

  @Test
  public void testDefaultUri() throws Exception {
    Configuration conf = new Configuration();
    URI uri = FileSystem.getDefaultUri(conf);
    assertNotNull(uri.getScheme());

    final URI exampleGoodUri = new URI("hdfs://foo.example.com:999");

    FileSystem.setDefaultUri(conf, exampleGoodUri);
    URI out = FileSystem.getDefaultUri(conf);
    assertEquals(exampleGoodUri, out);

    checkBadUri(conf, "bla"); // no scheme
    checkBadUri(conf, "local"); // deprecated syntax
    checkBadUri(conf, "foo:8020"); // no scheme, deprecated syntax.
    checkBadUri(conf, "hdfs://"); // not a valid uri; requires authority-part
    checkBadUri(conf, ""); // not a uri.

    // Check that none of these actually changed the conf.
    out = FileSystem.getDefaultUri(conf);
    assertEquals(exampleGoodUri, out);
  }
}
