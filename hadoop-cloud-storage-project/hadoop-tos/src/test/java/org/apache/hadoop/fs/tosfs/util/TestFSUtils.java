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

package org.apache.hadoop.fs.tosfs.util;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestFSUtils {
  @Test
  public void testNormalizeURI() throws URISyntaxException {
    URI uri = new URI("tos://abc/dir/key");
    URI normalizeURI = FSUtils.normalizeURI(uri, new Configuration());
    assertEquals("tos", normalizeURI.getScheme());
    assertEquals("abc", normalizeURI.getAuthority());
    assertEquals("abc", normalizeURI.getHost());
    assertEquals("/dir/key", normalizeURI.getPath());

    uri = new URI("/abc/dir/key");
    normalizeURI = FSUtils.normalizeURI(uri, new Configuration());
    assertNull(uri.getScheme());
    assertEquals("file", normalizeURI.getScheme());
    assertNull(uri.getAuthority());
    assertNull(normalizeURI.getAuthority());
    assertEquals("/abc/dir/key", uri.getPath());
    assertEquals("/", normalizeURI.getPath());

    uri = new URI("tos:///abc/dir/key");
    normalizeURI = FSUtils.normalizeURI(uri, new Configuration());
    assertEquals("tos", uri.getScheme());
    assertNull(uri.getAuthority());
    assertEquals("/abc/dir/key", uri.getPath());
    assertEquals("tos", normalizeURI.getScheme());
    assertNull(normalizeURI.getAuthority());
    assertEquals("/abc/dir/key", normalizeURI.getPath());

    Configuration conf = new Configuration();
    conf.set(FS_DEFAULT_NAME_KEY, "tos://bucket/");
    normalizeURI = FSUtils.normalizeURI(uri, conf);
    assertEquals("tos", normalizeURI.getScheme());
    assertEquals("bucket", normalizeURI.getAuthority());
    assertEquals("/", normalizeURI.getPath());
  }
}
