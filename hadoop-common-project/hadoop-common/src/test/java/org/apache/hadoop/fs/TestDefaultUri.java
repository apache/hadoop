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

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

/**
 * Test default URI related APIs in {@link FileSystem}.
 */
public class TestDefaultUri {
  private Configuration conf = new Configuration();

  @Test
  public void tetGetDefaultUri() {
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://nn_host");
    URI uri = FileSystem.getDefaultUri(conf);
    assertThat(uri.getScheme(), is("hdfs"));
    assertThat(uri.getAuthority(), is("nn_host"));
  }

  @Test
  public void tetGetDefaultUriWithPort() {
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://nn_host:5432");
    URI uri = FileSystem.getDefaultUri(conf);
    assertThat(uri.getScheme(), is("hdfs"));
    assertThat(uri.getAuthority(), is("nn_host:5432"));
  }

  @Test
  public void tetGetDefaultUriTrailingSlash() {
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://nn_host/");
    URI uri = FileSystem.getDefaultUri(conf);
    assertThat(uri.getScheme(), is("hdfs"));
    assertThat(uri.getAuthority(), is("nn_host"));
  }

  @Test
  public void tetGetDefaultUriNoScheme() {
    conf.set(FS_DEFAULT_NAME_KEY, "nn_host");
    URI uri = FileSystem.getDefaultUri(conf);
    assertThat(uri.getScheme(), is("hdfs"));
    assertThat(uri.getAuthority(), is("nn_host"));
  }

  @Test
  public void tetGetDefaultUriNoSchemeTrailingSlash() {
    conf.set(FS_DEFAULT_NAME_KEY, "nn_host/");
    try {
      FileSystem.getDefaultUri(conf);
      fail("Expect IAE: No scheme in default FS");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "No scheme in default FS", e);
    }
  }

  @Test
  public void tetFsGet() throws IOException {
    conf.set(FS_DEFAULT_NAME_KEY, "file:///");
    FileSystem fs = FileSystem.get(conf);
    assertThat(fs, instanceOf(LocalFileSystem.class));
  }

  @Test
  public void tetFsGetNoScheme() throws IOException {
    // Bare host name or address indicates hdfs scheme
    conf.set(FS_DEFAULT_NAME_KEY, "nn_host");
    try {
      FileSystem.get(conf);
      fail("Expect IOE: No FileSystem for scheme: hdfs");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "No FileSystem for scheme: hdfs", e);
    }
  }

  @Test
  public void tetFsGetNoSchemeTrailingSlash() throws IOException {
    // Bare host name or address with trailing slash is invalid
    conf.set(FS_DEFAULT_NAME_KEY, "nn_host/");
    try {
      FileSystem.get(conf);
      fail("Expect IAE: No scheme in default FS");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "No scheme in default FS", e);
    }
  }
}
