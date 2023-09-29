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

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests of auth path resolution.
 */
public class TestAuthoritativePath extends AbstractHadoopTestBase {

  private final Path root = new Path("/");

  private URI fsUri;

  private static final String BASE = "s3a://bucket";

  @Before
  public void setup() throws Exception {
    fsUri = new URI(BASE +"/");
  }

  private Configuration authPathsConf(String... paths) {
    Configuration conf = new Configuration(false);
    conf.set(AUTHORITATIVE_PATH, String.join(",", paths));
    return conf;
  }

  @Test
  public void testResolution() throws Throwable {
    assertAuthPaths(l("/one"), "/one/");
  }

  @Test
  public void testResolutionWithFQP() throws Throwable {
    assertAuthPaths(l("/one/",
        BASE + "/two/"),
        "/one/", "/two/");
  }
  @Test
  public void testOtherBucket() throws Throwable {
    assertAuthPaths(l("/one/",
        "s3a://landsat-pds/",
        BASE + "/two/"),
        "/one/", "/two/");
  }

  @Test
  public void testOtherScheme() throws Throwable {
    assertAuthPaths(l("/one/",
        "s3a://landsat-pds/",
        "http://bucket/two/"),
        "/one/");
  }

  /**
   * Get the auth paths; qualification is through
   * Path.makeQualified not the FS near-equivalent.
   * @param conf configuration
   * @return list of auth paths.
   */
  private Collection<String> getAuthoritativePaths(
      Configuration conf) {

    return S3Guard.getAuthoritativePaths(fsUri, conf,
        p -> {
          Path q = p.makeQualified(fsUri, root);
          assertThat(q.toUri().getAuthority())
              .describedAs("Path %s", q)
              .isEqualTo(fsUri.getAuthority());
          return S3AUtils.maybeAddTrailingSlash(q.toString());
        });
  }

  /**
   * take a varargs list and and return as an array.
   * @param s source
   * @return the values
   */
  private String[] l(String...s) {
    return s;
  }

  /**
   * Assert that the authoritative paths from a source list
   * are that expected.
   * @param src source entries to set as auth paths
   * @param expected the list of auth paths for a filesystem
   */
  private void assertAuthPaths(String[] src, String...expected) {
    Configuration conf = authPathsConf(src);
    List<String> collect = Arrays.stream(expected)
        .map(s -> BASE + s)
        .collect(Collectors.toList());
    Collection<String> paths = getAuthoritativePaths(conf);
    assertThat(paths)
        .containsExactlyInAnyOrderElementsOf(collect);
  }

}
