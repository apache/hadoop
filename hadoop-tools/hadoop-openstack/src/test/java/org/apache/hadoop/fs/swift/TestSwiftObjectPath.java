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
package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.http.RestClientBindings;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.fs.swift.util.SwiftUtils;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for SwiftObjectPath class.
 */
public class TestSwiftObjectPath implements SwiftTestConstants {
  private static final Log LOG = LogFactory.getLog(TestSwiftObjectPath.class);

  /**
   * What an endpoint looks like. This is derived from a (valid)
   * rackspace endpoint address
   */
  private static final String ENDPOINT =
          "https://storage101.region1.example.org/v1/MossoCloudFS_9fb40cc0-1234-5678-9abc-def000c9a66";

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testParsePath() throws Exception {
    final String pathString = "/home/user/files/file1";
    final Path path = new Path(pathString);
    final URI uri = new URI("http://container.localhost");
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(uri, path);
    final SwiftObjectPath actual = new SwiftObjectPath(
            RestClientBindings.extractContainerName(uri),
            pathString);

    assertEquals(expected, actual);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testParseUrlPath() throws Exception {
    final String pathString = "swift://container.service1/home/user/files/file1";
    final URI uri = new URI(pathString);
    final Path path = new Path(pathString);
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(uri, path);
    final SwiftObjectPath actual = new SwiftObjectPath(
            RestClientBindings.extractContainerName(uri),
            "/home/user/files/file1");

    assertEquals(expected, actual);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testHandleUrlAsPath() throws Exception {
    final String hostPart = "swift://container.service1";
    final String pathPart = "/home/user/files/file1";
    final String uriString = hostPart + pathPart;

    final SwiftObjectPath expected = new SwiftObjectPath(uriString, pathPart);
    final SwiftObjectPath actual = new SwiftObjectPath(uriString, uriString);

    assertEquals(expected, actual);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testParseAuthenticatedUrl() throws Exception {
    final String pathString = "swift://container.service1/v2/AUTH_00345h34l93459y4/home/tom/documents/finance.docx";
    final URI uri = new URI(pathString);
    final Path path = new Path(pathString);
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(uri, path);
    final SwiftObjectPath actual = new SwiftObjectPath(
            RestClientBindings.extractContainerName(uri),
            "/home/tom/documents/finance.docx");

    assertEquals(expected, actual);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testConvertToPath() throws Throwable {
    String initialpath = "/dir/file1";
    Path ipath = new Path(initialpath);
    SwiftObjectPath objectPath = SwiftObjectPath.fromPath(new URI(initialpath),
            ipath);
    URI endpoint = new URI(ENDPOINT);
    URI uri = SwiftRestClient.pathToURI(objectPath, endpoint);
    LOG.info("Inital Hadoop Path =" + initialpath);
    LOG.info("Merged URI=" + uri);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRootDirProbeEmptyPath() throws Throwable {
    SwiftObjectPath object=new SwiftObjectPath("container","");
    assertTrue(SwiftUtils.isRootDir(object));
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRootDirProbeRootPath() throws Throwable {
    SwiftObjectPath object=new SwiftObjectPath("container","/");
    assertTrue(SwiftUtils.isRootDir(object));
  }

  private void assertParentOf(SwiftObjectPath p1, SwiftObjectPath p2) {
    assertTrue(p1.toString() + " is not a parent of " + p2 ,p1.isEqualToOrParentOf(
      p2));
  }

  private void assertNotParentOf(SwiftObjectPath p1, SwiftObjectPath p2) {
    assertFalse(p1.toString() + " is a parent of " + p2, p1.isEqualToOrParentOf(
      p2));
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testChildOfProbe() throws Throwable {
    SwiftObjectPath parent = new SwiftObjectPath("container",
                                                 "/parent");
    SwiftObjectPath parent2 = new SwiftObjectPath("container",
                                                 "/parent2");
    SwiftObjectPath child = new SwiftObjectPath("container",
                                                 "/parent/child");
    SwiftObjectPath sibling = new SwiftObjectPath("container",
                                                 "/parent/sibling");
    SwiftObjectPath grandchild = new SwiftObjectPath("container",
                                                     "/parent/child/grandchild");
    assertParentOf(parent, child);
    assertParentOf(parent, grandchild);
    assertParentOf(child, grandchild);
    assertParentOf(parent, parent);
    assertNotParentOf(child, parent);
    assertParentOf(child, child);
    assertNotParentOf(parent, parent2);
    assertNotParentOf(grandchild, parent);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testChildOfRoot() throws Throwable {
    SwiftObjectPath root = new SwiftObjectPath("container", "/");
    SwiftObjectPath child = new SwiftObjectPath("container", "child");
    SwiftObjectPath grandchild = new SwiftObjectPath("container",
                                                     "/child/grandchild");
    assertParentOf(root, child);
    assertParentOf(root, grandchild);
    assertParentOf(child, grandchild);
    assertParentOf(root, root);
    assertNotParentOf(child, root);
    assertParentOf(child, child);
    assertNotParentOf(grandchild, root);
  }

}
