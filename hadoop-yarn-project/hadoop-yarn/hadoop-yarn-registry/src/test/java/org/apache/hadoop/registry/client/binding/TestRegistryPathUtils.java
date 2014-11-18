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

package org.apache.hadoop.registry.client.binding;

import static org.apache.hadoop.registry.client.binding.RegistryPathUtils.*;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.exceptions.InvalidPathnameException;
import org.junit.Assert;
import org.junit.Test;

public class TestRegistryPathUtils extends Assert {


  public static final String EURO = "\u20AC";

  @Test
  public void testFormatAscii() throws Throwable {

    String in = "hostname01101101-1";
    assertConverted(in, in);
  }

  /*
  * Euro symbol
   */
  @Test
  public void testFormatEuroSymbol() throws Throwable {
    assertConverted("xn--lzg", EURO);
  }

  @Test
  public void testFormatIdempotent() throws Throwable {
    assertConverted("xn--lzg", RegistryPathUtils.encodeForRegistry(EURO));
  }

  @Test
  public void testFormatCyrillicSpaced() throws Throwable {
    assertConverted("xn--pa 3-k4di", "\u0413PA\u0414 3");
  }

  protected void assertConverted(String expected, String in) {
    String out = RegistryPathUtils.encodeForRegistry(in);
    assertEquals("Conversion of " + in, expected, out);
  }

  @Test
  public void testPaths() throws Throwable {
    assertCreatedPathEquals("/", "/", "");
    assertCreatedPathEquals("/", "", "");
    assertCreatedPathEquals("/", "", "/");
    assertCreatedPathEquals("/", "/", "/");

    assertCreatedPathEquals("/a", "/a", "");
    assertCreatedPathEquals("/a", "/", "a");
    assertCreatedPathEquals("/a/b", "/a", "b");
    assertCreatedPathEquals("/a/b", "/a/", "b");
    assertCreatedPathEquals("/a/b", "/a", "/b");
    assertCreatedPathEquals("/a/b", "/a", "/b/");
    assertCreatedPathEquals("/a", "/a", "/");
    assertCreatedPathEquals("/alice", "/", "/alice");
    assertCreatedPathEquals("/alice", "/alice", "/");
  }




  @Test
  public void testComplexPaths() throws Throwable {
    assertCreatedPathEquals("/", "", "");
    assertCreatedPathEquals("/yarn/registry/users/hadoop/org-apache-hadoop",
        "/yarn/registry",
        "users/hadoop/org-apache-hadoop/");
  }


  private static void assertCreatedPathEquals(String expected, String base,
      String path) throws IOException {
    String fullPath = createFullPath(base, path);
    assertEquals("\"" + base + "\" + \"" + path + "\" =\"" + fullPath + "\"",
        expected, fullPath);
  }

  @Test
  public void testSplittingEmpty() throws Throwable {
    assertEquals(0, split("").size());
    assertEquals(0, split("/").size());
    assertEquals(0, split("///").size());
  }


  @Test
  public void testSplitting() throws Throwable {
    assertEquals(1, split("/a").size());
    assertEquals(0, split("/").size());
    assertEquals(3, split("/a/b/c").size());
    assertEquals(3, split("/a/b/c/").size());
    assertEquals(3, split("a/b/c").size());
    assertEquals(3, split("/a/b//c").size());
    assertEquals(3, split("//a/b/c/").size());
    List<String> split = split("//a/b/c/");
    assertEquals("a", split.get(0));
    assertEquals("b", split.get(1));
    assertEquals("c", split.get(2));
  }

  @Test
  public void testParentOf() throws Throwable {
    assertEquals("/", parentOf("/a"));
    assertEquals("/", parentOf("/a/"));
    assertEquals("/a", parentOf("/a/b"));
    assertEquals("/a/b", parentOf("/a/b/c"));
  }

  @Test
  public void testLastPathEntry() throws Throwable {
    assertEquals("",lastPathEntry("/"));
    assertEquals("",lastPathEntry("//"));
    assertEquals("c",lastPathEntry("/a/b/c"));
    assertEquals("c",lastPathEntry("/a/b/c/"));
  }

  @Test(expected = PathNotFoundException.class)
  public void testParentOfRoot() throws Throwable {
    parentOf("/");
  }

  @Test
  public void testValidPaths() throws Throwable {
    assertValidPath("/");
    assertValidPath("/a/b/c");
    assertValidPath("/users/drwho/org-apache-hadoop/registry/appid-55-55");
    assertValidPath("/a50");
  }

  @Test
  public void testInvalidPaths() throws Throwable {
    assertInvalidPath("/a_b");
    assertInvalidPath("/UpperAndLowerCase");
    assertInvalidPath("/space in string");
// Is this valid?    assertInvalidPath("/50");
  }


  private void assertValidPath(String path) throws InvalidPathnameException {
    validateZKPath(path);
  }


  private void assertInvalidPath(String path) throws InvalidPathnameException {
    try {
      validateElementsAsDNS(path);
      fail("path considered valid: " + path);
    } catch (InvalidPathnameException expected) {
      // expected
    }
  }


}
