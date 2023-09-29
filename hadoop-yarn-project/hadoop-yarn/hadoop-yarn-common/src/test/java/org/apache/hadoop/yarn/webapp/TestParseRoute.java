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

package org.apache.hadoop.yarn.webapp;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestParseRoute {

  @Test
  void testNormalAction() {
    assertEquals(Arrays.asList("/foo/action", "foo", "action", ":a1", ":a2"),
        WebApp.parseRoute("/foo/action/:a1/:a2"));
  }

  @Test
  void testDefaultController() {
    assertEquals(Arrays.asList("/", "default", "index"),
        WebApp.parseRoute("/"));
  }

  @Test
  void testDefaultAction() {
    assertEquals(Arrays.asList("/foo", "foo", "index"),
        WebApp.parseRoute("/foo"));
    assertEquals(Arrays.asList("/foo", "foo", "index"),
        WebApp.parseRoute("/foo/"));
  }

  @Test
  void testMissingAction() {
    assertEquals(Arrays.asList("/foo", "foo", "index", ":a1"),
        WebApp.parseRoute("/foo/:a1"));
  }

  @Test
  void testDefaultCapture() {
    assertEquals(Arrays.asList("/", "default", "index", ":a"),
        WebApp.parseRoute("/:a"));
  }

  @Test
  void testPartialCapture1() {
    assertEquals(Arrays.asList("/foo/action/bar", "foo", "action", "bar", ":a"),
        WebApp.parseRoute("/foo/action/bar/:a"));
  }

  @Test
  void testPartialCapture2() {
    assertEquals(Arrays.asList("/foo/action", "foo", "action", ":a1", "bar",
            ":a2", ":a3"),
        WebApp.parseRoute("/foo/action/:a1/bar/:a2/:a3"));
  }

  @Test
  void testLeadingPaddings() {
    assertEquals(Arrays.asList("/foo/action", "foo", "action", ":a"),
        WebApp.parseRoute(" /foo/action/ :a"));
  }

  @Test
  void testTrailingPaddings() {
    assertEquals(Arrays.asList("/foo/action", "foo", "action", ":a"),
        WebApp.parseRoute("/foo/action//:a / "));
    assertEquals(Arrays.asList("/foo/action", "foo", "action"),
        WebApp.parseRoute("/foo/action / "));
  }

  @Test
  void testMissingLeadingSlash() {
    assertThrows(WebAppException.class, () -> {
      WebApp.parseRoute("foo/bar");
    });
  }
}
