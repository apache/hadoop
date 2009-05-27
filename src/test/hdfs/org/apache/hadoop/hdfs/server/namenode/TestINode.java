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

package org.apache.hadoop.hdfs.server.namenode;

import junit.framework.TestCase;
import static org.junit.Assert.*;

public class TestINode extends TestCase {
  public void testPathNames() {
    assertArrayEquals(new String[]{"", "foo", "bar"},
                      INode.getPathNames("/foo/bar"));
    assertArrayEquals(new String[]{"", "foo"},
                      INode.getPathNames("/foo"));

    byte[][] comps = INode.getPathComponents("/foo/bar");
    assertEquals(3, comps.length);
    assertArrayEquals("".getBytes(), comps[0]);
    assertArrayEquals("foo".getBytes(), comps[1]);
    assertArrayEquals("bar".getBytes(), comps[2]);

    boolean thrown = false;
    try {
      INode.getPathComponents("foo");
    } catch (IllegalArgumentException iae) {
      thrown = true;
    }
    assertTrue(thrown);
  }
}