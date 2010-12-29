/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Testing Testing {@link Pair#equals(Object)} for deep checking of arrays
 */
public class TestPairEquals {

  /**
   * Testing {@link Pair#equals(Object)} for deep checking of arrays
   */
  @Test
  public void testEquals() {
    Pair<String, String> p1 = new Pair<String, String>("Hello", "World");
    Pair<String, String> p1a = new Pair<String, String>("Hello", "World");
    assertTrue(p1.equals(p1a));

    Pair<String, byte[]> p2 = new Pair<String, byte[]>("Hello", new byte[] { 1,
        0, 5 });
    Pair<String, byte[]> p2a = new Pair<String, byte[]>("Hello", new byte[] {
        1, 0, 5 });
    // Previously this test would fail as they are two different pointers to
    // arrays that inherently the same.
    assertTrue(p2.equals(p2a));

    Pair<char[], String> p3 = new Pair<char[], String>(new char[] { 'h', 'e' },
        "world");
    assertTrue(p3.equals(p3));

    // These kinds of tests will still fail as they have fundamentally different
    // elements
    Pair<Character[], String> p4 = new Pair<Character[], String>(
        new Character[] { new Character('h'), new Character('e') }, "world");
    // checking for autoboxing non-equality to the original class
    assertFalse(p3.equals(p4));

    // still fail for a different autoboxing situation (just to prove that it
    // is not just chars)
    Pair<String, Integer[]> p5 = new Pair<String, Integer[]>("hello",
        new Integer[] { new Integer(1), new Integer(982) });
    Pair<String, int[]> p5a = new Pair<String, int[]>("hello", new int[] { 1,
        982 });
    assertFalse(p5.equals(p5a));

    // will still fail for that different things
    Pair<String, byte[]> p6 = new Pair<String, byte[]>("Hello", new byte[] { 1,
        0, 4 });
    assertFalse(p2.equals(p6));

    // will still fail for the other predicate being different
    Pair<String, byte[]> p7 = new Pair<String, byte[]>("World", new byte[] { 1,
        0, 5 });
    assertFalse(p2.equals(p7));
  }
}
