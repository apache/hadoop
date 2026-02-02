/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Simple test case to test the Circular Iterator.
 */
public class TestCircularIterator {

  @Test
  public void testIteration() throws Exception {
    List<String> list = Arrays.asList("a", "b", "c", "d");
    CircularIterator<String> ci =
        new CircularIterator<>(null, list.iterator(), list);
    StringBuilder sb = new StringBuilder("");
    while (ci.hasNext()) {
      sb.append(ci.next());
    }
    assertEquals("abcd", sb.toString());

    Iterator<String> lIter = list.iterator();
    lIter.next();
    lIter.next();
    sb = new StringBuilder("");
    ci = new CircularIterator<>(null, lIter, list);
    while (ci.hasNext()) {
      sb.append(ci.next());
    }
    assertEquals("cdab", sb.toString());

    lIter = list.iterator();
    lIter.next();
    lIter.next();
    lIter.next();
    sb = new StringBuilder("");
    ci = new CircularIterator<>("x", lIter, list);
    while (ci.hasNext()) {
      sb.append(ci.next());
    }
    assertEquals("xdabc", sb.toString());

    list = Arrays.asList("a");
    lIter = list.iterator();
    lIter.next();
    sb = new StringBuilder("");
    ci = new CircularIterator<>("y", lIter, list);
    while (ci.hasNext()) {
      sb.append(ci.next());
    }
    assertEquals("ya", sb.toString());

    try {
      list = new ArrayList<>();
      lIter = list.iterator();
      new CircularIterator<>("y", lIter, list);
      fail("Should fail..");
    } catch (Exception e) {
      // foo bar
    }
  }
}
