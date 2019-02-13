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

package org.apache.hadoop.fs.shell;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.LinkedList;

import org.junit.Test;

/**
 * Test class to verify Tail shell command.
 */
public class TestTail {

  // check follow delay with -s parameter.
  @Test
  public void testSleepParameter() throws IOException {
    Tail tail = new Tail();
    LinkedList<String> options = new LinkedList<String>();
    options.add("-f");
    options.add("-s");
    options.add("10000");
    options.add("/path");
    tail.processOptions(options);
    assertEquals(10000, tail.getFollowDelay());
  }

  // check follow delay without -s parameter.
  @Test
  public void testFollowParameter() throws IOException {
    Tail tail = new Tail();
    LinkedList<String> options = new LinkedList<String>();
    options.add("-f");
    options.add("/path");
    tail.processOptions(options);
    // Follow delay should be the default 5000 ms.
    assertEquals(5000, tail.getFollowDelay());
  }
}