/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.io.Text;

public class TestTextSequence extends HBaseTestCase {

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }
  
  /**
   * Test compares of TextSequences and of TextSequence to Text.
   * @throws Exception
   */
  public void testCompare() throws Exception {
    final Text a = new Text("abcdef");
    final Text b = new Text("defghi");
    TextSequence as = new TextSequence(a, 3);
    TextSequence bs = new TextSequence(b, 0, 3);
    assertTrue(as.compareTo(bs) == 0);
    assertTrue(as.equals(bs));
    // Test where one is a Text and other is a TextSequence
    final Text family = new Text("abc:");
    final Text column = new Text(family.toString() + "qualifier");
    final TextSequence ts = new TextSequence(column, 0, family.getLength());
    assertTrue(ts.compareTo(family) == 0);
    assertTrue(ts.equals(family));
  }
}