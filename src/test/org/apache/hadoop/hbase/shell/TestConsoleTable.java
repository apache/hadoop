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
package org.apache.hadoop.hbase.shell;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * Test the console table class
 * TODO: Console table needs fixing.
 */
public class TestConsoleTable extends TestCase {
  public void testPrintLine() {
    ConsoleTable.printLine(0, "smallkey", "smallcolumn", "smallcelldata");
    ConsoleTable.printLine(0, "a large key too big for column", "smallcolumn",
      "smallcelldata");
    ConsoleTable.printLine(0, "smallkey", "smallcolumn", "smallcelldata");
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(TestConsoleTable.class));
  }
}
