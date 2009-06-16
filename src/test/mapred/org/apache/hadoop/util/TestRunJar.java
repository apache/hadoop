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
package org.apache.hadoop.util;


import java.io.File;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

/**
 * A test to rest the RunJar class.
 */
public class TestRunJar extends TestCase {
  
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp")).toString();
  
  public void testRunjar() throws Throwable {
  
   File outFile = new File(TEST_ROOT_DIR, "out");
     // delete if output file already exists.
    if (outFile.exists()) {
      outFile.delete();
    }
    
    String[] args = new String[3];
    args[0] = "build/test/mapred/testjar/testjob.jar";
    args[1] = "testjar.Hello";
    args[2] = outFile.toString();
    RunJar.main(args);
    assertTrue("RunJar failed", outFile.exists());
  }
}
