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

package org.apache.hadoop.test;

import org.apache.hadoop.io.TestArrayFile;
import org.apache.hadoop.io.TestSetFile;
import org.apache.hadoop.ipc.TestIPC;
import org.apache.hadoop.ipc.TestRPC;
import org.apache.hadoop.util.ProgramDriver;

/**
 * Driver for core tests.
 */
public class CoreTestDriver {

  private ProgramDriver pgd;
  
  public CoreTestDriver() {
    this(new ProgramDriver());
  }
  
  public CoreTestDriver(ProgramDriver pgd) {
    this.pgd = pgd;
    try {
      pgd.addClass("testsetfile", TestSetFile.class, 
          "A test for flat files of binary key/value pairs.");
      pgd.addClass("testarrayfile", TestArrayFile.class, 
          "A test for flat files of binary key/value pairs.");
      pgd.addClass("testrpc", TestRPC.class, "A test for rpc.");
      pgd.addClass("testipc", TestIPC.class, "A test for ipc.");
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }

  public void run(String argv[]) {
    int exitCode = -1;
    try {
      exitCode = pgd.run(argv);
    } catch(Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);
  }

  public static void main(String argv[]){
    new CoreTestDriver().run(argv);
  }
}
