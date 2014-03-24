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

import org.apache.hadoop.hdfs.BenchmarkThroughput;
import org.apache.hadoop.util.ProgramDriver;

/**
 * Driver for HDFS tests. The tests should NOT depend on map-reduce APIs.
 */
public class HdfsTestDriver {

  private final ProgramDriver pgd;

  public HdfsTestDriver() {
    this(new ProgramDriver());
  }
  
  public HdfsTestDriver(ProgramDriver pgd) {
    this.pgd = pgd;
    try {
      pgd.addClass("dfsthroughput", BenchmarkThroughput.class, 
          "measure hdfs throughput");
      pgd.addClass("minidfscluster", MiniDFSClusterManager.class, 
          "Run a single-process mini DFS cluster");
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
    new HdfsTestDriver().run(argv);
  }
}
