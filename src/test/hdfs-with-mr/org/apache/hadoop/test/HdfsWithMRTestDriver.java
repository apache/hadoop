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

import org.apache.hadoop.fs.DFSCIOTest;
import org.apache.hadoop.fs.DistributedFSCheck;
import org.apache.hadoop.fs.TestDFSIO;
import org.apache.hadoop.fs.TestFileSystem;
import org.apache.hadoop.hdfs.NNBench;
import org.apache.hadoop.io.FileBench;
import org.apache.hadoop.util.ProgramDriver;

/*
 * Driver for HDFS tests, which require map-reduce to run.
 */
public class HdfsWithMRTestDriver {
  
  
  private ProgramDriver pgd;

  public HdfsWithMRTestDriver() {
    this(new ProgramDriver());
  }
  
  public HdfsWithMRTestDriver(ProgramDriver pgd) {
    this.pgd = pgd;
    try {
      pgd.addClass("nnbench", NNBench.class, 
          "A benchmark that stresses the namenode.");
      pgd.addClass("testfilesystem", TestFileSystem.class, 
          "A test for FileSystem read/write.");
      pgd.addClass("TestDFSIO", TestDFSIO.class, 
          "Distributed i/o benchmark.");
      pgd.addClass("DFSCIOTest", DFSCIOTest.class, "" +
          "Distributed i/o benchmark of libhdfs.");
      pgd.addClass("DistributedFSCheck", DistributedFSCheck.class, 
          "Distributed checkup of the file system consistency.");
      pgd.addClass("filebench", FileBench.class, 
          "Benchmark SequenceFile(Input|Output)Format " +
          "(block,record compressed and uncompressed), " +
          "Text(Input|Output)Format (compressed and uncompressed)");
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }

  public void run(String argv[]) {
    try {
      pgd.driver(argv);
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }

  public static void main(String argv[]){
    new HdfsWithMRTestDriver().run(argv);
  }
}

