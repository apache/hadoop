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

import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.mapred.TestMapRed;
import org.apache.hadoop.mapred.TestTextInputFormat;
import org.apache.hadoop.mapred.TestSequenceFileInputFormat;
import org.apache.hadoop.dfs.ClusterTestDFS;
import org.apache.hadoop.fs.TestFileSystem;
import org.apache.hadoop.io.TestArrayFile;
import org.apache.hadoop.io.TestSetFile;
import org.apache.hadoop.io.TestSequenceFile;
import org.apache.hadoop.ipc.TestIPC;
import org.apache.hadoop.ipc.TestRPC;
import org.apache.hadoop.fs.DistributedFSCheck;
import org.apache.hadoop.fs.TestDFSIO;
import org.apache.hadoop.fs.DFSCIOTest;

public class AllTestDriver {
  
  /**
   * A description of the test program for running all the tests using jar file
   * @date April 2006
   */
    
    public static void main(String argv[]){
	ProgramDriver pgd = new ProgramDriver();
	try {
	    pgd.addClass("mapredtest", TestMapRed.class, "A map/reduce test check.");
	    pgd.addClass("clustertestdfs", ClusterTestDFS.class, "A pseudo distributed test for DFS.");
	    pgd.addClass("testfilesystem", TestFileSystem.class, "A test for FileSystem read/write.");
	    pgd.addClass("testsequencefile", TestSequenceFile.class, "A test for flat files of binary key value pairs.");
	    pgd.addClass("testsetfile", TestSetFile.class, "A test for flat files of binary key/value pairs.");
	    pgd.addClass("testarrayfile", TestArrayFile.class, "A test for flat files of binary key/value pairs.");
	    pgd.addClass("testrpc", TestRPC.class, "A test for rpc.");
	    pgd.addClass("testipc", TestIPC.class, "A test for ipc.");
	    pgd.addClass("testsequencefileinputformat", TestSequenceFileInputFormat.class, "A test for sequence file input format.");
	    pgd.addClass("testtextinputformat", TestTextInputFormat.class, "A test for text input format.");
      pgd.addClass("TestDFSIO", TestDFSIO.class, "Distributed i/o benchmark.");
      pgd.addClass("DFSCIOTest", DFSCIOTest.class, "Distributed i/o benchmark of libhdfs.");
      pgd.addClass("DistributedFSCheck", DistributedFSCheck.class, "Distributed checkup of the file system consistency.");
	    pgd.driver(argv);
	}
	catch(Throwable e){
	    e.printStackTrace();
	}
    }
}
