/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.rawlocal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;

public class TestRawlocalContractRename extends AbstractContractRenameTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new RawlocalFSContract(conf);
  }
  
  /**
   * Test fallback rename code <code>handleEmptyDstDirectoryOnWindows()</code>
   * even on not Windows platform where the normal <code>File.renameTo()</code>
   * is supposed to work well. This test has been added for HADOOP-9805.
   * 
   * @see AbstractContractRenameTest#testRenameWithNonEmptySubDirPOSIX()
   */
  @Test
  public void testRenameWithNonEmptySubDirPOSIX() throws Throwable {
    final Path renameTestDir = path("testRenameWithNonEmptySubDir");
    final Path srcDir = new Path(renameTestDir, "src1");
    final Path srcSubDir = new Path(srcDir, "sub");
    final Path finalDir = new Path(renameTestDir, "dest");
    FileSystem fs = getFileSystem();
    ContractTestUtils.rm(fs, renameTestDir, true, false);

    fs.mkdirs(srcDir);
    fs.mkdirs(finalDir);
    ContractTestUtils.writeTextFile(fs, new Path(srcDir, "source.txt"),
        "this is the file in src dir", false);
    ContractTestUtils.writeTextFile(fs, new Path(srcSubDir, "subfile.txt"),
        "this is the file in src/sub dir", false);

    ContractTestUtils.assertPathExists(fs, "not created in src dir",
        new Path(srcDir, "source.txt"));
    ContractTestUtils.assertPathExists(fs, "not created in src/sub dir",
        new Path(srcSubDir, "subfile.txt"));
    
    RawLocalFileSystem rlfs = (RawLocalFileSystem) fs;
    rlfs.handleEmptyDstDirectoryOnWindows(srcDir, rlfs.pathToFile(srcDir),
        finalDir, rlfs.pathToFile(finalDir));
    
    // Accept only POSIX rename behavior in this test
    ContractTestUtils.assertPathExists(fs, "not renamed into dest dir",
        new Path(finalDir, "source.txt"));
    ContractTestUtils.assertPathExists(fs, "not renamed into dest/sub dir",
        new Path(finalDir, "sub/subfile.txt"));
    
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted",
        new Path(srcDir, "source.txt"));
  }

}
