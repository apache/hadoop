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

package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

public class TestS3AContractRename extends AbstractContractRenameTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  public void testRenameDirIntoExistingDir() throws Throwable {
    describe("Verify renaming a dir into an existing dir puts the files"
             +" from the source dir into the existing dir"
             +" and leaves existing files alone");
    FileSystem fs = getFileSystem();
    String sourceSubdir = "source";
    Path srcDir = path(sourceSubdir);
    Path srcFilePath = new Path(srcDir, "source-256.txt");
    byte[] srcDataset = dataset(256, 'a', 'z');
    writeDataset(fs, srcFilePath, srcDataset, srcDataset.length, 1024, false);
    Path destDir = path("dest");

    Path destFilePath = new Path(destDir, "dest-512.txt");
    byte[] destDateset = dataset(512, 'A', 'Z');
    writeDataset(fs, destFilePath, destDateset, destDateset.length, 1024,
        false);
    assertIsFile(destFilePath);

    boolean rename = fs.rename(srcDir, destDir);
    assertFalse("s3a doesn't support rename to non-empty directory", rename);
  }
}
