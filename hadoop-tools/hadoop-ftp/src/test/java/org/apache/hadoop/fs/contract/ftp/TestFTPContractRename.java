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

package org.apache.hadoop.fs.contract.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.ftp.FTPFileSystem;

import java.io.IOException;

public class TestFTPContractRename extends AbstractContractRenameTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new FTPContract(conf);
  }

  /**
   * Check the exception was about cross-directory renames
   * -if not, rethrow it.
   * @param e exception raised
   * @throws IOException
   */
  private void verifyUnsupportedDirRenameException(IOException e) throws IOException {
    if (!e.toString().contains(FTPFileSystem.E_SAME_DIRECTORY_ONLY)) {
      throw e;
    }
  }

  @Override
  public void testRenameDirIntoExistingDir() throws Throwable {
    try {
      super.testRenameDirIntoExistingDir();
      fail("Expected a failure");
    } catch (IOException e) {
      verifyUnsupportedDirRenameException(e);
    }
  }

  @Override
  public void testRenameFileNonexistentDir() throws Throwable {
    try {
      super.testRenameFileNonexistentDir();
      fail("Expected a failure");
    } catch (IOException e) {
      verifyUnsupportedDirRenameException(e);
    }
  }
}
