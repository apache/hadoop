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

package org.apache.hadoop.fs.s3a;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCopyFromLocalTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.s3a.S3AContract;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestS3ACopyFromLocalFile extends
        AbstractContractCopyFromLocalTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Test
  public void testLocalFilesOnly() throws Throwable {
    describe("Copying into other file systems must fail");
    Path dest = fileToPath(createTempDirectory("someDir"));

    intercept(IllegalArgumentException.class,
        () -> getFileSystem().copyFromLocalFile(false, true, dest, dest));
  }

  @Test
  public void testOnlyFromLocal() throws Throwable {
    describe("Copying must be from a local file system");
    File source = createTempFile("someFile");
    Path dest = copyFromLocal(source, true);

    intercept(IllegalArgumentException.class,
        () -> getFileSystem().copyFromLocalFile(true, true, dest, dest));
  }
}
