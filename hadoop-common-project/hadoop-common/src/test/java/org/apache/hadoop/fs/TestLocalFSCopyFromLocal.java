/*
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

package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCopyFromLocalTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;
import org.junit.Test;

import java.io.File;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestLocalFSCopyFromLocal extends AbstractContractCopyFromLocalTest {
    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new LocalFSContract(conf);
    }

    @Test
    public void testSourceIsDirectoryAndDestinationIsFile() throws Throwable {
        describe("Source is a directory and destination is a file should fail");

        File file = createTempFile("local");
        File source = createTempDirectory("srcDir");
        Path destination = copyFromLocal(file, false);
        Path sourcePath = new Path(source.toURI());

        intercept(FileAlreadyExistsException.class,
                () -> getFileSystem().copyFromLocalFile(false, true,
                        sourcePath, destination));
    }
}
