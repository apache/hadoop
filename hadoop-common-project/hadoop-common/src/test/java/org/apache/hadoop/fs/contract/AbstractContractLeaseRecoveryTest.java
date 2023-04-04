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

package org.apache.hadoop.fs.contract;

import static org.apache.hadoop.fs.CommonPathCapabilities.LEASE_RECOVERABLE;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Path;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public abstract class AbstractContractLeaseRecoveryTest extends
  AbstractFSContractTestBase {

  @Test
  public void testLeaseRecovery() throws Throwable {
    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    ContractTestUtils.touch(fs, path);
    LeaseRecoverable leaseRecoverableFs = verifyAndGetLeaseRecoverableInstance(fs, path);

    Assertions.assertThat(leaseRecoverableFs.recoverLease(path))
      .describedAs("Issuing lease recovery on a closed file must be successful")
      .isTrue();

    Assertions.assertThat(leaseRecoverableFs.isFileClosed(path))
      .describedAs("Get the isFileClose status on a closed file must be successful")
      .isTrue();
  }

  @Test
  public void testLeaseRecoveryFileNotExist() throws Throwable {
    final Path path = new Path("notExist");
    final FileSystem fs = getFileSystem();
    LeaseRecoverable leaseRecoverableFs = verifyAndGetLeaseRecoverableInstance(fs, path);

    Assertions.assertThatThrownBy(() -> leaseRecoverableFs.recoverLease(path))
      .describedAs("Issuing lease recovery on a non exist file must throw exception")
      .hasMessageContaining("File does not exist")
      .isInstanceOf(FileNotFoundException.class);

    Assertions.assertThatThrownBy(() ->leaseRecoverableFs.isFileClosed(path))
      .describedAs("Get the isFileClose status on non exist file must throw exception")
      .hasMessageContaining("File does not exist")
      .isInstanceOf(FileNotFoundException.class);
  }

  @Test
  public void testLeaseRecoveryFileOnDirectory() throws Throwable {
    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    LeaseRecoverable leaseRecoverableFs = verifyAndGetLeaseRecoverableInstance(fs, path);
    final Path parentDirectory = path.getParent();

    Assertions.assertThatThrownBy(() -> leaseRecoverableFs.recoverLease(parentDirectory))
      .describedAs("Issuing lease recovery on a directory must throw exception")
      .hasMessageContaining("Path is not a file")
      .isInstanceOf(FileNotFoundException.class);

    Assertions.assertThatThrownBy(() ->leaseRecoverableFs.isFileClosed(parentDirectory))
      .describedAs("Get the isFileClose status on a directory must throw exception")
      .hasMessageContaining("Path is not a file")
      .isInstanceOf(FileNotFoundException.class);
  }

  private LeaseRecoverable verifyAndGetLeaseRecoverableInstance(FileSystem fs, Path path)
    throws IOException {
    Assertions.assertThat(fs.hasPathCapability(path, LEASE_RECOVERABLE))
      .describedAs("path capability %s of %s",
        LEASE_RECOVERABLE, path)
      .isTrue();
    Assertions.assertThat(fs instanceof LeaseRecoverable)
      .describedAs("File system %s must be an instance of %s",
        fs, LeaseRecoverable.class.getClass())
      .isTrue();
    // lease recovery function test
    return (LeaseRecoverable) fs;
  }
}
