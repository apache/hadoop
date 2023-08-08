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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.SafeMode;
import org.apache.hadoop.fs.SafeModeAction;

public abstract class AbstractContractSafeModeTest extends AbstractFSContractTestBase {

  @Test
  public void testSafeMode() throws Throwable {
    final FileSystem fs = getFileSystem();
    SafeMode fsWithSafeMode = verifyAndGetSafeModeInstance(fs);
    Assertions.assertThat(fsWithSafeMode.setSafeMode(SafeModeAction.GET))
        .describedAs("Getting the status of safe mode before entering should be off.").isFalse();
    Assertions.assertThat(fsWithSafeMode.setSafeMode(SafeModeAction.ENTER))
        .describedAs("Entering Safe mode and safe mode turns on.").isTrue();
    Assertions.assertThat(fsWithSafeMode.setSafeMode(SafeModeAction.GET))
        .describedAs("Getting the status of safe mode after entering, safe mode should be on.")
        .isTrue();
    Assertions.assertThat(fsWithSafeMode.setSafeMode(SafeModeAction.LEAVE))
        .describedAs("Leaving safe mode, and safe mode switches off.").isFalse();
    Assertions.assertThat(fsWithSafeMode.setSafeMode(SafeModeAction.FORCE_EXIT))
        .describedAs("Force exist safe mode at any time, safe mode should always switches off.")
        .isFalse();
  }

  private SafeMode verifyAndGetSafeModeInstance(FileSystem fs) {
    Assertions.assertThat(fs)
        .describedAs("File system %s must be an instance of %s", fs, SafeMode.class.getClass())
        .isInstanceOf(SafeMode.class);
    return (SafeMode) fs;
  }
}
