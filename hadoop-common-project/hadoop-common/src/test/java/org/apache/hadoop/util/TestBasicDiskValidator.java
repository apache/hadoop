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
package org.apache.hadoop.util;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.util.DiskChecker.DiskErrorException;

import java.io.File;

/**
 * The class to test BasicDiskValidator.
 */
public class TestBasicDiskValidator extends TestDiskChecker {
  @Override
  protected void checkDirs(boolean isDir, String perm, boolean success)
      throws Throwable {
    File localDir = isDir ? createTempDir() : createTempFile();
    try {
      Shell.execCommand(Shell.getSetPermissionCommand(perm, false,
          localDir.getAbsolutePath()));

      DiskValidatorFactory.getInstance(BasicDiskValidator.NAME).
          checkStatus(localDir);
      assertTrue("call to checkDir() succeeded.", success);
    } catch (DiskErrorException e) {
      // call to checkDir() succeeded even though it was expected to fail
      // if success is false, otherwise throw the exception
      if (success) {
        throw e;
      }
    } finally {
      localDir.delete();
    }
  }
}
