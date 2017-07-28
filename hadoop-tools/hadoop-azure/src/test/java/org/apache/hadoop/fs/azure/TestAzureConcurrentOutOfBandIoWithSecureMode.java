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

package org.apache.hadoop.fs.azure;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

/**
 * Extends TestAzureConcurrentOutOfBandIo in order to run testReadOOBWrites with secure mode
 * (fs.azure.secure.mode) both enabled and disabled.
 */
public class TestAzureConcurrentOutOfBandIoWithSecureMode extends  TestAzureConcurrentOutOfBandIo {

  // Overridden TestCase methods.
  @Before
  @Override
  public void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createOutOfBandStore(
        UPLOAD_BLOCK_SIZE, DOWNLOAD_BLOCK_SIZE, true);
    assumeNotNull(testAccount);
  }
}