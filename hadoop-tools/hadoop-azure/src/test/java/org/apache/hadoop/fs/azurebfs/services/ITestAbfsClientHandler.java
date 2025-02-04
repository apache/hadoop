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

package org.apache.hadoop.fs.azurebfs.services;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;

/**
 * Test AbfsClientHandler initialization.
 */
public class ITestAbfsClientHandler extends AbstractAbfsIntegrationTest {

  public ITestAbfsClientHandler() throws Exception{

  }

  /**
   * Test to verify Client Handler holds both type of clients, and they can be accessed as needed.
   * @throws Exception if test fails
   */
  @Test
  public void testAbfsClientHandlerInitialization() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AbfsClientHandler clientHandler = fs.getAbfsStore().getClientHandler();
    Assertions.assertThat(clientHandler.getClient(AbfsServiceType.DFS)).isInstanceOf(AbfsDfsClient.class);
    Assertions.assertThat(clientHandler.getClient(AbfsServiceType.BLOB)).isInstanceOf(AbfsBlobClient.class);
  }
}
