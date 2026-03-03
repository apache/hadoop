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

import java.net.URI;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import static org.apache.hadoop.fs.azure.NativeAzureFileSystem.WASB_INIT_ERROR_MESSAGE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test to verify WASB initialization fails as expected.
 */
public class TestWasbInitFailure {

  /**
   * Test that initialization of Non-secure WASB FileSystem fails as expected.
   * @throws Exception on any failure
   */
  @Test
  public void testWasbInitFails() throws Exception {
    URI wasbUri = URI.create("wasb://container@account.blob.core.windows.net");
    assertFailure(wasbUri);
  }

  /**
   * Test that initialization of Secure WASB FileSystem fails as expected.
   * @throws Exception on any failure
   */
  @Test
  public void testSecureWasbInitFails() throws Exception {
    URI wasbUri = URI.create("wasbs://container@account.blob.core.windows.net");
    assertFailure(wasbUri);
  }

  private void assertFailure(URI uri) throws Exception {
    Configuration conf = new Configuration();
    UnsupportedOperationException ex = intercept(UnsupportedOperationException.class, () -> {
      FileSystem.newInstance(uri, conf).close();
    });
    Assertions.assertThat(ex.getMessage())
        .contains(WASB_INIT_ERROR_MESSAGE);

    ex = intercept(UnsupportedOperationException.class, () -> {
      FileSystem.get(uri, conf).close();
    });
    Assertions.assertThat(ex.getMessage())
        .contains(WASB_INIT_ERROR_MESSAGE);
  }
}
