/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.common;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

public class IoTest {

  private static class TestResource implements Closeable {
    public boolean isOpen = true;

    @Override
    public void close() throws IOException {
      isOpen = false;
      throw new IOException("foo");
    }
  }

  @Test
  public void verifyCloseIgnoringIoException() {
    ExceptionAsserts.assertThrows(
        IOException.class,
        "foo",
        () -> { (new TestResource()).close(); });

    // Should not throw.
    TestResource resource = new TestResource();
    assertTrue(resource.isOpen);
    Io.closeIgnoringIoException(resource);
    assertTrue(!resource.isOpen);
  }
}
