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

package org.apache.hadoop.runc.squashfs;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class TestSquashFsException {

  @Test
  public void noArgConstructorShouldHaveNullMessageAndCause() {
    try {
      throw new SquashFsException();
    } catch (SquashFsException e) {
      assertNull(e.getMessage());
      assertNull(e.getCause());
    }
  }

  @Test
  public void stringConstructorShouldHaveSameMessageAndNullCause() {
    try {
      throw new SquashFsException("test");
    } catch (SquashFsException e) {
      assertEquals("test", e.getMessage());
      assertNull(e.getCause());
    }
  }

  @Test
  public void throwableConstructorShouldHaveGeneratedMessageAndSameCause() {
    Exception cause = null;
    try {
      try {
        throw new IOException("cause");
      } catch (IOException e2) {
        cause = e2;
      }
      throw new SquashFsException(cause);
    } catch (SquashFsException e) {
      assertEquals(IOException.class.getName() + ": cause", e.getMessage());
      assertSame(cause, e.getCause());
    }
  }

  @Test
  public void twoArgConstructorShouldHaveSameMessageAndCause() {
    Exception cause = null;
    try {
      try {
        throw new IOException("cause");
      } catch (IOException e2) {
        cause = e2;
      }
      throw new SquashFsException("test", cause);
    } catch (SquashFsException e) {
      assertEquals("test", e.getMessage());
      assertSame(cause, e.getCause());
    }
  }

}
