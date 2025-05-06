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

import java.nio.channels.ClosedChannelException;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.test.HadoopTestBase;

public class TestCloseableReferenceCount extends HadoopTestBase {
  @Test
  public void testReference() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    clr.reference();
    assertEquals(1, clr.getReferenceCount(), "Incorrect reference count");
  }

  @Test
  public void testUnreference() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    clr.reference();
    clr.reference();
    assertFalse(clr.unreference(),
        "New reference count should not equal STATUS_CLOSED_MASK");
    assertEquals(1, clr.getReferenceCount(), "Incorrect reference count");
  }

  @Test
  public void testUnreferenceCheckClosed() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    clr.reference();
    clr.reference();
    clr.unreferenceCheckClosed();
    assertEquals(1, clr.getReferenceCount(), "Incorrect reference count");
  }

  @Test
  public void testSetClosed() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    assertTrue(clr.isOpen(), "Reference count should be open");
    clr.setClosed();
    assertFalse(clr.isOpen(), "Reference count should be closed");
  }

  @Test
  public void testReferenceClosedReference() throws ClosedChannelException {
    assertThrows(ClosedChannelException.class, () -> {
      CloseableReferenceCount clr = new CloseableReferenceCount();
      clr.setClosed();
      assertFalse(clr.isOpen(), "Reference count should be closed");
      clr.reference();
    });
  }

  @Test
  public void testUnreferenceClosedReference() throws ClosedChannelException {
    assertThrows(ClosedChannelException.class, () -> {
      CloseableReferenceCount clr = new CloseableReferenceCount();
      clr.reference();
      clr.setClosed();
      assertFalse(clr.isOpen(), "Reference count should be closed");
      clr.unreferenceCheckClosed();
    });
  }

  @Test
  public void testDoubleClose() throws ClosedChannelException {
    assertThrows(ClosedChannelException.class, () -> {
      CloseableReferenceCount clr = new CloseableReferenceCount();
      assertTrue(clr.isOpen(), "Reference count should be open");
      clr.setClosed();
      assertFalse(clr.isOpen(), "Reference count should be closed");
      clr.setClosed();
    });
  }
}
