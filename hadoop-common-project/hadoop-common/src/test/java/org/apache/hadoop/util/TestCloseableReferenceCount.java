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

import org.junit.Test;

import org.apache.hadoop.test.HadoopTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCloseableReferenceCount extends HadoopTestBase {
  @Test
  public void testReference() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    clr.reference();
    assertEquals("Incorrect reference count", 1, clr.getReferenceCount());
  }

  @Test
  public void testUnreference() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    clr.reference();
    clr.reference();
    assertFalse("New reference count should not equal STATUS_CLOSED_MASK",
        clr.unreference());
    assertEquals("Incorrect reference count", 1, clr.getReferenceCount());
  }

  @Test
  public void testUnreferenceCheckClosed() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    clr.reference();
    clr.reference();
    clr.unreferenceCheckClosed();
    assertEquals("Incorrect reference count", 1, clr.getReferenceCount());
  }

  @Test
  public void testSetClosed() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    assertTrue("Reference count should be open", clr.isOpen());
    clr.setClosed();
    assertFalse("Reference count should be closed", clr.isOpen());
  }

  @Test(expected = ClosedChannelException.class)
  public void testReferenceClosedReference() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    clr.setClosed();
    assertFalse("Reference count should be closed", clr.isOpen());
    clr.reference();
  }

  @Test(expected = ClosedChannelException.class)
  public void testUnreferenceClosedReference() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    clr.reference();
    clr.setClosed();
    assertFalse("Reference count should be closed", clr.isOpen());
    clr.unreferenceCheckClosed();
  }

  @Test(expected = ClosedChannelException.class)
  public void testDoubleClose() throws ClosedChannelException {
    CloseableReferenceCount clr = new CloseableReferenceCount();
    assertTrue("Reference count should be open", clr.isOpen());
    clr.setClosed();
    assertFalse("Reference count should be closed", clr.isOpen());
    clr.setClosed();
  }
}
