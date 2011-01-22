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

package org.apache.hadoop.metrics2.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestTryIterator {

  /**
   * Test a common use case
   */
  @Test public void testCommonIteration() {
    Iterator<Integer> it = new TryIterator<Integer>() {
      private int count = 0;
      @Override protected Integer tryNext() {
        switch (count++) {
          case 0: return 0;
          case 1: return 1;
          case 2: return done();
          default: fail("Should not reach here");
        }
        return null;
      }
    };

    assertTrue("has next", it.hasNext());
    assertEquals("next", 0, (int) it.next());

    assertTrue("has next", it.hasNext());
    assertTrue("has next", it.hasNext()); // should be idempotent

    assertEquals("current", 1, (int) ((TryIterator<Integer>) it).current());
    assertEquals("current 1", 1, (int) ((TryIterator<Integer>) it).current());
    assertEquals("next", 1, (int) it.next());

    assertTrue("no next", !it.hasNext());
    assertTrue("no next", !it.hasNext()); // ditto

    try {
      it.next();
      fail("Should throw exception");
    }
    catch (NoSuchElementException expected) {
      expected.getCause();
    }
  }

  /**
   * Test empty conditions
   */
  @Test public void testEmptyIteration() {
    TryIterator<Integer> it = new TryIterator<Integer>() {
      private boolean doneDone = false;
      @Override public Integer tryNext() {
        if (doneDone) {
          fail("Should not be called again");
        }
        doneDone = true;
        return done();
      }
    };

    assertTrue("should not has next", !it.hasNext());

    try {
      it.current();
      fail("should throw");
    }
    catch (NoSuchElementException expected) {
      expected.getCause();
    }

    try {
      it.next();
      fail("should throw");
    }
    catch (NoSuchElementException expected) {
      expected.getCause();
    }
  }

  /**
   * Test tryNext throwing exceptions
   */
  @Test public void testExceptionInTryNext() {
    final RuntimeException exception = new RuntimeException("expected");

    Iterator<Integer> it = new TryIterator<Integer>() {
      @Override public Integer tryNext() {
        throw exception;
      }
    };

    try {
      it.hasNext();
      fail("should throw");
    }
    catch (Exception expected) {
      assertSame(exception, expected);
    }
  }

  /**
   * Test remove method on the iterator, which should throw
   */
  @Test public void testRemove() {
    Iterator<Integer> it = new TryIterator<Integer>() {
      private boolean called = false;
      @Override public Integer tryNext() {
        if (called) {
          return done();
        }
        called = true;
        return 0;
      }
    };

    assertEquals("should be 0", 0, (int) it.next());

    try {
      it.remove();
    }
    catch (UnsupportedOperationException expected) {
      expected.getCause();
    }
  }

  /**
   * Test calling hasNext in tryNext, which is illegal
   */
  @Test public void testHasNextInTryNext() {
    Iterator<Integer> it = new TryIterator<Integer>() {
      @Override public Integer tryNext() {
        hasNext();
        return null;
      }
    };

    try {
      it.hasNext();
      fail("should throw");
    } catch (IllegalStateException expected) {
      expected.getCause();
    }
  }
}
