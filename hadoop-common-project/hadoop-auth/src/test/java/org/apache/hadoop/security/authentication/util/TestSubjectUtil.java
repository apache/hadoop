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

package org.apache.hadoop.security.authentication.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

public class TestSubjectUtil {

  // "1.8"->8, "9"->9, "10"->10
  private static final int JAVA_SPEC_VER = Math.max(8, Integer.parseInt(
      System.getProperty("java.specification.version").split("\\.")[0]));

  @Test
  void testHasCallAs() {
    assertEquals(JAVA_SPEC_VER > 17, SubjectUtil.HAS_CALL_AS);
  }

  @Test
  void testDoAsPrivilegedActionExceptionPropagation() {
    // in Java 12 onwards, always throw the original exception thrown by action;
    // in lower Java versions, throw a PrivilegedActionException that wraps the
    // original exception when action throws a checked exception
    Throwable e = assertThrows(Exception.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            RuntimeException innerE = new RuntimeException("Inner Dummy RuntimeException");
            throw SubjectUtil.sneakyThrow(new IOException("Dummy IOException", innerE));
          }
        })
    );
    if (JAVA_SPEC_VER > 11) {
      assertInstanceOf(IOException.class, e);
      assertEquals("Dummy IOException", e.getMessage());
      assertInstanceOf(RuntimeException.class, e.getCause());
      assertEquals("Inner Dummy RuntimeException", e.getCause().getMessage());
      assertNull(e.getCause().getCause());
    } else {
      assertInstanceOf(PrivilegedActionException.class, e);
      assertNull(e.getMessage());
      assertInstanceOf(IOException.class, e.getCause());
      assertEquals("Dummy IOException", e.getCause().getMessage());
      assertInstanceOf(RuntimeException.class, e.getCause().getCause());
      assertEquals("Inner Dummy RuntimeException", e.getCause().getCause().getMessage());
      assertNull(e.getCause().getCause().getCause());
    }

    // same as above case because PrivilegedActionException is a checked exception
    e = assertThrows(PrivilegedActionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            throw SubjectUtil.sneakyThrow(new PrivilegedActionException(null));
          }
        })
    );
    if (JAVA_SPEC_VER > 11) {
      assertInstanceOf(PrivilegedActionException.class, e);
      assertNull(e.getMessage());
      assertNull(e.getCause());
    } else {
      assertInstanceOf(PrivilegedActionException.class, e);
      assertNull(e.getMessage());
      assertInstanceOf(PrivilegedActionException.class, e.getCause());
      assertNull(e.getCause().getMessage());
      assertNull(e.getCause().getCause());
    }

    // throw a PrivilegedActionException that wraps the original exception when action throws
    // a runtime exception
    e = assertThrows(RuntimeException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            throw new RuntimeException("Dummy RuntimeException");
          }
        })
    );
    assertInstanceOf(RuntimeException.class, e);
    assertEquals("Dummy RuntimeException", e.getMessage());
    assertNull(e.getCause());

    // same as above case because CompletionException is a runtime exception
    e = assertThrows(CompletionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            throw new CompletionException("Dummy CompletionException", null);
          }
        })
    );
    assertInstanceOf(CompletionException.class, e);
    assertEquals("Dummy CompletionException", e.getMessage());
    assertNull(e.getCause());

    // throw the original error when action throws an error
    e = assertThrows(LinkageError.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            throw new LinkageError("Dummy LinkageError");
          }
        })
    );
    assertInstanceOf(LinkageError.class, e);
    assertEquals("Dummy LinkageError", e.getMessage());
    assertNull(e.getCause());

    // throw NPE when action is NULL
    assertThrows(NullPointerException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), (PrivilegedAction<Object>) null)
    );
  }

  @Test
  void testDoAsPrivilegedExceptionActionExceptionPropagation() {
    // throw PrivilegedActionException that wraps the original exception when action throws
    // a checked exception
    Throwable e = assertThrows(PrivilegedActionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            RuntimeException innerE = new RuntimeException("Inner Dummy RuntimeException");
            throw new IOException("Dummy IOException", innerE);
          }
        })
    );
    assertInstanceOf(PrivilegedActionException.class, e);
    assertNull(e.getMessage());
    assertInstanceOf(IOException.class, e.getCause());
    assertEquals("Dummy IOException", e.getCause().getMessage());
    assertInstanceOf(RuntimeException.class, e.getCause().getCause());
    assertEquals("Inner Dummy RuntimeException", e.getCause().getCause().getMessage());
    assertNull(e.getCause().getCause().getCause());

    // same as above because PrivilegedActionException is a checked exception
    e = assertThrows(PrivilegedActionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            throw new PrivilegedActionException(null);
          }
        })
    );
    assertInstanceOf(PrivilegedActionException.class, e);
    assertNull(e.getMessage());
    assertInstanceOf(PrivilegedActionException.class, e.getCause());
    assertNull(e.getCause().getMessage());
    assertNull(e.getCause().getCause());

    // throw the original exception when action throw a runtime exception
    e = assertThrows(RuntimeException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            throw new RuntimeException("Dummy RuntimeException");
          }
        })
    );
    assertInstanceOf(RuntimeException.class, e);
    assertEquals("Dummy RuntimeException", e.getMessage());
    assertNull(e.getCause());

    // same as above case because CompletionException is a runtime exception
    e = assertThrows(CompletionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            throw new CompletionException(null);
          }
        })
    );
    assertInstanceOf(CompletionException.class, e);
    assertNull(e.getMessage());
    assertNull(e.getCause());

    // throw the original error when action throw an error
    e = assertThrows(LinkageError.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            throw new LinkageError("Dummy LinkageError");
          }
        })
    );
    assertInstanceOf(LinkageError.class, e);
    assertEquals("Dummy LinkageError", e.getMessage());
    assertNull(e.getCause());

    // throw NPE when action is NULL
    assertThrows(NullPointerException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), (PrivilegedExceptionAction<Object>) null)
    );
  }

  @Test
  void testCallAsExceptionPropagation() {
    // always throw a CompletionException that wraps the original exception, when action throw
    // a checked or runtime exception
    Throwable e = assertThrows(CompletionException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            RuntimeException innerE = new RuntimeException("Inner Dummy RuntimeException");
            throw new IOException("Dummy IOException", innerE);
          }
        })
    );
    assertInstanceOf(CompletionException.class, e);
    if (JAVA_SPEC_VER > 11) {
      assertEquals("java.io.IOException: Dummy IOException", e.getMessage());
      assertInstanceOf(IOException.class, e.getCause());
      assertEquals("Dummy IOException", e.getCause().getMessage());
      assertInstanceOf(RuntimeException.class, e.getCause().getCause());
      assertEquals("Inner Dummy RuntimeException", e.getCause().getCause().getMessage());
      assertNull(e.getCause().getCause().getCause());
    } else {
      assertEquals(
          "java.security.PrivilegedActionException: java.io.IOException: Dummy IOException",
          e.getMessage());
      assertInstanceOf(PrivilegedActionException.class, e.getCause());
      assertNull(e.getCause().getMessage());
      assertInstanceOf(IOException.class, e.getCause().getCause());
      assertEquals("Dummy IOException", e.getCause().getCause().getMessage());
      assertInstanceOf(RuntimeException.class, e.getCause().getCause().getCause());
      assertEquals("Inner Dummy RuntimeException",
          e.getCause().getCause().getCause().getMessage());
      assertNull(e.getCause().getCause().getCause().getCause());
    }

    e = assertThrows(CompletionException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            throw new PrivilegedActionException(null);
          }
        })
    );
    assertInstanceOf(CompletionException.class, e);
    if (JAVA_SPEC_VER > 11) {
      assertEquals("java.security.PrivilegedActionException", e.getMessage());
      assertInstanceOf(PrivilegedActionException.class, e.getCause());
      assertNull(e.getCause().getMessage());
      assertNull(e.getCause().getCause());
    } else {
      assertEquals(
          "java.security.PrivilegedActionException: java.security.PrivilegedActionException",
          e.getMessage());
      assertInstanceOf(PrivilegedActionException.class, e.getCause());
      assertNull(e.getCause().getMessage());
      assertInstanceOf(PrivilegedActionException.class, e.getCause().getCause());
      assertNull(e.getCause().getCause().getMessage());
      assertNull(e.getCause().getCause().getCause());
    }

    e = assertThrows(CompletionException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            throw new RuntimeException("Dummy RuntimeException");
          }
        })
    );
    assertInstanceOf(CompletionException.class, e);
    assertEquals("java.lang.RuntimeException: Dummy RuntimeException", e.getMessage());
    assertInstanceOf(RuntimeException.class, e.getCause());
    assertEquals("Dummy RuntimeException", e.getCause().getMessage());
    assertNull(e.getCause().getCause());

    e = assertThrows(CompletionException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            throw new CompletionException(null);
          }
        })
    );
    assertInstanceOf(CompletionException.class, e);
    assertEquals("java.util.concurrent.CompletionException", e.getMessage());
    assertInstanceOf(CompletionException.class, e.getCause());
    assertNull(e.getCause().getMessage());

    // throw original error when action throw an error
    e = assertThrows(LinkageError.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            throw new LinkageError("Dummy LinkageError");
          }
        })
    );
    assertInstanceOf(LinkageError.class, e);
    assertEquals("Dummy LinkageError", e.getMessage());
    assertNull(e.getCause());

    // throw NPE when action is NULL
    assertThrows(NullPointerException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), null)
    );
  }

  @Test
  void testSneakyThrow() {
    IOException e = assertThrows(IOException.class, this::throwCheckedException);
    assertEquals("Dummy IOException", e.getMessage());
  }

  // A method that throw a checked exception, but has no exception declaration in signature
  private void throwCheckedException() {
    throw SubjectUtil.sneakyThrow(new IOException("Dummy IOException"));
  }
}
