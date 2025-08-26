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

import org.junit.Assert;
import org.junit.Test;

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
  public void testHasCallAs() {
    Assert.assertEquals(JAVA_SPEC_VER > 17, SubjectUtil.HAS_CALL_AS);
  }

  @Test
  public void testDoAsPrivilegedActionExceptionPropagation() {
    // in Java 12 onwards, always throw the original exception thrown by action;
    // in lower Java versions, throw a PrivilegedActionException that wraps the
    // original exception when action throws a checked exception
    Throwable e = Assert.assertThrows(Exception.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            RuntimeException innerE = new RuntimeException("Inner Dummy RuntimeException");
            throw SubjectUtil.sneakyThrow(new IOException("Dummy IOException", innerE));
          }
        })
    );
    if (JAVA_SPEC_VER > 11) {
      Assert.assertTrue(e instanceof IOException);
      Assert.assertEquals("Dummy IOException", e.getMessage());
      Assert.assertTrue(e.getCause() instanceof RuntimeException);
      Assert.assertEquals("Inner Dummy RuntimeException", e.getCause().getMessage());
      Assert.assertNull(e.getCause().getCause());
    } else {
      Assert.assertTrue(e instanceof PrivilegedActionException);
      Assert.assertNull(e.getMessage());
      Assert.assertTrue(e.getCause() instanceof IOException);
      Assert.assertEquals("Dummy IOException", e.getCause().getMessage());
      Assert.assertTrue(e.getCause().getCause() instanceof RuntimeException);
      Assert.assertEquals("Inner Dummy RuntimeException", e.getCause().getCause().getMessage());
      Assert.assertNull(e.getCause().getCause().getCause());
    }

    // same as above case because PrivilegedActionException is a checked exception
    e = Assert.assertThrows(PrivilegedActionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            throw SubjectUtil.sneakyThrow(new PrivilegedActionException(null));
          }
        })
    );
    if (JAVA_SPEC_VER > 11) {
      Assert.assertTrue(e instanceof PrivilegedActionException);
      Assert.assertNull(e.getMessage());
      Assert.assertNull(e.getCause());
    } else {
      Assert.assertTrue(e instanceof PrivilegedActionException);
      Assert.assertNull(e.getMessage());
      Assert.assertTrue(e.getCause() instanceof PrivilegedActionException);
      Assert.assertNull(e.getCause().getMessage());
      Assert.assertNull(e.getCause().getCause());
    }

    // throw a PrivilegedActionException that wraps the original exception when action throws
    // a runtime exception
    e = Assert.assertThrows(RuntimeException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            throw new RuntimeException("Dummy RuntimeException");
          }
        })
    );
    Assert.assertTrue(e instanceof RuntimeException);
    Assert.assertEquals("Dummy RuntimeException", e.getMessage());
    Assert.assertNull(e.getCause());

    // same as above case because CompletionException is a runtime exception
    e = Assert.assertThrows(CompletionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            throw new CompletionException("Dummy CompletionException", null);
          }
        })
    );
    Assert.assertTrue(e instanceof CompletionException);
    Assert.assertEquals("Dummy CompletionException", e.getMessage());
    Assert.assertNull(e.getCause());

    // throw the original error when action throws an error
    e = Assert.assertThrows(LinkageError.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            throw new LinkageError("Dummy LinkageError");
          }
        })
    );
    Assert.assertTrue(e instanceof LinkageError);
    Assert.assertEquals("Dummy LinkageError", e.getMessage());
    Assert.assertNull(e.getCause());

    // throw NPE when action is NULL
    Assert.assertThrows(NullPointerException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), (PrivilegedAction<Object>) null)
    );
  }

  @Test
  public void testDoAsPrivilegedExceptionActionExceptionPropagation() {
    // throw PrivilegedActionException that wraps the original exception when action throws
    // a checked exception
    Throwable e = Assert.assertThrows(PrivilegedActionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            RuntimeException innerE = new RuntimeException("Inner Dummy RuntimeException");
            throw new IOException("Dummy IOException", innerE);
          }
        })
    );
    Assert.assertTrue(e instanceof PrivilegedActionException);
    Assert.assertNull(e.getMessage());
    Assert.assertTrue(e.getCause() instanceof IOException);
    Assert.assertEquals("Dummy IOException", e.getCause().getMessage());
    Assert.assertTrue(e.getCause().getCause() instanceof RuntimeException);
    Assert.assertEquals("Inner Dummy RuntimeException", e.getCause().getCause().getMessage());
    Assert.assertNull(e.getCause().getCause().getCause());

    // same as above because PrivilegedActionException is a checked exception
    e = Assert.assertThrows(PrivilegedActionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            throw new PrivilegedActionException(null);
          }
        })
    );
    Assert.assertTrue(e instanceof PrivilegedActionException);
    Assert.assertNull(e.getMessage());
    Assert.assertTrue(e.getCause() instanceof PrivilegedActionException);
    Assert.assertNull(e.getCause().getMessage());
    Assert.assertNull(e.getCause().getCause());

    // throw the original exception when action throw a runtime exception
    e = Assert.assertThrows(RuntimeException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            throw new RuntimeException("Dummy RuntimeException");
          }
        })
    );
    Assert.assertTrue(e instanceof RuntimeException);
    Assert.assertEquals("Dummy RuntimeException", e.getMessage());
    Assert.assertNull(e.getCause());

    // same as above case because CompletionException is a runtime exception
    e = Assert.assertThrows(CompletionException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            throw new CompletionException(null);
          }
        })
    );
    Assert.assertTrue(e instanceof CompletionException);
    Assert.assertNull(e.getMessage());
    Assert.assertNull(e.getCause());

    // throw the original error when action throw an error
    e = Assert.assertThrows(LinkageError.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            throw new LinkageError("Dummy LinkageError");
          }
        })
    );
    Assert.assertTrue(e instanceof LinkageError);
    Assert.assertEquals("Dummy LinkageError", e.getMessage());
    Assert.assertNull(e.getCause());

    // throw NPE when action is NULL
    Assert.assertThrows(NullPointerException.class, () ->
        SubjectUtil.doAs(SubjectUtil.current(), (PrivilegedExceptionAction<Object>) null)
    );
  }

  @Test
  public void testCallAsExceptionPropagation() {
    // always throw a CompletionException that wraps the original exception, when action throw
    // a checked or runtime exception
    Throwable e = Assert.assertThrows(CompletionException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            RuntimeException innerE = new RuntimeException("Inner Dummy RuntimeException");
            throw new IOException("Dummy IOException", innerE);
          }
        })
    );
    Assert.assertTrue(e instanceof CompletionException);
    if (JAVA_SPEC_VER > 11) {
      Assert.assertEquals("java.io.IOException: Dummy IOException", e.getMessage());
      Assert.assertTrue(e.getCause() instanceof IOException);
      Assert.assertEquals("Dummy IOException", e.getCause().getMessage());
      Assert.assertTrue(e.getCause().getCause() instanceof RuntimeException);
      Assert.assertEquals("Inner Dummy RuntimeException", e.getCause().getCause().getMessage());
      Assert.assertNull(e.getCause().getCause().getCause());
    } else {
      Assert.assertEquals(
          "java.security.PrivilegedActionException: java.io.IOException: Dummy IOException",
          e.getMessage());
      Assert.assertTrue(e.getCause() instanceof PrivilegedActionException);
      Assert.assertNull(e.getCause().getMessage());
      Assert.assertTrue(e.getCause().getCause() instanceof IOException);
      Assert.assertEquals("Dummy IOException", e.getCause().getCause().getMessage());
      Assert.assertTrue(e.getCause().getCause().getCause() instanceof RuntimeException);
      Assert.assertEquals("Inner Dummy RuntimeException",
          e.getCause().getCause().getCause().getMessage());
      Assert.assertNull(e.getCause().getCause().getCause().getCause());
    }

    e = Assert.assertThrows(CompletionException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            throw new PrivilegedActionException(null);
          }
        })
    );
    Assert.assertTrue(e instanceof CompletionException);
    if (JAVA_SPEC_VER > 11) {
      Assert.assertEquals("java.security.PrivilegedActionException", e.getMessage());
      Assert.assertTrue(e.getCause() instanceof PrivilegedActionException);
      Assert.assertNull(e.getCause().getMessage());
      Assert.assertNull(e.getCause().getCause());
    } else {
      Assert.assertEquals(
          "java.security.PrivilegedActionException: java.security.PrivilegedActionException",
          e.getMessage());
      Assert.assertTrue(e.getCause() instanceof PrivilegedActionException);
      Assert.assertNull(e.getCause().getMessage());
      Assert.assertTrue(e.getCause().getCause() instanceof PrivilegedActionException);
      Assert.assertNull(e.getCause().getCause().getMessage());
      Assert.assertNull(e.getCause().getCause().getCause());
    }

    e = Assert.assertThrows(CompletionException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            throw new RuntimeException("Dummy RuntimeException");
          }
        })
    );
    Assert.assertTrue(e instanceof CompletionException);
    Assert.assertEquals("java.lang.RuntimeException: Dummy RuntimeException", e.getMessage());
    Assert.assertTrue(e.getCause() instanceof RuntimeException);
    Assert.assertEquals("Dummy RuntimeException", e.getCause().getMessage());
    Assert.assertNull(e.getCause().getCause());

    e = Assert.assertThrows(CompletionException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            throw new CompletionException(null);
          }
        })
    );
    Assert.assertTrue(e instanceof CompletionException);
    Assert.assertEquals("java.util.concurrent.CompletionException", e.getMessage());
    Assert.assertTrue(e.getCause() instanceof CompletionException);
    Assert.assertNull(e.getCause().getMessage());

    // throw original error when action throw an error
    e = Assert.assertThrows(LinkageError.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            throw new LinkageError("Dummy LinkageError");
          }
        })
    );
    Assert.assertTrue(e instanceof LinkageError);
    Assert.assertEquals("Dummy LinkageError", e.getMessage());
    Assert.assertNull(e.getCause());

    // throw NPE when action is NULL
    Assert.assertThrows(NullPointerException.class, () ->
        SubjectUtil.callAs(SubjectUtil.current(), null)
    );
  }

  @Test
  public void testSneakyThrow() {
    IOException e = Assert.assertThrows(IOException.class, this::throwCheckedException);
    Assert.assertEquals("Dummy IOException", e.getMessage());
  }

  // A method that throw a checked exception, but has no exception declaration in signature
  private void throwCheckedException() {
    throw SubjectUtil.sneakyThrow(new IOException("Dummy IOException"));
  }
}
