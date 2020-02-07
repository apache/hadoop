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

package org.apache.hadoop.yarn.ipc;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Assert;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;

import org.apache.hadoop.thirdparty.protobuf.ServiceException;

public class TestRPCUtil {

  @Test
  public void testUnknownExceptionUnwrapping() {
    Class<? extends Throwable> exception = YarnException.class;
    String className = "UnknownException.class";
    verifyRemoteExceptionUnwrapping(exception, className);
  }

  @Test
  public void testRemoteIOExceptionUnwrapping() {
    Class<? extends Throwable> exception = IOException.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());
  }

  @Test
  public void testRemoteIOExceptionDerivativeUnwrapping() {
    // Test IOException sub-class
    Class<? extends Throwable> exception = FileNotFoundException.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());
  }

  @Test
  public void testRemoteYarnExceptionUnwrapping() {
    Class<? extends Throwable> exception = YarnException.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());

  }

  @Test
  public void testRemoteYarnExceptionDerivativeUnwrapping() {
    Class<? extends Throwable> exception = YarnTestException.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());
  }

  @Test
  public void testRemoteRuntimeExceptionUnwrapping() {
    Class<? extends Throwable> exception = NullPointerException.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());
  }

  @Test
  public void testUnexpectedRemoteExceptionUnwrapping() {
    // Non IOException, YarnException thrown by the remote side.
    Class<? extends Throwable> exception = Exception.class;
    verifyRemoteExceptionUnwrapping(RemoteException.class, exception.getName());
  }
  
  @Test
  public void testRemoteYarnExceptionWithoutStringConstructor() {
    // Derivatives of YarnException should always define a string constructor.
    Class<? extends Throwable> exception = YarnTestExceptionNoConstructor.class;
    verifyRemoteExceptionUnwrapping(RemoteException.class, exception.getName());
  }
  
  @Test
  public void testRPCServiceExceptionUnwrapping() {
    String message = "ServiceExceptionMessage";
    ServiceException se = new ServiceException(message);

    Throwable t = null;
    try {
      RPCUtil.unwrapAndThrowException(se);
    } catch (Throwable thrown) {
      t = thrown;
    }

    Assert.assertTrue(IOException.class.isInstance(t));
    Assert.assertTrue(t.getMessage().contains(message));
  }

  @Test
  public void testRPCIOExceptionUnwrapping() {
    String message = "DirectIOExceptionMessage";
    IOException ioException = new FileNotFoundException(message);
    ServiceException se = new ServiceException(ioException);

    Throwable t = null;
    try {
      RPCUtil.unwrapAndThrowException(se);
    } catch (Throwable thrown) {
      t = thrown;
    }
    Assert.assertTrue(FileNotFoundException.class.isInstance(t));
    Assert.assertTrue(t.getMessage().contains(message));
  }

  @Test
  public void testRPCRuntimeExceptionUnwrapping() {
    String message = "RPCRuntimeExceptionUnwrapping";
    RuntimeException re = new NullPointerException(message);
    ServiceException se = new ServiceException(re);

    Throwable t = null;
    try {
      RPCUtil.unwrapAndThrowException(se);
    }  catch (Throwable thrown) {
      t = thrown;
    }

    Assert.assertTrue(NullPointerException.class.isInstance(t));
    Assert.assertTrue(t.getMessage().contains(message));
  }

  private void verifyRemoteExceptionUnwrapping(
      Class<? extends Throwable> expectedLocalException,
      String realExceptionClassName) {
  String message = realExceptionClassName + "Message";
    RemoteException re = new RemoteException(realExceptionClassName, message);
    ServiceException se = new ServiceException(re);

    Throwable t = null;
    try {
      RPCUtil.unwrapAndThrowException(se);
    } catch (Throwable thrown) {
      t = thrown;
    }

    Assert.assertTrue("Expected exception [" + expectedLocalException
        + "] but found " + t, expectedLocalException.isInstance(t));
    Assert.assertTrue(
        "Expected message [" + message + "] but found " + t.getMessage(), t
            .getMessage().contains(message));
  }

  private static class YarnTestException extends YarnException {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    public YarnTestException(String message) {
      super(message);
    }
  }

  private static class YarnTestExceptionNoConstructor extends
      YarnException {
    private static final long serialVersionUID = 1L;

  }
}
