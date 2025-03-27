/*
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

package org.apache.hadoop.ipc;

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.ipc.internal.ShadedProtobufHelper;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.verifyCause;

/**
 * Test methods in {@link ShadedProtobufHelper}.
 */
public class TestShadedProtobufHelper extends AbstractHadoopTestBase {

  @Test
  public void testExtractRemoteExceptionNoCause() throws Throwable {
    ServiceException source = new ServiceException("empty");

    IOException ex = ShadedProtobufHelper.getRemoteException(source);
    verifyCause(ServiceException.class, ex);
  }

  @Test
  public void testExtractRemoteExceptionIOECause() throws Throwable {
    IOException source = new IOException("ioe");

    IOException ex = ShadedProtobufHelper.getRemoteException(
        new ServiceException(source));
    // if not the same, throw
    if (!(ex == source)) {
      throw ex;
    }
  }

  @Test
  public void testExtractRemoteExceptionOtherCause() throws Throwable {
    NullPointerException source = new NullPointerException("npe");

    IOException ex = ShadedProtobufHelper.getRemoteException(
        new ServiceException(source));
    // if not the same, throw
    ServiceException c1 = verifyCause(ServiceException.class, ex);
    verifyCause(NullPointerException.class, c1);
  }

  @Test
  public void testIPCWrapperServiceException() throws Throwable {
    intercept(IOException.class, "expected", () -> {
      ipc(() -> {
        throw new ServiceException("expected");
      });
    });
  }

  @Test
  public void testIPCWrapperNPE() throws Throwable {
    final IOException ex = intercept(IOException.class, "npe", () -> {
      ipc(() -> {
        throw new ServiceException(new NullPointerException("npe"));
      });
    });
    ServiceException c1 = verifyCause(ServiceException.class, ex);
    verifyCause(NullPointerException.class, c1);
  }

}
