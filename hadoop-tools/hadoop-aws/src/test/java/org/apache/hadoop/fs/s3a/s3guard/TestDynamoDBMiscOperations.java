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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.waiters.WaiterTimedOutException;
import org.junit.Test;

import org.apache.hadoop.fs.s3a.AWSClientIOException;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.translateTableWaitFailure;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit test suite for misc dynamoDB metastore operations.
 */
public class TestDynamoDBMiscOperations extends HadoopTestBase {

  private static final String TIMEOUT_ERROR_MESSAGE
      = "Table table-name did not transition into ACTIVE state.";

  @Test
  public void testUnwrapTableWaitTimeout() throws Throwable {
    final Exception waiterTimedOut =
        new WaiterTimedOutException("waiter timed out");
    final AWSClientIOException ex = intercept(AWSClientIOException.class,
        TIMEOUT_ERROR_MESSAGE,
        () -> {
          throw translateTableWaitFailure("example",
              new IllegalArgumentException(TIMEOUT_ERROR_MESSAGE,
                  waiterTimedOut));
        });
    assertEquals(waiterTimedOut, ex.getCause());
  }

  @Test
  public void testTranslateIllegalArgumentException() throws Throwable {
    final IllegalArgumentException e =
        new IllegalArgumentException(TIMEOUT_ERROR_MESSAGE);
    final IOException ex = intercept(IOException.class,
        TIMEOUT_ERROR_MESSAGE,
        () -> {
          throw translateTableWaitFailure("example", e);
        });
    assertEquals(e, ex.getCause());
  }

  @Test
  public void testTranslateWrappedDDBException() throws Throwable {
    final Exception inner = new ResourceNotFoundException("ddb");
    final IllegalArgumentException e =
        new IllegalArgumentException("outer", inner);
    final FileNotFoundException ex = intercept(FileNotFoundException.class,
        "outer",
        () -> {
          throw translateTableWaitFailure("example", e);
        });
    assertEquals(inner, ex.getCause());
  }

  @Test
  public void testTranslateWrappedOtherException() throws Throwable {
    final Exception inner = new NullPointerException("npe");
    final IllegalArgumentException e =
        new IllegalArgumentException("outer", inner);
    final IOException ex = intercept(IOException.class,
        "outer",
        () -> {
          throw translateTableWaitFailure("example", e);
        });
    assertEquals(e, ex.getCause());
  }

}
