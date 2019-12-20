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
import java.util.List;

import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.waiters.WaiterTimedOutException;
import org.junit.Test;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.AWSClientIOException;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.test.HadoopTestBase;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStoreTableManager.translateTableWaitFailure;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  @Test
  public void testInnerListChildrenDirectoryNpe() throws Exception {
    DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore();
    Path p = mock(Path.class);
    List<PathMetadata> metas = mock(List.class);

    when(metas.isEmpty()).thenReturn(false);
    DDBPathMetadata dirPathMeta = null;

    assertNull("The return value should be null.",
        ddbms.getDirListingMetadataFromDirMetaAndList(p, metas, dirPathMeta));
  }

  @Test
  public void testAncestorStateForDir() throws Throwable {
    final DynamoDBMetadataStore.AncestorState ancestorState
        = new DynamoDBMetadataStore.AncestorState(
            null, BulkOperationState.OperationType.Rename, null);

    // path 1 is a directory
    final Path path1 = new Path("s3a://bucket/1");
    final S3AFileStatus status1 = new S3AFileStatus(true,
        path1, "hadoop");
    final DDBPathMetadata md1 = new DDBPathMetadata(
        status1);
    ancestorState.put(md1);
    assertTrue("Status not found in ancestors",
        ancestorState.contains(path1));
    final DDBPathMetadata result = ancestorState.get(path1);
    assertEquals(status1, result.getFileStatus());
    assertTrue("Lookup failed",
        ancestorState.findEntry(path1, true));
    final Path path2 = new Path("s3a://bucket/2");
    assertFalse("Lookup didn't fail",
        ancestorState.findEntry(path2, true));
    assertFalse("Lookup didn't fail",
        ancestorState.contains(path2));
    assertNull("Lookup didn't fail",
        ancestorState.get(path2));
  }

  @Test
  public void testAncestorStateForFile() throws Throwable {
    final DynamoDBMetadataStore.AncestorState ancestorState
        = new DynamoDBMetadataStore.AncestorState(
            null, BulkOperationState.OperationType.Rename, null);

    // path 1 is a file
    final Path path1 = new Path("s3a://bucket/1");
    final S3AFileStatus status1 = new S3AFileStatus(
        1024_1024_1024L,
        0,
        path1,
        32_000_000,
        "hadoop",
        "e4",
        "f5");
    final DDBPathMetadata md1 = new DDBPathMetadata(
        status1);
    ancestorState.put(md1);
    assertTrue("Lookup failed",
        ancestorState.findEntry(path1, false));
    intercept(PathIOException.class,
        DynamoDBMetadataStore.E_INCONSISTENT_UPDATE,
        () -> ancestorState.findEntry(path1, true));
  }

  @Test
  public void testNoBulkRenameThroughInitiateBulkWrite() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> S3Guard.initiateBulkWrite(null,
            BulkOperationState.OperationType.Rename, null));
  }
  @Test
  public void testInitiateBulkWrite() throws Throwable {
    assertNull(
        S3Guard.initiateBulkWrite(null,
            BulkOperationState.OperationType.Put, null));
  }

}
