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

package org.apache.hadoop.fs.azure;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.ResponseReceivedEvent;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.StorageEvent;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import org.apache.hadoop.classification.InterfaceAudience;
import org.junit.Test;

import java.net.HttpURLConnection;

/**
 * Tests for <code>BlobOperationDescriptor</code>.
 */
public class TestBlobOperationDescriptor extends AbstractWasbTestBase {
  private BlobOperationDescriptor.OperationType lastOperationTypeReceived;
  private BlobOperationDescriptor.OperationType lastOperationTypeSent;
  private long lastContentLengthReceived;

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  @Test
  public void testAppendBlockOperations() throws Exception {
    CloudBlobContainer container = getTestAccount().getRealContainer();

    OperationContext context = new OperationContext();
    context.getResponseReceivedEventHandler().addListener(
        new ResponseReceivedEventHandler());
    context.getSendingRequestEventHandler().addListener(
        new SendingRequestEventHandler());

    CloudAppendBlob appendBlob = container.getAppendBlobReference(
        "testAppendBlockOperations");
    assertNull(lastOperationTypeSent);
    assertNull(lastOperationTypeReceived);
    assertEquals(0, lastContentLengthReceived);

    try (
        BlobOutputStream output
            = appendBlob.openWriteNew(null, null, context);
    ) {
      assertEquals(BlobOperationDescriptor.OperationType.CreateBlob,
          lastOperationTypeReceived);
      assertEquals(0, lastContentLengthReceived);

      String message = "this is a test";
      output.write(message.getBytes("UTF-8"));
      output.flush();
      assertEquals(BlobOperationDescriptor.OperationType.AppendBlock,
          lastOperationTypeSent);
      assertEquals(BlobOperationDescriptor.OperationType.AppendBlock,
          lastOperationTypeReceived);
      assertEquals(message.length(), lastContentLengthReceived);
    }
  }

  @Test
  public void testPutBlockOperations() throws Exception {
    CloudBlobContainer container = getTestAccount().getRealContainer();

    OperationContext context = new OperationContext();
    context.getResponseReceivedEventHandler().addListener(
        new ResponseReceivedEventHandler());
    context.getSendingRequestEventHandler().addListener(
        new SendingRequestEventHandler());

    CloudBlockBlob blockBlob = container.getBlockBlobReference(
        "testPutBlockOperations");
    assertNull(lastOperationTypeSent);
    assertNull(lastOperationTypeReceived);
    assertEquals(0, lastContentLengthReceived);

    try (
        BlobOutputStream output
            = blockBlob.openOutputStream(null,
            null,
            context);
    ) {
      assertNull(lastOperationTypeReceived);
      assertEquals(0, lastContentLengthReceived);

      String message = "this is a test";
      output.write(message.getBytes("UTF-8"));
      output.flush();
      assertEquals(BlobOperationDescriptor.OperationType.PutBlock,
          lastOperationTypeSent);
      assertEquals(BlobOperationDescriptor.OperationType.PutBlock,
          lastOperationTypeReceived);
      assertEquals(message.length(), lastContentLengthReceived);
    }
    assertEquals(BlobOperationDescriptor.OperationType.PutBlockList,
        lastOperationTypeSent);
    assertEquals(BlobOperationDescriptor.OperationType.PutBlockList,
        lastOperationTypeReceived);
    assertEquals(0, lastContentLengthReceived);
  }

  @Test
  public void testPutPageOperations() throws Exception {
    CloudBlobContainer container = getTestAccount().getRealContainer();

    OperationContext context = new OperationContext();
    context.getResponseReceivedEventHandler().addListener(
        new ResponseReceivedEventHandler());
    context.getSendingRequestEventHandler().addListener(
        new SendingRequestEventHandler());

    CloudPageBlob pageBlob = container.getPageBlobReference(
        "testPutPageOperations");
    assertNull(lastOperationTypeSent);
    assertNull(lastOperationTypeReceived);
    assertEquals(0, lastContentLengthReceived);

    try (
        BlobOutputStream output = pageBlob.openWriteNew(1024,
            null,
            null,
            context);
    ) {
      assertEquals(BlobOperationDescriptor.OperationType.CreateBlob,
          lastOperationTypeReceived);
      assertEquals(0, lastContentLengthReceived);

      final int pageSize = 512;
      byte[] buffer = new byte[pageSize];
      output.write(buffer);
      output.flush();
      assertEquals(BlobOperationDescriptor.OperationType.PutPage,
          lastOperationTypeSent);
      assertEquals(BlobOperationDescriptor.OperationType.PutPage,
          lastOperationTypeReceived);
      assertEquals(buffer.length, lastContentLengthReceived);
    }
  }

  @Test
  public void testGetBlobOperations() throws Exception {
    CloudBlobContainer container = getTestAccount().getRealContainer();

    OperationContext context = new OperationContext();
    context.getResponseReceivedEventHandler().addListener(
        new ResponseReceivedEventHandler());
    context.getSendingRequestEventHandler().addListener(
        new SendingRequestEventHandler());

    CloudBlockBlob blockBlob = container.getBlockBlobReference(
        "testGetBlobOperations");
    assertNull(lastOperationTypeSent);
    assertNull(lastOperationTypeReceived);
    assertEquals(0, lastContentLengthReceived);

    String message = "this is a test";

    try (
        BlobOutputStream output = blockBlob.openOutputStream(null,
            null,
            context);
    ) {
      assertNull(lastOperationTypeReceived);
      assertEquals(0, lastContentLengthReceived);

      output.write(message.getBytes("UTF-8"));
      output.flush();
      assertEquals(BlobOperationDescriptor.OperationType.PutBlock,
          lastOperationTypeSent);
      assertEquals(BlobOperationDescriptor.OperationType.PutBlock,
          lastOperationTypeReceived);
      assertEquals(message.length(), lastContentLengthReceived);
    }
    assertEquals(BlobOperationDescriptor.OperationType.PutBlockList,
        lastOperationTypeSent);
    assertEquals(BlobOperationDescriptor.OperationType.PutBlockList,
        lastOperationTypeReceived);
    assertEquals(0, lastContentLengthReceived);

    try (
        BlobInputStream input = blockBlob.openInputStream(null,
            null,
            context);
    ) {
      assertEquals(BlobOperationDescriptor.OperationType.GetProperties,
          lastOperationTypeSent);
      assertEquals(BlobOperationDescriptor.OperationType.GetProperties,
          lastOperationTypeReceived);
      assertEquals(0, lastContentLengthReceived);

      byte[] buffer = new byte[1024];
      int numBytesRead = input.read(buffer);
      assertEquals(BlobOperationDescriptor.OperationType.GetBlob,
          lastOperationTypeSent);
      assertEquals(BlobOperationDescriptor.OperationType.GetBlob,
          lastOperationTypeReceived);
      assertEquals(message.length(), lastContentLengthReceived);
      assertEquals(numBytesRead, lastContentLengthReceived);
    }
  }

  /**
   * Called after the Azure Storage SDK receives a response.
   *
   * @param event The connection, operation, and request state.
   */
  private void responseReceived(ResponseReceivedEvent event) {
    HttpURLConnection conn = (HttpURLConnection) event.getConnectionObject();
    BlobOperationDescriptor.OperationType operationType
        = BlobOperationDescriptor.getOperationType(conn);
    lastOperationTypeReceived = operationType;

    switch (operationType) {
      case AppendBlock:
      case PutBlock:
      case PutPage:
        lastContentLengthReceived
            = BlobOperationDescriptor.getContentLengthIfKnown(conn,
            operationType);
        break;
      case GetBlob:
        lastContentLengthReceived
            = BlobOperationDescriptor.getContentLengthIfKnown(conn,
            operationType);
        break;
      default:
        lastContentLengthReceived = 0;
        break;
    }
  }

  /**
   * Called before the Azure Storage SDK sends a request.
   *
   * @param event The connection, operation, and request state.
   */
  private void sendingRequest(SendingRequestEvent event) {
    this.lastOperationTypeSent
        = BlobOperationDescriptor.getOperationType(
            (HttpURLConnection) event.getConnectionObject());
  }

  /**
   * The ResponseReceivedEvent is fired after the Azure Storage SDK receives a
   * response.
   */
  @InterfaceAudience.Private
  class ResponseReceivedEventHandler
      extends StorageEvent<ResponseReceivedEvent> {

    /**
     * Called after the Azure Storage SDK receives a response.
     *
     * @param event The connection, operation, and request state.
     */
    @Override
    public void eventOccurred(ResponseReceivedEvent event) {
      responseReceived(event);
    }
  }

  /**
   * The SendingRequestEvent is fired before the Azure Storage SDK sends a
   * request.
   */
  @InterfaceAudience.Private
  class SendingRequestEventHandler extends StorageEvent<SendingRequestEvent> {

    /**
     * Called before the Azure Storage SDK sends a request.
     *
     * @param event The connection, operation, and request state.
     */
    @Override
    public void eventOccurred(SendingRequestEvent event) {
      sendingRequest(event);
    }
  }
}
