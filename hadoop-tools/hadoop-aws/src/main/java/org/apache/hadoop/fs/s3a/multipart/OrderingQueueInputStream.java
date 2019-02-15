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

package org.apache.hadoop.fs.s3a.multipart;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PushbackInputStream;

/**
 * An {@link java.io.InputStream} that uses an {@link OrderingQueue} as its
 * source of bytes.
 */
public final class OrderingQueueInputStream extends AbortableInputStream {

  private final OrderingQueue orderingQueue;
  private final Runnable closeAction;
  private final Runnable abortAction;

  private PushbackInputStream inputStream =
      new PushbackInputStream(new ByteArrayInputStream(new byte[0]));

  public OrderingQueueInputStream(
      OrderingQueue orderingQueue,
      Runnable closeAction,
      Runnable abortAction) {
    this.orderingQueue = orderingQueue;
    this.closeAction = closeAction;
    this.abortAction = abortAction;
  }

  @Override
  public int read() throws IOException {
    fillByteArrayInputStream();
    return inputStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    fillByteArrayInputStream();
    return inputStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    fillByteArrayInputStream();
    return inputStream.read(b, off, len);
  }

  @Override
  public int available() throws IOException {
    fillByteArrayInputStream();
    return inputStream.available();
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
    closeAction.run();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  private void fillByteArrayInputStream() throws IOException {
    int read = inputStream.read();
    if (read == -1) {
      try {
        byte[] bytes = orderingQueue.popInOrder();
        if (bytes != null) {
          inputStream =
              new PushbackInputStream(new ByteArrayInputStream(bytes));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    } else {
      inputStream.unread(read);
    }
  }

  @Override
  /**
   * Do same thing as close since we have small part downloads.
   */
  public void abort() {
    try {
      inputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    abortAction.run();
  }
}
