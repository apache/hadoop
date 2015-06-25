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
package org.apache.hadoop.hdfs.web.http2;

import java.io.IOException;
import java.util.Arrays;

import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http.MetaData.Response;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.ResetFrame;
import org.eclipse.jetty.util.Callback;

public class StreamListener extends Stream.Listener.Adapter {

  private boolean finish = false;

  private byte[] buf = new byte[0];

  private int status = -1;

  private boolean reset;

  @Override
  public void onData(Stream stream, DataFrame frame, Callback callback) {
    synchronized (this) {
      if (reset) {
        callback.failed(new IllegalStateException("Stream already closed"));
      }
      if (status == -1) {
        callback
            .failed(new IllegalStateException("Haven't received header yet"));
      }
      int bufLen = buf.length;
      int newBufLen = bufLen + frame.getData().remaining();
      buf = Arrays.copyOf(buf, newBufLen);
      frame.getData().get(buf, bufLen, frame.getData().remaining());
      if (frame.isEndStream()) {
        finish = true;
      }
      notifyAll();
      callback.succeeded();
    }
  }

  @Override
  public void onHeaders(Stream stream, HeadersFrame frame) {
    synchronized (this) {
      if (reset) {
        throw new IllegalStateException("Stream already closed");
      }
      if (status != -1) {
        throw new IllegalStateException("Header already received");
      }
      MetaData meta = frame.getMetaData();
      if (!meta.isResponse()) {
        throw new IllegalStateException("Received non-response header");
      }
      status = ((Response) meta).getStatus();
      if (frame.isEndStream()) {
        finish = true;
        notifyAll();
      }
    }
  }

  @Override
  public void onReset(Stream stream, ResetFrame frame) {
    synchronized (this) {
      reset = true;
      finish = true;
      notifyAll();
    }
  }

  public int getStatus() throws InterruptedException, IOException {
    synchronized (this) {
      while (!finish) {
        wait();
      }
      if (reset) {
        throw new IOException("Stream reset");
      }
      return status;
    }
  }

  public byte[] getData() throws InterruptedException, IOException {
    synchronized (this) {
      while (!finish) {
        wait();
      }
      if (reset) {
        throw new IOException("Stream reset");
      }
      return buf;
    }
  }
}
