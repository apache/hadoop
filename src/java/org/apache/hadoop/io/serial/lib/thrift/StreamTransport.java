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

package org.apache.hadoop.io.serial.lib.thrift;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Define a Thrift transport that we can dynamically change the input and
 * output stream. Otherwise, we need to recreate the encoder and decoder
 * for each object.
 */
class StreamTransport extends TTransport {
  private InputStream in = null;
  private OutputStream out = null;

  void open(InputStream in) {
    if (this.in != null || this.out != null) {
      throw new IllegalStateException("opening an open transport");
    }
    this.in = in;
  }
  
  void open(OutputStream out) {
    if (this.in != null || this.out != null) {
      throw new IllegalStateException("opening an open transport");
    }
    this.out = out;
  }

  @Override
  public void close() {
    if (in != null) {
      in = null;
    } else if (out != null) {
      out = null;
    }
  }

  @Override
  public boolean isOpen() {
    return in != null || out != null;
  }

  @Override
  public void open() {
    // NOTHING
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    try {
      return in.read(buf, off, len);
    } catch (IOException ie) {
      throw new TTransportException("problem reading stream", ie);
    }
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    try {
      out.write(buf, off, len);
    } catch (IOException ie) {
      throw new TTransportException("problem writing stream", ie);
    }
  }

}
