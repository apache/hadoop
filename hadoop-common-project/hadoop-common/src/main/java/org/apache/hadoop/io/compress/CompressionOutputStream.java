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

package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A compression output stream.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class CompressionOutputStream extends OutputStream {
  /**
   * The output stream to be compressed. 
   */
  protected final OutputStream out;
  
  /**
   * Create a compression output stream that writes
   * the compressed bytes to the given stream.
   * @param out
   */
  protected CompressionOutputStream(OutputStream out) {
    this.out = out;
  }
  
  @Override
  public void close() throws IOException {
    finish();
    out.close();
  }
  
  @Override
  public void flush() throws IOException {
    out.flush();
  }
  
  /**
   * Write compressed bytes to the stream.
   * Made abstract to prevent leakage to underlying stream.
   */
  @Override
  public abstract void write(byte[] b, int off, int len) throws IOException;

  /**
   * Finishes writing compressed data to the output stream 
   * without closing the underlying stream.
   */
  public abstract void finish() throws IOException;
  
  /**
   * Reset the compression to the initial state. 
   * Does not reset the underlying stream.
   */
  public abstract void resetState() throws IOException;

}
