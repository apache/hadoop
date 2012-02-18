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
package org.apache.hadoop.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * OutputStream implementation that wraps a DataOutput.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DataOutputOutputStream extends OutputStream {

  private final DataOutput out;

  /**
   * Construct an OutputStream from the given DataOutput. If 'out'
   * is already an OutputStream, simply returns it. Otherwise, wraps
   * it in an OutputStream.
   * @param out the DataOutput to wrap
   * @return an OutputStream instance that outputs to 'out'
   */
  public static OutputStream constructOutputStream(DataOutput out) {
    if (out instanceof OutputStream) {
      return (OutputStream)out;
    } else {
      return new DataOutputOutputStream(out);
    }
  }
  
  private DataOutputOutputStream(DataOutput out) {
    this.out = out;
  }
  
  @Override
  public void write(int b) throws IOException {
    out.writeByte(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }
  

}
