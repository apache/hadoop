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

package org.apache.hadoop.fs.azurebfs.services;

public class AbfsRestIODataParameters {

  private byte[] buffer;
  private int bufferOffset;
  private int bufferLength;
  private String fastpathFileHandle;

  public AbfsRestIODataParameters(byte[] buffer,
      int bufferOffset,
      int bufferLength,
      String fastpathFileHandle) {
    this.buffer = buffer;
    this.bufferOffset = bufferOffset;
    this.bufferLength = bufferLength;
    this.fastpathFileHandle = fastpathFileHandle;
  }

  public AbfsRestIODataParameters(byte[] buffer,
      int bufferOffset,
      int bufferLength) {
    this.buffer = buffer;
    this.bufferOffset = bufferOffset;
    this.bufferLength = bufferLength;
    this.fastpathFileHandle = null;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public int getBufferOffset() {
    return bufferOffset;
  }

  public int getBufferLength() {
    return bufferLength;
  }

  public String getFastpathFileHandle() {
    return fastpathFileHandle;
  }
}
