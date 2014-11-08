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
package org.apache.hadoop.hdfs.hdfsdb;

public class Options extends NativeObject {
  public enum CompressionType {
    NO_COMPRESSION(0),
    SNAPPY(1);

    final int value;
    CompressionType(int value) {
      this.value = value;
    }
  }
  public Options() {
    super(construct());
  }

  public Options createIfMissing(boolean value) {
    createIfMissing(nativeHandle, value);
    return this;
  }

  public Options compressionType(int type) {
    CompressionType ctype = CompressionType.values()[type];
    return compressionType(ctype);
  }

  public Options compressionType(CompressionType type) {
    compressionType(nativeHandle, type.value);
    return this;
  }

  public Options writeBufferSize(int value) {
    writeBufferSize(nativeHandle, value);
    return this;
  }

  public Options blockSize(int value) {
    blockSize(nativeHandle, value);
    return this;
  }

  @Override
  public void close() {
    if (nativeHandle != 0) {
      destruct(nativeHandle);
      nativeHandle = 0;
    }
  }

  private static native long construct();
  private static native void destruct(long handle);
  private static native void createIfMissing(long handle, boolean value);
  private static native void compressionType(long handle, int value);
  private static native void writeBufferSize(long handle, int value);
  private static native void blockSize(long handle, int value);
}
