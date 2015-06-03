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

import java.io.IOException;

public class DB extends NativeObject {
  public static DB open(Options options, String path) throws IOException {
    long handle = open(options.nativeHandle(), path);
    return new DB(handle);
  }

  @Override
  public void close() {
    if (this.nativeHandle != 0) {
      close(nativeHandle);
      nativeHandle = 0;
    }
  }

  public byte[] get(ReadOptions options, byte[] key) throws IOException {
    return get(nativeHandle, options.nativeHandle(), key);
  }

  public byte[] snapshotGet(ReadOptions options, byte[] key) throws
                                                             IOException {
    return snapshotGet(nativeHandle, options.nativeHandle(), key);
  }

  public void write(WriteOptions options, WriteBatch batch) throws IOException {
    write(nativeHandle, options.nativeHandle(), batch.nativeHandle());
  }

  public void put(WriteOptions options, byte[] key, byte[] value) {
    put(nativeHandle, options.nativeHandle(), key, value);
  }

  public void delete(WriteOptions options, byte[] key) {
    delete(nativeHandle, options.nativeHandle(), key);
  }

  public Iterator iterator(ReadOptions options) {
    return new Iterator(newIterator(nativeHandle, options.nativeHandle()));
  }

  public Snapshot snapshot() {
    return new Snapshot(nativeHandle, newSnapshot(nativeHandle));
  }

  public byte[] dbGetTest(byte[] key) throws IOException {
    return getTest(nativeHandle, key);
  }

  private DB(long handle) {
    super(handle);
  }

  private static native long open(long options, String path) throws IOException;
  private static native void close(long handle);
  private static native byte[] get(long handle, long options,
                                   byte[] key) throws IOException;
  private static native byte[] snapshotGet(long handle, long options,
      byte[] key) throws IOException;
  private static native void write(long handle, long options,
                                   long batch) throws IOException;
  private static native void put(long handle, long options,
                                 byte[] key, byte[] value);
  private static native void delete(long handle, long options,
                                    byte[] key);
  private static native long newIterator(long handle, long readOptions);
  private static native long newSnapshot(long handle);
  static native void releaseSnapshot(long handle, long snapshotHandle);

  private static native byte[] getTest(long handle, byte[] key) throws IOException;

}
