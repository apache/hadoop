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
package org.apache.hadoop.hdfs.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IOUtils;

import com.google.common.io.Files;
import com.google.common.primitives.Longs;

/**
 * Class that represents a file on disk which stores a single <code>long</code>
 * value, but does not make any effort to make it truly durable. This is in
 * contrast to {@link PersistentLongFile} which fsync()s the value on every
 * change.
 * 
 * This should be used for values which are updated frequently (such that
 * performance is important) and not required to be up-to-date for correctness.
 * 
 * This class also differs in that it stores the value as binary data instead
 * of a textual string.
 */
@InterfaceAudience.Private
public class BestEffortLongFile implements Closeable {

  private final File file;
  private final long defaultVal;

  private long value;
  
  private FileChannel ch = null;
  
  private ByteBuffer buf = ByteBuffer.allocate(Long.SIZE/8);
  
  public BestEffortLongFile(File file, long defaultVal) {
    this.file = file;
    this.defaultVal = defaultVal;
  }
  
  public long get() throws IOException {
    lazyOpen();
    return value;
  }

  public void set(long newVal) throws IOException {
    lazyOpen();
    buf.clear();
    buf.putLong(newVal);
    buf.flip();
    IOUtils.writeFully(ch, buf, 0);
    value = newVal;
  }
  
  private void lazyOpen() throws IOException {
    if (ch != null) {
      return;
    }

    // Load current value.
    byte[] data = null;
    try {
      data = Files.toByteArray(file);
    } catch (FileNotFoundException fnfe) {
      // Expected - this will use default value.
    }

    if (data != null && data.length != 0) {
      if (data.length != Longs.BYTES) {
        throw new IOException("File " + file + " had invalid length: " +
            data.length);
      }
      value = Longs.fromByteArray(data);
    } else {
      value = defaultVal;
    }
    
    // Now open file for future writes.
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    try {
      ch = raf.getChannel();
    } finally {
      if (ch == null) {
        IOUtils.closeStream(raf);
      }
    }
  }
  
  @Override
  public void close() throws IOException {
    if (ch != null) {
      ch.close();
    }
  }
}
