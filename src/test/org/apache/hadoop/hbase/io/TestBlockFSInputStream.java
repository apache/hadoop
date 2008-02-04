/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.io.DataInputBuffer;

public class TestBlockFSInputStream extends TestCase {
  
  static class InMemoryFSInputStream extends FSInputStream {

    private byte[] data;
    private DataInputBuffer din = new DataInputBuffer();
    
    public InMemoryFSInputStream(byte[] data) {
      this.data = data;
      din.reset(data, data.length);
    }
    
    @Override
    public long getPos() throws IOException {
      return din.getPosition();
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos > data.length) {
        throw new IOException("Cannot seek after EOF");
      }
      din.reset(data, (int) pos, data.length - (int) pos);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read() throws IOException {
      return din.read();
    }
    
  }
  
  private byte[] data;
  private BlockFSInputStream stream;
  
  @Override
  protected void setUp() throws Exception {
    data = new byte[34];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }
    FSInputStream byteStream = new InMemoryFSInputStream(data);
    stream = new BlockFSInputStream(byteStream, 34, 10);
  }

  public void testReadForwards() throws IOException {
    for (int i = 0; i < data.length; i++) {
      assertEquals(i, stream.getPos());
      assertEquals(i, stream.read());
    }

  }
  
  public void testReadBackwards() throws IOException {
    for (int i = data.length - 1; i >= 0; i--) {
      stream.seek(i);
      assertEquals(i, stream.getPos());
      assertEquals(i, stream.read());
    }
  }
  
  public void testReadInChunks() throws IOException {
    
    byte[] buf = new byte[data.length];
    int chunkLength = 6;
    
    assertEquals(6, stream.read(buf, 0, chunkLength));
    assertEquals(4, stream.read(buf, 6, chunkLength));
    
    assertEquals(6, stream.read(buf, 10, chunkLength));
    assertEquals(4, stream.read(buf, 16, chunkLength));
    
    assertEquals(6, stream.read(buf, 20, chunkLength));
    assertEquals(4, stream.read(buf, 26, chunkLength));

    assertEquals(4, stream.read(buf, 30, chunkLength));
    
    assertEquals(0, stream.available());
    
    assertEquals(-1, stream.read());
    
    for (int i = 0; i < buf.length; i++) {
      assertEquals(i, buf[i]);
    }


  }
}
