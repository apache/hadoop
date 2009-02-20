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
package org.apache.hadoop.chukwa.datacollection.writer;

import java.io.*;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;

public class InMemoryWriter implements ChukwaWriter {

  ByteArrayOutputStream buf;
  
  public void close() {
    buf.reset();
  }

  public void init() throws IOException {
    buf = new ByteArrayOutputStream();
  }

  public void add(Chunk data) throws IOException {
    DataOutputStream dos = new DataOutputStream(buf);
    data.write(dos);
    synchronized(this) {
      notify();
    }
  }
  
  DataInputStream dis = null;
  /**
   * Try to read bytes, waiting up to ms
   * @param bytes amount to try to read
   * @param ms  time to wait
   * @return a newly read-in chunk
   * @throws IOException
   */
  public Chunk readOutChunk(int bytes, int ms) throws IOException {

    long readStartTime = System.currentTimeMillis();
    try {
      while(buf.size() < bytes )  {
        synchronized(this) {
          long timeLeft = ms - System.currentTimeMillis() + readStartTime;
          if(timeLeft > 0)
              wait(timeLeft);
        }
      }
      if(dis == null)
       dis = new DataInputStream( new ByteArrayInputStream(buf.toByteArray()));
      
      return ChunkImpl.read(dis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
    
  }

}
