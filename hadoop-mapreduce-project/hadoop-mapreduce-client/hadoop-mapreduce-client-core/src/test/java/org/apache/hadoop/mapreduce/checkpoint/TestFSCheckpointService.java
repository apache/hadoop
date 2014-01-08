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
package org.apache.hadoop.mapreduce.checkpoint;

import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.checkpoint.CheckpointService.CheckpointWriteChannel;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.mockito.*;

public class TestFSCheckpointService {

  private final int BUFSIZE = 1024;

  @Test
  public void testCheckpointCreate() throws Exception {
    checkpointCreate(ByteBuffer.allocate(BUFSIZE));
  }

  @Test
  public void testCheckpointCreateDirect() throws Exception {
    checkpointCreate(ByteBuffer.allocateDirect(BUFSIZE));
  }

  public void checkpointCreate(ByteBuffer b) throws Exception {
    int WRITES = 128;
    FileSystem fs = mock(FileSystem.class);
    DataOutputBuffer dob = new DataOutputBuffer();
    FSDataOutputStream hdfs = spy(new FSDataOutputStream(dob, null));
    @SuppressWarnings("resource") // backed by array
    DataOutputBuffer verif = new DataOutputBuffer();
    when(fs.create(isA(Path.class), eq((short)1))).thenReturn(hdfs);
    when(fs.rename(isA(Path.class), isA(Path.class))).thenReturn(true);

    Path base = new Path("/chk");
    Path finalLoc = new Path("/chk/checkpoint_chk0");
    Path tmp = FSCheckpointService.tmpfile(finalLoc);

    FSCheckpointService chk = new FSCheckpointService(fs, base,
        new SimpleNamingService("chk0"), (short) 1);
    CheckpointWriteChannel out = chk.create();

    Random r = new Random();

    final byte[] randBytes = new byte[BUFSIZE];
    for (int i = 0; i < WRITES; ++i) {
      r.nextBytes(randBytes);
      int s = r.nextInt(BUFSIZE - 1);
      int e = r.nextInt(BUFSIZE - s) + 1;
      verif.write(randBytes, s, e);
      b.clear();
      b.put(randBytes).flip();
      b.position(s).limit(b.position() + e);
      out.write(b);
    }
    verify(fs, never()).rename(any(Path.class), eq(finalLoc));
    CheckpointID cid = chk.commit(out);
    verify(hdfs).close();
    verify(fs).rename(eq(tmp), eq(finalLoc));

    assertArrayEquals(Arrays.copyOfRange(verif.getData(), 0, verif.getLength()),
        Arrays.copyOfRange(dob.getData(), 0, dob.getLength()));
  }

  @Test
  public void testDelete() throws Exception {
    FileSystem fs = mock(FileSystem.class);
    Path chkloc = new Path("/chk/chk0");
    when(fs.delete(eq(chkloc), eq(false))).thenReturn(true);
    Path base = new Path("/otherchk");
    FSCheckpointID id = new FSCheckpointID(chkloc);
    FSCheckpointService chk = new FSCheckpointService(fs, base,
        new SimpleNamingService("chk0"), (short) 1);
    assertTrue(chk.delete(id));
    verify(fs).delete(eq(chkloc), eq(false));
  }

}
