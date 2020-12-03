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

package org.apache.hadoop.runc.squashfs.table;

import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestFileTableReader {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tempFile;

  private SuperBlock sb;
  private byte[] data;
  private FileTableReader reader;

  @Before
  public void setUp() throws Exception {
    tempFile = temp.newFile();
    sb = new SuperBlock();
    try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw")) {
      sb.writeData(raf);
      data = new byte[1024];
      for (int i = 0; i < data.length; i++) {
        data[i] = (byte) (i & 0xff);
      }
      raf.write(data);
    }

    reader = new FileTableReader(tempFile);
  }

  @Test
  public void readShouldExposeByteBuffer() throws Exception {
    ByteBuffer bb = reader.read(SuperBlock.SIZE, data.length);
    for (int i = 0; i < data.length; i++) {
      assertEquals(String.format("Wrong value for element %d", i),
          (byte) (i & 0xff), bb.get());
    }
  }

  @Test
  public void readShouldExposeByteBufferAtOffset() throws Exception {
    ByteBuffer bb = reader.read(SuperBlock.SIZE + 1L, data.length - 1);
    for (int i = 0; i < data.length - 1; i++) {
      assertEquals(String.format("Wrong value for element %d", i),
          (byte) ((i + 1) & 0xff), bb.get());
    }
  }

  @Test(expected = EOFException.class)
  public void readShouldThrowExceptionOnEof() throws Exception {
    reader.read(SuperBlock.SIZE + 1023L, 2);
  }

  @Test
  public void getSuperBlockShouldReturnConstructedInstance() {
    assertEquals(sb.getModificationTime(),
        reader.getSuperBlock().getModificationTime());
  }

  @Test
  public void closeShouldCloseUnderlyingReaderIfRequested() throws Exception {
    try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
      reader = new FileTableReader(raf, sb, true);
      reader.close();

      try {
        raf.seek(0L);
        fail("exception not thrown");
      } catch (IOException e) {
        System.out.println("Got EOF");
      }
    }
  }

  @Test
  public void closeShouldNotCloseUnderlyingReaderIfNotRequested()
      throws Exception {
    try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
      reader = new FileTableReader(raf, sb, false);
      reader.close();
      raf.seek(0L);
    }
  }
}
