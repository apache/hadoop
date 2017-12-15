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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the implementation of {@link ProvidedReplica}.
 */
public class TestProvidedReplicaImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestProvidedReplicaImpl.class);
  private static final String BASE_DIR =
      new FileSystemTestHelper().getTestRootDir();
  private static final String FILE_NAME = "provided-test";
  // length of the file that is associated with the provided blocks.
  private static final long FILE_LEN = 128 * 1024 * 10L + 64 * 1024;
  // length of each provided block.
  private static final long BLK_LEN = 128 * 1024L;

  private static List<ProvidedReplica> replicas;

  private static void createFileIfNotExists(String baseDir) throws IOException {
    File newFile = new File(baseDir, FILE_NAME);
    newFile.getParentFile().mkdirs();
    if(!newFile.exists()) {
      newFile.createNewFile();
      OutputStream writer = new FileOutputStream(newFile.getAbsolutePath());
      byte[] bytes = new byte[1];
      bytes[0] = (byte) 0;
      for(int i=0; i< FILE_LEN; i++) {
        writer.write(bytes);
      }
      writer.flush();
      writer.close();
      LOG.info("Created provided file " + newFile +
          " of length " + newFile.length());
    }
  }

  private static void createProvidedReplicas(Configuration conf) {
    long numReplicas = (long) Math.ceil((double) FILE_LEN/BLK_LEN);
    File providedFile = new File(BASE_DIR, FILE_NAME);
    replicas = new ArrayList<ProvidedReplica>();

    LOG.info("Creating " + numReplicas + " provided replicas");
    for (int i=0; i<numReplicas; i++) {
      long currentReplicaLength =
          FILE_LEN >= (i+1)*BLK_LEN ? BLK_LEN : FILE_LEN - i*BLK_LEN;
      replicas.add(
          new FinalizedProvidedReplica(i, providedFile.toURI(), i*BLK_LEN,
          currentReplicaLength, 0, null, null, conf, null));
    }
  }

  @Before
  public void setUp() throws IOException {
    createFileIfNotExists(new File(BASE_DIR).getAbsolutePath());
    createProvidedReplicas(new Configuration());
  }

  /**
   * Checks if {@code ins} matches the provided file from offset
   * {@code fileOffset} for length {@ dataLength}.
   * @param file the local file
   * @param ins input stream to compare against
   * @param fileOffset offset
   * @param dataLength length
   * @throws IOException
   */
  public static void verifyReplicaContents(File file,
      InputStream ins, long fileOffset, long dataLength)
          throws IOException {

    InputStream fileIns = new FileInputStream(file);
    fileIns.skip(fileOffset);

    try (ReadableByteChannel i =
        Channels.newChannel(new BoundedInputStream(fileIns, dataLength))) {
      try (ReadableByteChannel j = Channels.newChannel(ins)) {
        ByteBuffer ib = ByteBuffer.allocate(4096);
        ByteBuffer jb = ByteBuffer.allocate(4096);
        while (true) {
          int il = i.read(ib);
          int jl = j.read(jb);
          if (il < 0 || jl < 0) {
            assertEquals(il, jl);
            break;
          }
          ib.flip();
          jb.flip();
          int cmp = Math.min(ib.remaining(), jb.remaining());
          for (int k = 0; k < cmp; ++k) {
            assertEquals(ib.get(), jb.get());
          }
          ib.compact();
          jb.compact();
        }
      }
    }
  }

  @Test
  public void testProvidedReplicaRead() throws IOException {

    File providedFile = new File(BASE_DIR, FILE_NAME);
    for (int i = 0; i < replicas.size(); i++) {
      ProvidedReplica replica = replicas.get(i);
      // block data should exist!
      assertTrue(replica.blockDataExists());
      assertEquals(providedFile.toURI(), replica.getBlockURI());
      verifyReplicaContents(providedFile, replica.getDataInputStream(0),
          BLK_LEN*i, replica.getBlockDataLength());
    }
    LOG.info("All replica contents verified");

    providedFile.delete();
    // the block data should no longer be found!
    for(int i=0; i < replicas.size(); i++) {
      ProvidedReplica replica = replicas.get(i);
      assertTrue(!replica.blockDataExists());
    }
  }

}
