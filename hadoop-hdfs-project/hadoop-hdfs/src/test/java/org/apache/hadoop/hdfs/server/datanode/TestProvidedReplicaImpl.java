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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ProvidedVolumeImpl;
import org.apache.hadoop.io.IOUtils;
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
  private static ProvidedVolumeImpl mockFsVolume;

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

  private static void createProvidedReplicas(Configuration conf)
      throws IOException {
    long numReplicas = (long) Math.ceil((double) FILE_LEN/BLK_LEN);
    File providedFile = new File(BASE_DIR, FILE_NAME);
    replicas = new ArrayList<ProvidedReplica>();
    FileSystem localFS = FileSystem.get(providedFile.toURI(), conf);
    LOG.info("Creating " + numReplicas + " provided replicas");
    for (int i=0; i<numReplicas; i++) {
      long currentReplicaLength =
          FILE_LEN >= (i+1)*BLK_LEN ? BLK_LEN : FILE_LEN - i*BLK_LEN;
      replicas.add(
          new FinalizedProvidedReplica(i, providedFile.toURI(), i*BLK_LEN,
          currentReplicaLength, 0, null, mockFsVolume, conf, localFS));
    }
  }

  @Before
  public void setUp() throws IOException {
    mockFsVolume = mock(ProvidedVolumeImpl.class);
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

  @Test
  public void testContainsBlock() throws URISyntaxException {
    // containsBlock() returns true if both URIs are null, or if the
    // first argument is a prefix of the second.
    assertTrue(ProvidedReplica.containsBlock(null, null));
    assertTrue(ProvidedReplica.containsBlock(new URI("file:/a/b/c/"),
        new URI("file:/a/b/c/d/e.file")));
    assertTrue(ProvidedReplica.containsBlock(new URI("/a/b/c/"),
        new URI("file:/a/b/c/d/e.file")));
    assertTrue(ProvidedReplica.containsBlock(new URI("/a/b/c"),
        new URI("file:/a/b/c/d/e.file")));
    assertTrue(ProvidedReplica.containsBlock(new URI("/a/b/c/"),
        new URI("/a/b/c/d/e.file")));
    assertTrue(ProvidedReplica.containsBlock(new URI("file:/a/b/c/"),
        new URI("/a/b/c/d/e.file")));
    assertTrue(ProvidedReplica.containsBlock(new URI("s3a:/bucket1/dir1/"),
        new URI("s3a:/bucket1/dir1/temp.txt")));

    // it returns false otherwise.
    assertFalse(ProvidedReplica.containsBlock(new URI("file:/a"), null));
    assertFalse(ProvidedReplica.containsBlock(new URI("/a/b/e"),
        new URI("file:/a/b/c/d/e.file")));
    assertFalse(ProvidedReplica.containsBlock(new URI("file:/a/b/e"),
        new URI("file:/a/b/c/d/e.file")));
    assertFalse(ProvidedReplica.containsBlock(new URI("s3a:/bucket2/dir1/"),
        new URI("s3a:/bucket1/dir1/temp.txt")));
    assertFalse(ProvidedReplica.containsBlock(new URI("s3a:/bucket1/dir1/"),
        new URI("s3a:/bucket1/temp.txt")));
    assertFalse(ProvidedReplica.containsBlock(new URI("/bucket1/dir1/"),
        new URI("s3a:/bucket1/dir1/temp.txt")));
  }

  /**
   * Tests if a ProvidedReplica uses the correct FileSystem to access
   * remote paths.
   * @throws Exception
   */
  @Test
  public void testRemoteFS() throws Exception {
    Configuration conf = new Configuration();
    // a FS ref for the local FileSystem
    FileSystem localFS = FileSystem.get(new File(BASE_DIR).toURI(), conf);
    int blockId = 1001;
    File blockFile1 = new File(BASE_DIR, String.valueOf(blockId));
    blockFile1.createNewFile();
    blockFile1.deleteOnExit();
    // create a PROVIDED replica for a local file.
    ProvidedReplica r = new FinalizedProvidedReplica(blockId,
        blockFile1.toURI(), 0, 1024, 1, null, mockFsVolume, conf, localFS);
    // it should use the FS passed in.
    assertEquals(localFS, r.getRemoteFS());
    assertNotNull(r.getDataInputStream(0));

    // same test as above but with a file in a different directory.
    File blockFile2 = new File(new File(BASE_DIR).getParentFile(),
        "newBlockFile.dat");
    blockFile2.createNewFile();
    blockFile2.deleteOnExit();
    r = new FinalizedProvidedReplica(blockId,
        blockFile2.toURI(), 0, 1024, 1, null, mockFsVolume, conf, localFS);
    // the FS passed in should be used.
    assertEquals(localFS, r.getRemoteFS());
    assertNotNull(r.getDataInputStream(0));

    // test with a remote hdfs:// URI
    URI remoteBlockURI = new URI("hdfs://localhost/path/to/file.dat");
    r = new FinalizedProvidedReplica(blockId, remoteBlockURI,
        0, 1024, 1, null, mockFsVolume, conf, localFS);
    // the FS used cannot be the same as the local FS.
    assertNotEquals(localFS, r.getRemoteFS());

    // test the other constructor of ProvidedReplica for the hdfs:// URI.
    r = new FinalizedProvidedReplica(blockId, new Path(remoteBlockURI),
        "pathSuffix.dat", 0, 1024, 1, null, mockFsVolume, conf, localFS);
    assertNotEquals(localFS, r.getRemoteFS());
  }

  /**
   * Tests that a ProvidedReplica supports path handles.
   *
   * @throws Exception
   */
  @Test
  public void testProvidedReplicaWithPathHandle() throws Exception {

    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();

      DistributedFileSystem fs = cluster.getFileSystem();

      // generate random data
      int chunkSize = 512;
      Random r = new Random(12345L);
      byte[] data = new byte[chunkSize];
      r.nextBytes(data);

      Path file = new Path("/testfile");
      try (FSDataOutputStream fout = fs.create(file)) {
        fout.write(data);
      }

      ProvidedVolumeImpl volume = mock(ProvidedVolumeImpl.class);

      PathHandle pathHandle = fs.getPathHandle(fs.getFileStatus(file),
          Options.HandleOpt.changed(true), Options.HandleOpt.moved(true));
      FinalizedProvidedReplica replica = new FinalizedProvidedReplica(0,
          file.toUri(), 0, chunkSize, 0, pathHandle, volume, conf, fs);
      byte[] content = new byte[chunkSize];
      IOUtils.readFully(replica.getDataInputStream(0), content, 0, chunkSize);
      assertArrayEquals(data, content);

      fs.rename(file, new Path("/testfile.1"));
      // read should continue succeeding after the rename operation
      IOUtils.readFully(replica.getDataInputStream(0), content, 0, chunkSize);
      assertArrayEquals(data, content);

      replica.setPathHandle(null);
      try {
        // expected to fail as URI of the provided replica is no longer valid.
        replica.getDataInputStream(0);
        fail("Expected an exception");
      } catch (IOException e) {
        LOG.info("Expected exception " + e);
      }
    } catch (Exception e) {
      cluster.shutdown();
    }
  }
}
