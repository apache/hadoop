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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/** Utilities for append-related tests */ 
public class AppendTestUtil {
  /** For specifying the random number generator seed,
   *  change the following value:
   */
  static final Long RANDOM_NUMBER_GENERATOR_SEED = null;

  static final Logger LOG = LoggerFactory.getLogger(AppendTestUtil.class);

  private static final Random SEED = new Random();
  static {
    final long seed = RANDOM_NUMBER_GENERATOR_SEED == null?
        SEED.nextLong(): RANDOM_NUMBER_GENERATOR_SEED;
    LOG.info("seed=" + seed);
    SEED.setSeed(seed);
  }

  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      final Random r =  new Random();
      synchronized(SEED) { 
        final long seed = SEED.nextLong();
        r.setSeed(seed);
        LOG.info(Thread.currentThread().getName() + ": seed=" + seed);
      }
      return r;
    }
  };
  static final int BLOCK_SIZE = 1024;
  static final int NUM_BLOCKS = 10;
  static final int FILE_SIZE = NUM_BLOCKS * BLOCK_SIZE + 1;
  static long seed = -1;

  static int nextInt() {return RANDOM.get().nextInt();}
  static int nextInt(int n) {return RANDOM.get().nextInt(n);}
  static int nextLong() {return RANDOM.get().nextInt();}

  public static byte[] randomBytes(long seed, int size) {
    LOG.info("seed=" + seed + ", size=" + size);
    final byte[] b = new byte[size];
    final Random rand = new Random(seed);
    rand.nextBytes(b);
    return b;
  }

  /** @return a random file partition of length n. */
  public static int[] randomFilePartition(int n, int parts) {
    int[] p = new int[parts];
    for(int i = 0; i < p.length; i++) {
      p[i] = nextInt(n - i - 1) + 1;
    }
    Arrays.sort(p);
    for(int i = 1; i < p.length; i++) {
      if (p[i] <= p[i - 1]) {
        p[i] = p[i - 1] + 1;
      }
    }
    
    LOG.info("partition=" + Arrays.toString(p));
    assertTrue("i=0", p[0] > 0 && p[0] < n);
    for(int i = 1; i < p.length; i++) {
      assertTrue("i=" + i, p[i] > p[i - 1] && p[i] < n);
    }
    return p;
  }

  static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      LOG.info("ms=" + ms, e);
    }
  }
  
  /**
   * Returns the reference to a new instance of FileSystem created 
   * with different user name
   * @param conf current Configuration
   * @return FileSystem instance
   * @throws IOException
   * @throws InterruptedException 
   */
  public static FileSystem createHdfsWithDifferentUsername(final Configuration conf
      ) throws IOException, InterruptedException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName()+"_XXX";
    UserGroupInformation ugi = 
      UserGroupInformation.createUserForTesting(username, new String[]{"supergroup"});
    
    return DFSTestUtil.getFileSystemAs(ugi, conf);
  }

  public static void write(OutputStream out, int offset, int length) throws IOException {
    final byte[] bytes = new byte[length];
    for(int i = 0; i < length; i++) {
      bytes[i] = (byte)(offset + i);
    }
    out.write(bytes);
  }
  
  public static void check(FileSystem fs, Path p, long length) throws IOException {
    int i = -1;
    try {
      final FileStatus status = fs.getFileStatus(p);
      FSDataInputStream in = fs.open(p);
      if (in.getWrappedStream() instanceof DFSInputStream) {
        long len = ((DFSInputStream)in.getWrappedStream()).getFileLength();
        assertEquals(length, len);
      } else {
        assertEquals(length, status.getLen());
      }
      
      for(i++; i < length; i++) {
        assertEquals((byte)i, (byte)in.read());  
      }
      i = -(int)length;
      assertEquals(-1, in.read()); //EOF  
      in.close();
    } catch(IOException ioe) {
      throw new IOException("p=" + p + ", length=" + length + ", i=" + i, ioe);
    }
  }

  public static void check(DistributedFileSystem fs, Path p, int position,
      int length) throws IOException {
    byte[] buf = new byte[length];
    int i = 0;
    try {
      FSDataInputStream in = fs.open(p);
      in.read(position, buf, 0, buf.length);
      for(i = position; i < length + position; i++) {
        assertEquals((byte) i, buf[i - position]);
      }
      in.close();
    } catch(IOException ioe) {
      throw new IOException("p=" + p + ", length=" + length + ", i=" + i, ioe);
    }
  }

  /**
   *  create a buffer that contains the entire test file data.
   */
  public static byte[] initBuffer(int size) {
    if (seed == -1)
      seed = nextLong();
    return randomBytes(seed, size);
  }

  /**
   *  Creates a file but does not close it
   *  Make sure to call close() on the returned stream
   *  @throws IOException an exception might be thrown
   */
  public static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
    return fileSys.create(name, true,
        fileSys.getConf().getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, BLOCK_SIZE);
  }

  public static void checkFullFile(FileSystem fs, Path file, int len,
      final byte[] compareContent) throws IOException {
    checkFullFile(fs, file, len, compareContent, file.toString());
  }

  /**
   *  Compare the content of a file created from FileSystem and Path with
   *  the specified byte[] buffer's content
   *  @throws IOException an exception might be thrown
   */
  public static void checkFullFile(FileSystem fs, Path name, int len,
                            final byte[] compareContent, String message) throws IOException {
    checkFullFile(fs, name, len, compareContent, message, true);
  }

  public static void checkFullFile(FileSystem fs, Path name, int len,
      final byte[] compareContent, String message,
      boolean checkFileStatus) throws IOException {
    if (checkFileStatus) {
      final FileStatus status = fs.getFileStatus(name);
      assertEquals("len=" + len + " but status.getLen()=" + status.getLen(),
          len, status.getLen());
    }

    FSDataInputStream stm = fs.open(name);
    byte[] actual = new byte[len];
    stm.readFully(0, actual);
    checkData(actual, 0, compareContent, message);
    stm.close();
  }

  private static void checkData(final byte[] actual, int from,
                                final byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                   expected[from+idx]+" actual "+actual[idx],
                   expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }

  public static void testAppend(FileSystem fs, Path p) throws IOException {
    final int size = 1000;
    final byte[] bytes = randomBytes(seed, size);

    { //create file
      final FSDataOutputStream out = fs.create(p, (short)1);
      out.write(bytes);
      out.close();
      assertEquals(bytes.length, fs.getFileStatus(p).getLen());
    }

    final int appends = 50;
    for (int i = 2; i < appends; i++) {
      //append
      final FSDataOutputStream out = fs.append(p);
      out.write(bytes);
      out.close();
      assertEquals(i * bytes.length, fs.getFileStatus(p).getLen());
    }

    // Check the appended content
    final FSDataInputStream in = fs.open(p);
    for (int i = 0; i < appends - 1; i++) {
      byte[] read = new byte[size];
      in.read(i * bytes.length, read, 0, size);
      assertArrayEquals(bytes, read);
    }
    in.close();
  }
}