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

import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestWriteRead {

  // Junit test settings. 
  private static final int WR_NTIMES = 350; 
  private static final int WR_CHUNK_SIZE = 10000;

  private static final int BUFFER_SIZE = 8192 * 100;
  private static final String ROOT_DIR = "/tmp/";
  private static final long blockSize = 1024*100;

  // command-line options. Different defaults for unit test vs real cluster
  String filenameOption = ROOT_DIR + "fileX1";
  int chunkSizeOption = 10000;
  int loopOption = 10;

  private MiniDFSCluster cluster;
  private Configuration conf; // = new HdfsConfiguration();
  private FileSystem mfs; // = cluster.getFileSystem();
  private FileContext mfc; // = FileContext.getFileContext();

  // configuration
  private boolean useFCOption = false; // use either FileSystem or FileContext
  private boolean verboseOption = true;
  private boolean positionReadOption = false;
  private boolean truncateOption = false;
  private final boolean abortTestOnFailure = true;

  static private Log LOG = LogFactory.getLog(TestWriteRead.class);

  @Before
  public void initJunitModeTest() throws Exception {
    LOG.info("initJunitModeTest");

    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize); // 100K
                                                              // blocksize

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    mfs = cluster.getFileSystem();
    mfc = FileContext.getFileContext();

    Path rootdir = new Path(ROOT_DIR);
    mfs.mkdirs(rootdir);
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  // Equivalence of @Before for cluster mode testing.
  private void initClusterModeTest() throws IOException {

    LOG = LogFactory.getLog(TestWriteRead.class);
    LOG.info("initClusterModeTest");

    conf = new Configuration();
    mfc = FileContext.getFileContext();
    mfs = FileSystem.get(conf);
  }

  /** Junit Test reading while writing. */
  @Test
  public void testWriteReadSeq() throws IOException {
    useFCOption = false; 
    positionReadOption = false;
    String fname = filenameOption;
    long rdBeginPos = 0;
    // need to run long enough to fail: takes 25 to 35 seec on Mac
    int stat = testWriteAndRead(fname, WR_NTIMES, WR_CHUNK_SIZE, rdBeginPos);
    LOG.info("Summary status from test1: status= " + stat);
    Assert.assertEquals(0, stat);
  }

  /** Junit Test position read while writing. */
  @Test
  public void testWriteReadPos() throws IOException {
    String fname = filenameOption;
    positionReadOption = true;   // position read
    long rdBeginPos = 0;
    int stat = testWriteAndRead(fname, WR_NTIMES, WR_CHUNK_SIZE, rdBeginPos);
    Assert.assertEquals(0, stat);
  }

  /** Junit Test position read of the current block being written. */
  @Test
  public void testReadPosCurrentBlock() throws IOException {
    String fname = filenameOption;
    positionReadOption = true;   // position read
    int wrChunkSize = (int)(blockSize) + (int)(blockSize/2);
    long rdBeginPos = blockSize+1;
    int numTimes=5;
    int stat = testWriteAndRead(fname, numTimes, wrChunkSize, rdBeginPos);
    Assert.assertEquals(0, stat);
  }
   
  // equivalent of TestWriteRead1
  private int clusterTestWriteRead1() throws IOException {
    long rdBeginPos = 0;
    int stat = testWriteAndRead(filenameOption, loopOption, chunkSizeOption, rdBeginPos);
    return stat;
  }

  /**
   * Open the file to read from begin to end. Then close the file. 
   * Return number of bytes read. 
   * Support both sequential read and position read.
   */
  private long readData(String fname, byte[] buffer, long byteExpected, long beginPosition)
      throws IOException {
    long totalByteRead = 0;
    Path path = getFullyQualifiedPath(fname);

    FSDataInputStream in = null;
    try {
      in = openInputStream(path);

      long visibleLenFromReadStream = ((HdfsDataInputStream)in).getVisibleLength();

      if (visibleLenFromReadStream < byteExpected)
      {
        throw new IOException(visibleLenFromReadStream
            + " = visibleLenFromReadStream < bytesExpected= "
            + byteExpected);
      }

      totalByteRead = readUntilEnd(in, buffer, buffer.length, fname,
          beginPosition, visibleLenFromReadStream, positionReadOption);
      in.close();

      // reading more data than visibleLeng is OK, but not less
      if (totalByteRead + beginPosition < byteExpected ){
        throw new IOException("readData mismatch in byte read: expected=" 
            + byteExpected + " ; got " +  (totalByteRead + beginPosition));
      }
      return totalByteRead + beginPosition;

    } catch (IOException e) {
      throw new IOException("##### Caught Exception in readData. "
          + "Total Byte Read so far = " + totalByteRead + " beginPosition = "
          + beginPosition, e);
    } finally {
      if (in != null)
        in.close();
    }
  }

  /**
   * read chunks into buffer repeatedly until total of VisibleLen byte are read.
   * Return total number of bytes read
   */
  private long readUntilEnd(FSDataInputStream in, byte[] buffer, long size,
      String fname, long pos, long visibleLen, boolean positionReadOption)
      throws IOException {

    if (pos >= visibleLen || visibleLen <= 0)
      return 0;

    int chunkNumber = 0;
    long totalByteRead = 0;
    long currentPosition = pos;
    int byteRead = 0;
    long byteLeftToRead = visibleLen - pos;
    int byteToReadThisRound = 0;

    if (!positionReadOption) {
      in.seek(pos);
      currentPosition = in.getPos();
    }
    if (verboseOption)
      LOG.info("reader begin: position: " + pos + " ; currentOffset = "
          + currentPosition + " ; bufferSize =" + buffer.length
          + " ; Filename = " + fname);
    try {
      while (byteLeftToRead > 0 && currentPosition < visibleLen) {
        byteToReadThisRound = (int) (byteLeftToRead >= buffer.length 
            ? buffer.length : byteLeftToRead);
        if (positionReadOption) {
          byteRead = in.read(currentPosition, buffer, 0, byteToReadThisRound);
        } else {
          byteRead = in.read(buffer, 0, byteToReadThisRound);
        }
        if (byteRead <= 0)
          break;
        chunkNumber++;
        totalByteRead += byteRead;
        currentPosition += byteRead;
        byteLeftToRead -= byteRead;

        if (verboseOption) {
          LOG.info("reader: Number of byte read: " + byteRead
              + " ; totalByteRead = " + totalByteRead + " ; currentPosition="
              + currentPosition + " ; chunkNumber =" + chunkNumber
              + "; File name = " + fname);
        }
      }
    } catch (IOException e) {
      throw new IOException(
          "#### Exception caught in readUntilEnd: reader  currentOffset = "
              + currentPosition + " ; totalByteRead =" + totalByteRead
              + " ; latest byteRead = " + byteRead + "; visibleLen= "
              + visibleLen + " ; bufferLen = " + buffer.length
              + " ; Filename = " + fname, e);
    }

    if (verboseOption)
      LOG.info("reader end:   position: " + pos + " ; currentOffset = "
          + currentPosition + " ; totalByteRead =" + totalByteRead
          + " ; Filename = " + fname);

    return totalByteRead;
  }

  private void writeData(FSDataOutputStream out, byte[] buffer, int length)
      throws IOException {

    int totalByteWritten = 0;
    int remainToWrite = length;

    while (remainToWrite > 0) {
      int toWriteThisRound = remainToWrite > buffer.length ? buffer.length
          : remainToWrite;
      out.write(buffer, 0, toWriteThisRound);
      totalByteWritten += toWriteThisRound;
      remainToWrite -= toWriteThisRound;
    }
    if (totalByteWritten != length) {
      throw new IOException("WriteData: failure in write. Attempt to write " 
          + length + " ; written=" + totalByteWritten);
    }
  }

  /**
   * Common routine to do position read while open the file for write. 
   * After each iteration of write, do a read of the file from begin to end. 
   * Return 0 on success, else number of failure.
   */
  private int testWriteAndRead(String fname, int loopN, int chunkSize, long readBeginPosition)
      throws IOException {

    int countOfFailures = 0;
    long byteVisibleToRead = 0;
    FSDataOutputStream out = null;

    byte[] outBuffer = new byte[BUFFER_SIZE];
    byte[] inBuffer = new byte[BUFFER_SIZE];

    for (int i = 0; i < BUFFER_SIZE; i++) {
      outBuffer[i] = (byte) (i & 0x00ff);
    }

    try {
      Path path = getFullyQualifiedPath(fname);
      long fileLengthBeforeOpen = 0;

      if (ifExists(path)) {
        if (truncateOption) {
          out = useFCOption ? mfc.create(path,EnumSet.of(CreateFlag.OVERWRITE)): 
                mfs.create(path, truncateOption);
          LOG.info("File already exists. File open with Truncate mode: "+ path);
        } else {
          out = useFCOption ? mfc.create(path, EnumSet.of(CreateFlag.APPEND))
              : mfs.append(path);
          fileLengthBeforeOpen = getFileLengthFromNN(path);
          LOG.info("File already exists of size " + fileLengthBeforeOpen
              + " File open for Append mode: " + path);
        }
      } else {
        out = useFCOption ? mfc.create(path, EnumSet.of(CreateFlag.CREATE))
            : mfs.create(path);
      }

      long totalByteWritten = fileLengthBeforeOpen;
      long totalByteVisible = fileLengthBeforeOpen;
      long totalByteWrittenButNotVisible = 0;

      boolean toFlush;
      for (int i = 0; i < loopN; i++) {
        toFlush = (i % 2) == 0;

        writeData(out, outBuffer, chunkSize);

        totalByteWritten += chunkSize;

        if (toFlush) {
          out.hflush();
          totalByteVisible += chunkSize + totalByteWrittenButNotVisible;
          totalByteWrittenButNotVisible = 0;
        } else {
          totalByteWrittenButNotVisible += chunkSize;
        }

        if (verboseOption) {
          LOG.info("TestReadWrite - Written " + chunkSize
              + ". Total written = " + totalByteWritten
              + ". TotalByteVisible = " + totalByteVisible + " to file "
              + fname);
        }
        byteVisibleToRead = readData(fname, inBuffer, totalByteVisible, readBeginPosition);

        String readmsg = "Written=" + totalByteWritten + " ; Expected Visible="
            + totalByteVisible + " ; Got Visible=" + byteVisibleToRead
            + " of file " + fname;

        if (byteVisibleToRead >= totalByteVisible
            && byteVisibleToRead <= totalByteWritten) {
          readmsg = "pass: reader sees expected number of visible byte. "
              + readmsg + " [pass]";
        } else {
          countOfFailures++;
          readmsg = "fail: reader see different number of visible byte. "
              + readmsg + " [fail]";
          if (abortTestOnFailure) {
            throw new IOException(readmsg);
          }
        }
        LOG.info(readmsg);
      }

      // test the automatic flush after close
      writeData(out, outBuffer, chunkSize);
      totalByteWritten += chunkSize;
      totalByteVisible += chunkSize + totalByteWrittenButNotVisible;
      totalByteWrittenButNotVisible += 0;

      out.close();

      byteVisibleToRead = readData(fname, inBuffer, totalByteVisible, readBeginPosition);

      String readmsg2 = "Written=" + totalByteWritten + " ; Expected Visible="
          + totalByteVisible + " ; Got Visible=" + byteVisibleToRead
          + " of file " + fname;
      String readmsg;
      
      if (byteVisibleToRead >= totalByteVisible
          && byteVisibleToRead <= totalByteWritten) {
        readmsg = "pass: reader sees expected number of visible byte on close. "
            + readmsg2 + " [pass]";
      } else {
        countOfFailures++;
        readmsg = "fail: reader sees different number of visible byte on close. "
            + readmsg2 + " [fail]";
        LOG.info(readmsg);
        if (abortTestOnFailure)
          throw new IOException(readmsg);
      }

      // now check if NN got the same length 
      long lenFromFc = getFileLengthFromNN(path);
      if (lenFromFc != byteVisibleToRead){
        readmsg = "fail: reader sees different number of visible byte from NN "
          + readmsg2 + " [fail]";
        throw new IOException(readmsg);   
      }
    } catch (IOException e) {
      throw new IOException(
          "##### Caught Exception in testAppendWriteAndRead. Close file. "
              + "Total Byte Read so far = " + byteVisibleToRead, e);
    } finally {
      if (out != null)
        out.close();
    }
    return -countOfFailures;
  }

  // //////////////////////////////////////////////////////////////////////
  // // helper function:
  // /////////////////////////////////////////////////////////////////////
  private FSDataInputStream openInputStream(Path path) throws IOException {
    FSDataInputStream in = useFCOption ? mfc.open(path) : mfs.open(path);
    return in;
  }

  // length of a file (path name) from NN.
  private long getFileLengthFromNN(Path path) throws IOException {
    FileStatus fileStatus = useFCOption ? mfc.getFileStatus(path) : 
        mfs.getFileStatus(path);
    return fileStatus.getLen();
  }

  private boolean ifExists(Path path) throws IOException {
    return useFCOption ? mfc.util().exists(path) : mfs.exists(path);
  }

  private Path getFullyQualifiedPath(String pathString) {
    return useFCOption ? mfc.makeQualified(new Path(ROOT_DIR, pathString))
        : mfs.makeQualified(new Path(ROOT_DIR, pathString));
  }

  private void usage() {
    LOG.info("Usage: [-useSeqRead | -usePosRead] [-append|truncate]"
        + " -chunkSize nn -loop ntimes  -f filename");
    System.out.println("Usage: [-useSeqRead | -usePosRead] [-append|truncate]"
        + " -chunkSize nn -loop ntimes  -f filename");
    System.out.println("Defaults: -chunkSize=10000, -loop=10, -f=/tmp/fileX1, "
        + "use sequential read, use append mode if file already exists");
    System.exit(0);
  }

  private void dumpOptions() {
    LOG.info("  Option setting: filenameOption = " + filenameOption);
    LOG.info("  Option setting: chunkSizeOption = " + chunkSizeOption);
    LOG.info("  Option setting: loopOption = " + loopOption);
    LOG.info("  Option setting: posReadOption = " + positionReadOption);
    LOG.info("  Option setting: truncateOption = " + truncateOption);
    LOG.info("  Option setting: verboseOption = " + verboseOption);
  }

  private void getCmdLineOption(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-f")) {
        filenameOption = args[++i];
      } else if (args[i].equals("-chunkSize")) {
        chunkSizeOption = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-loop")) {
        loopOption = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-usePosRead")) {
        positionReadOption = true;
      } else if (args[i].equals("-useSeqRead")) {
        positionReadOption = false;
      } else if (args[i].equals("-truncate")) {
        truncateOption = true;
      } else if (args[i].equals("-append")) {
        truncateOption = false;
      } else if (args[i].equals("-verbose")) {
        verboseOption = true;
      } else if (args[i].equals("-noVerbose")) {
        verboseOption = false;
      } else {
        usage();
      }
    }
    if (verboseOption)
      dumpOptions();
    return;
  }

  /**
   * Entry point of the test when using a real cluster. 
   * Usage: [-loop ntimes] [-chunkSize nn] [-f filename] 
   *     [-useSeqRead |-usePosRead] [-append |-truncate] [-verbose |-noVerbose]
   * -loop: iterate ntimes: each iteration consists of a write, then a read 
   * -chunkSize: number of byte for each write
   * -f filename: filename to write and read 
   * [-useSeqRead | -usePosRead]: use Position Read, or default Sequential Read 
   * [-append | -truncate]: if file already exist, Truncate or default Append 
   * [-verbose | -noVerbose]: additional debugging messages if verbose is on
   * Default: -loop = 10; -chunkSize = 10000; -f filename = /tmp/fileX1
   *     Use Sequential Read, Append Mode, verbose on.
   */
  public static void main(String[] args) {
    try {
      TestWriteRead trw = new TestWriteRead();
      trw.initClusterModeTest();
      trw.getCmdLineOption(args);

      int stat = trw.clusterTestWriteRead1();

      if (stat == 0) {
        System.out.println("Status: clusterTestWriteRead1 test PASS");
      } else {
        System.out.println("Status: clusterTestWriteRead1 test FAIL with "
                + stat + " failures");
      }
      System.exit(stat);
    } catch (IOException e) {
      LOG.info("#### Exception in Main");
      e.printStackTrace();
      System.exit(-2);
    }
  }
}
