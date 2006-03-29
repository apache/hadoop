/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import junit.framework.AssertionFailedError;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FilenameFilter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.logging.Logger;
import java.util.Random;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Test DFS.
 * ClusterTestDFS is a JUnit test for DFS using "pseudo multiprocessing" (or 
 more strictly, pseudo distributed) meaning all daemons run in one process 
 and sockets are used to communicate between daemons.  The test permutes
 * various block sizes, number of files, file sizes, and number of
 * datanodes.  After creating 1 or more files and filling them with random
 * data, one datanode is shutdown, and then the files are verfified.
 * Next, all the random test files are deleted and we test for leakage
 * (non-deletion) by directly checking the real directories corresponding
 * to the datanodes still running.
 * <p>
 * Usage notes: TEST_PERMUTATION_MAX can be adjusted to perform more or
 * less testing of permutations.  The ceiling of useful permutation is
 * TEST_PERMUTATION_MAX_CEILING.
 * <p>
 * DFSClient emits many messages that can be ignored like:
 * "Failed to connect to *:7000:java.net.ConnectException: Connection refused: connect"
 * because a datanode is forced to close during testing.
 * <p>
 * Warnings about "Zero targets found" can be ignored (these are naggingly
 * emitted even though it is not possible to achieve the desired replication
 * level with the number of active datanodes.)
 * <p>
 * Possible Extensions:
 * <p>Bring a datanode down and restart it to verify reconnection to namenode.
 * <p>Simulate running out of disk space on one datanode only.
 * <p>Bring the namenode down and restart it to verify that datanodes reconnect.
 * <p>
 * <p>For a another approach to filesystem testing, see the high level
 * (HadoopFS level) test {@link org.apache.hadoop.fs.TestFileSystem}.
 * @author Paul Baclace
 */
public class ClusterTestDFS extends TestCase implements FSConstants {
  private static final Logger LOG =
      LogFormatter.getLogger("org.apache.hadoop.dfs.ClusterTestDFS");

  private static Configuration conf = new Configuration();
  private static int BUFFER_SIZE =
      conf.getInt("io.file.buffer.size", 4096);

  private static int testCycleNumber = 0;

  /**
   * all DFS test files go under this base directory
   */
  private static String baseDirSpecified;

  /**
   * base dir as File
   */
  private static File baseDir;

  /** DFS block sizes to permute over in multiple test cycles
   * (array length should be prime).
   */
  private static final int[] BLOCK_SIZES = {100000, 4096};

  /** DFS file sizes to permute over in multiple test cycles
   * (array length should be prime).
   */
  private static final int[] FILE_SIZES =
      {100000, 100001, 4095, 4096, 4097, 1000000, 1000001};

  /** DFS file counts to permute over in multiple test cycles
   * (array length should be prime).
   */
  private static final int[] FILE_COUNTS = {1, 10, 100};

  /** Number of useful permutations or test cycles.
   * (The 2 factor represents the alternating 2 or 3 number of datanodes
   * started.)
   */
  private static final int TEST_PERMUTATION_MAX_CEILING =
    BLOCK_SIZES.length * FILE_SIZES.length * FILE_COUNTS.length * 2;

  /** Number of permutations of DFS test parameters to perform.
   * If this is greater than ceiling TEST_PERMUTATION_MAX_CEILING, then the
   * ceiling value is used.
   */
  private static final int TEST_PERMUTATION_MAX = 3;
  private Constructor randomDataGeneratorCtor = null;

  static {
    baseDirSpecified = System.getProperty("test.dfs.data", "/tmp/dfs_test");
    baseDir = new File(baseDirSpecified);
  }

  protected void setUp() throws Exception {
    super.setUp();
    conf.setBoolean("test.dfs.same.host.targets.allowed", true);
  }

 /**
  * Remove old files from temp area used by this test case and be sure
  * base temp directory can be created.
  */
  protected void prepareTempFileSpace() {
    if (baseDir.exists()) {
      try { // start from a blank slate
        FileUtil.fullyDelete(baseDir, conf);
      } catch (Exception ignored) {
      }
    }
    baseDir.mkdirs();
    if (!baseDir.isDirectory()) {
      throw new RuntimeException("Value of root directory property test.dfs.data for dfs test is not a directory: "
          + baseDirSpecified);
    }
  }

  /**
   * Pseudo Distributed FS Test.
   * Test DFS by running all the necessary daemons in one process.
   * Test various block sizes, number of files, disk space consumption,
   * and leakage.
   *
   * @throws Exception
   */
  public void testFsPseudoDistributed()
      throws Exception {
    while (testCycleNumber < TEST_PERMUTATION_MAX &&
        testCycleNumber < TEST_PERMUTATION_MAX_CEILING) {
        int blockSize = BLOCK_SIZES[testCycleNumber % BLOCK_SIZES.length];
        int numFiles = FILE_COUNTS[testCycleNumber % FILE_COUNTS.length];
        int fileSize = FILE_SIZES[testCycleNumber % FILE_SIZES.length];
        prepareTempFileSpace();
        testFsPseudoDistributed(fileSize, numFiles, blockSize,
            (testCycleNumber % 2) + 2);
    }
  }

  /**
   * Pseudo Distributed FS Testing.
   * Do one test cycle with given parameters.
   *
   * @param nBytes         number of bytes to write to each file.
   * @param numFiles       number of files to create.
   * @param blockSize      block size to use for this test cycle.
   * @param initialDNcount number of datanodes to create
   * @throws Exception
   */
  public void testFsPseudoDistributed(long nBytes, int numFiles,
                                      int blockSize, int initialDNcount)
      throws Exception {
    long startTime = System.currentTimeMillis();
    int bufferSize = Math.min(BUFFER_SIZE, blockSize);
    boolean checkDataDirsEmpty = false;
    int iDatanodeClosed = 0;
    Random randomDataGenerator = makeRandomDataGenerator();
    final int currentTestCycleNumber = testCycleNumber;
    msg("using randomDataGenerator=" + randomDataGenerator.getClass().getName());

    //
    //     modify config for test

    //
    // set given config param to override other config settings
    conf.setInt("test.dfs.block_size", blockSize);
    // verify that config changed
    assertTrue(blockSize == conf.getInt("test.dfs.block_size", 2)); // 2 is an intentional obviously-wrong block size
    // downsize for testing (just to save resources)
    conf.setInt("dfs.namenode.handler.count", 3);
    if (false) { //  use MersenneTwister, if present
      conf.set("hadoop.random.class",
                          "org.apache.hadoop.util.MersenneTwister");
    }
    conf.setLong("dfs.blockreport.intervalMsec", 50*1000L);
    conf.setLong("dfs.datanode.startupMsec", 15*1000L);

    String nameFSDir = baseDirSpecified + "/name";
    msg("----Start Test Cycle=" + currentTestCycleNumber +
        " test.dfs.block_size=" + blockSize +
        " nBytes=" + nBytes +
        " numFiles=" + numFiles +
        " initialDNcount=" + initialDNcount);

    //
    //          start a NameNode

    int nameNodePort = 9000 + testCycleNumber++; // ToDo: settable base port
    String nameNodeSocketAddr = "localhost:" + nameNodePort;
    NameNode nameNodeDaemon = new NameNode(new File(nameFSDir), nameNodePort, conf);
    DFSClient dfsClient = null;
    try {
      //
      //        start some DataNodes
      //
      ArrayList listOfDataNodeDaemons = new ArrayList();
      conf.set("fs.default.name", nameNodeSocketAddr);
      for (int i = 0; i < initialDNcount; i++) {
        // uniquely config real fs path for data storage for this datanode
        String dataDir = baseDirSpecified + "/datanode" + i;
        conf.set("dfs.data.dir", dataDir);
        DataNode dn = DataNode.makeInstanceForDir(dataDir, conf);
        if (dn != null) {
          listOfDataNodeDaemons.add(dn);
          (new Thread(dn, "DataNode" + i + ": " + dataDir)).start();
        }
      }
      try {
        assertTrue("insufficient datanodes for test to continue",
            (listOfDataNodeDaemons.size() >= 2));

        //
        //          wait for datanodes to report in
        awaitQuiescence();

        //  act as if namenode is a remote process
        dfsClient = new DFSClient(new InetSocketAddress("localhost", nameNodePort), conf);

        //
        //           write nBytes of data using randomDataGenerator to numFiles
        //
        ArrayList testfilesList = new ArrayList();
        byte[] buffer = new byte[bufferSize];
        UTF8 testFileName = null;
        for (int iFileNumber = 0; iFileNumber < numFiles; iFileNumber++) {
          testFileName = new UTF8("/f" + iFileNumber);
          testfilesList.add(testFileName);
          FSOutputStream nos = dfsClient.create(testFileName, false);
          try {
            for (long nBytesWritten = 0L;
                 nBytesWritten < nBytes;
                 nBytesWritten += buffer.length) {
              if ((nBytesWritten + buffer.length) > nBytes) {
                // calculate byte count needed to exactly hit nBytes in length
                //  to keep randomDataGenerator in sync during the verify step
                int pb = (int) (nBytes - nBytesWritten);
                byte[] bufferPartial = new byte[pb];
                randomDataGenerator.nextBytes(bufferPartial);
                nos.write(bufferPartial);
              } else {
                randomDataGenerator.nextBytes(buffer);
                nos.write(buffer);
              }
            }
          } finally {
            nos.flush();
            nos.close();
          }
        }

        //
        // No need to wait for blocks to be replicated because replication
        //  is supposed to be complete when the file is closed.
        //

        //
        //                     take one datanode down
        iDatanodeClosed =
            currentTestCycleNumber % listOfDataNodeDaemons.size();
        DataNode dn = (DataNode) listOfDataNodeDaemons.get(iDatanodeClosed);
        msg("shutdown datanode daemon " + iDatanodeClosed +
            " dn=" + dn.data);
        try {
          dn.shutdown();
        } catch (Exception e) {
          msg("ignoring datanode shutdown exception=" + e);
        }

        //
        //          verify data against a "rewound" randomDataGenerator
        //               that all of the data is intact
        long lastLong = randomDataGenerator.nextLong();
        randomDataGenerator = makeRandomDataGenerator(); // restart (make new) PRNG
        ListIterator li = testfilesList.listIterator();
        while (li.hasNext()) {
          testFileName = (UTF8) li.next();
          FSInputStream nis = dfsClient.open(testFileName);
          byte[] bufferGolden = new byte[bufferSize];
          int m = 42;
          try {
            while (m != -1) {
              m = nis.read(buffer);
              if (m == buffer.length) {
                randomDataGenerator.nextBytes(bufferGolden);
                assertBytesEqual(buffer, bufferGolden, buffer.length);
              } else if (m > 0) {
                byte[] bufferGoldenPartial = new byte[m];
                randomDataGenerator.nextBytes(bufferGoldenPartial);
                assertBytesEqual(buffer, bufferGoldenPartial, bufferGoldenPartial.length);
              }
            }
          } finally {
            nis.close();
          }
        }
        // verify last randomDataGenerator rand val to ensure last file length was checked
        long lastLongAgain = randomDataGenerator.nextLong();
        assertEquals(lastLong, lastLongAgain);
        msg("Finished validating all file contents");

        //
        //                    now delete all the created files
        msg("Delete all random test files under DFS via remaining datanodes");
        li = testfilesList.listIterator();
        while (li.hasNext()) {
          testFileName = (UTF8) li.next();
          assertTrue(dfsClient.delete(testFileName));
        }

        //
        //                   wait for delete to be propagated
        //                  (unlike writing files, delete is lazy)
        msg("Test thread sleeping while datanodes propagate delete...");
        awaitQuiescence();
        msg("Test thread awakens to verify file contents");

        //
        //             check that the datanode's block directory is empty
        //                (except for datanode that had forced shutdown)
        checkDataDirsEmpty = true; // do it during finally clause

      } catch (AssertionFailedError afe) {
        throw afe;
      } catch (Throwable t) {
        msg("Unexpected exception_b: " + t);
        t.printStackTrace();
      } finally {
        //
        // shut down datanode daemons (this takes advantage of being same-process)
        msg("begin shutdown of all datanode daemons for test cycle " +
            currentTestCycleNumber);

        for (int i = 0; i < listOfDataNodeDaemons.size(); i++) {
          DataNode dataNode = (DataNode) listOfDataNodeDaemons.get(i);
          if (i != iDatanodeClosed) {
            try {
              if (checkDataDirsEmpty) {
                File dataDir = new File(dataNode.data.diskUsage.getDirPath());
                assertNoBlocks(dataDir);

              }
              dataNode.shutdown();
            } catch (Exception e) {
              msg("ignoring exception during (all) datanode shutdown, e=" + e);
            }
          }
        }
      }
      msg("finished shutdown of all datanode daemons for test cycle " +
          currentTestCycleNumber);
      if (dfsClient != null) {
        try {
          msg("close down subthreads of DFSClient");
          dfsClient.close();
        } catch (Exception ignored) { }
        msg("finished close down of DFSClient");
      }
    } catch (AssertionFailedError afe) {
      throw afe;
    } catch (Throwable t) {
      msg("Unexpected exception_a: " + t);
      t.printStackTrace();
    } finally {
      // shut down namenode daemon (this takes advantage of being same-process)
      msg("begin shutdown of namenode daemon for test cycle " +
          currentTestCycleNumber);
      try {
        nameNodeDaemon.stop();
      } catch (Exception e) {
        msg("ignoring namenode shutdown exception=" + e);
      }
      msg("finished shutdown of namenode daemon for test cycle " +
          currentTestCycleNumber);
    }
    msg("test cycle " + currentTestCycleNumber + " elapsed time=" +
        (System.currentTimeMillis() - startTime) / 1000. + "sec");
    msg("threads still running (look for stragglers): ");
    msg(summarizeThreadGroup());
  }

  private void assertNoBlocks(File datanodeDir) {
    File datanodeDataDir = new File(datanodeDir, "data");
    String[] blockFilenames =
        datanodeDataDir.list(
            new FilenameFilter() {
              public boolean accept(File dir, String name){
                return Block.isBlockFilename(new File(dir, name));}});
    // if this fails, the delete did not propagate because either
    //   awaitQuiescence() returned before the disk images were removed
    //   or a real failure was detected.
    assertTrue(" data dir not empty: " + datanodeDataDir,
               blockFilenames.length==0);
  }

  /**
   * Make a data generator.
   * Allows optional use of high quality PRNG by setting property
   * hadoop.random.class to the full class path of a subclass of
   * java.util.Random such as "...util.MersenneTwister".
   * The property test.dfs.random.seed can supply a seed for reproducible
   * testing (a default is set here if property is not set.)
   */
  private Random makeRandomDataGenerator() {
    long seed = conf.getLong("test.dfs.random.seed", 0xB437EF);
    try {
      if (randomDataGeneratorCtor == null) {
        // lazy init
        String rndDataGenClassname =
            conf.get("hadoop.random.class", "java.util.Random");
        Class clazz = Class.forName(rndDataGenClassname);
        randomDataGeneratorCtor = clazz.getConstructor(new Class[]{Long.TYPE});
      }

      if (randomDataGeneratorCtor != null) {
        Object arg[] = {new Long(seed)};
        return (Random) randomDataGeneratorCtor.newInstance(arg);
      }
    } catch (ClassNotFoundException absorb) {
    } catch (NoSuchMethodException absorb) {
    } catch (SecurityException absorb) {
    } catch (InstantiationException absorb) {
    } catch (IllegalAccessException absorb) {
    } catch (IllegalArgumentException absorb) {
    } catch (InvocationTargetException absorb) {
    }

    // last resort
    return new java.util.Random(seed);
  }

  /** Wait for the DFS datanodes to become quiescent.
   * The initial implementation is to sleep for some fixed amount of time,
   * but a better implementation would be to really detect when distributed
   * operations are completed.
   * @throws InterruptedException
   */
  private void awaitQuiescence() throws InterruptedException {
    // ToDo: Need observer pattern, not static sleep
    // Doug suggested that the block report interval could be made shorter
    //   and then observing that would be a good way to know when an operation
    //   was complete (quiescence detect).
    sleepAtLeast(60000);
  }

  private void assertBytesEqual(byte[] buffer, byte[] bufferGolden, int len) {
    for (int i = 0; i < len; i++) {
      assertEquals(buffer[i], bufferGolden[i]);
    }
  }

  private void msg(String s) {
    //System.out.println(s);
    LOG.info(s);
  }

  public static void sleepAtLeast(int tmsec) {
    long t0 = System.currentTimeMillis();
    long t1 = t0;
    long tslept = t1 - t0;
    while (tmsec > tslept) {
      try {
        long tsleep = tmsec - tslept;
        Thread.sleep(tsleep);
        t1 = System.currentTimeMillis();
      }  catch (InterruptedException ie) {
        t1 = System.currentTimeMillis();
      }
      tslept = t1 - t0;
    }
  }

  public static String summarizeThreadGroup() {
    int n = 10;
    int k = 0;
    Thread[] tarray = null;
    StringBuffer sb = new StringBuffer(500);
    do {
      n = n * 10;
      tarray = new Thread[n];
      k = Thread.enumerate(tarray);
    } while (k == n); // while array is too small...
    for (int i = 0; i < k; i++) {
      Thread thread = tarray[i];
      sb.append(thread.toString());
      sb.append("\n");
    }
    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
    String usage = "Usage: ClusterTestDFS (no args)";
    if (args.length != 0) {
      System.err.println(usage);
      System.exit(-1);
    }
    String[] testargs = {"org.apache.hadoop.dfs.ClusterTestDFS"};
    junit.textui.TestRunner.main(testargs);
  }

}
