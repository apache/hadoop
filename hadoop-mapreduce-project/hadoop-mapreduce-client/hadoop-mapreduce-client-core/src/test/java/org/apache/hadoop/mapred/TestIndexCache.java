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
package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestIndexCache {
  private JobConf conf;
  private FileSystem fs;
  private Path p;

  @Before
  public void setUp() throws IOException {
    conf = new JobConf();
    fs = FileSystem.getLocal(conf).getRaw();
    p =  new Path(System.getProperty("test.build.data", "/tmp"),
        "cache").makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  @Test
  public void testLRCPolicy() throws Exception {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("seed: " + seed);
    fs.delete(p, true);
    conf.setInt(TTConfig.TT_INDEX_CACHE, 1);
    final int partsPerMap = 1000;
    final int bytesPerFile = partsPerMap * 24;
    IndexCache cache = new IndexCache(conf);

    // fill cache
    int totalsize = bytesPerFile;
    for (; totalsize < 1024 * 1024; totalsize += bytesPerFile) {
      Path f = new Path(p, Integer.toString(totalsize, 36));
      writeFile(fs, f, totalsize, partsPerMap);
      IndexRecord rec = cache.getIndexInformation(
        Integer.toString(totalsize, 36), r.nextInt(partsPerMap), f,
        UserGroupInformation.getCurrentUser().getShortUserName());
      checkRecord(rec, totalsize);
    }

    // delete files, ensure cache retains all elem
    for (FileStatus stat : fs.listStatus(p)) {
      fs.delete(stat.getPath(),true);
    }
    for (int i = bytesPerFile; i < 1024 * 1024; i += bytesPerFile) {
      Path f = new Path(p, Integer.toString(i, 36));
      IndexRecord rec = cache.getIndexInformation(Integer.toString(i, 36),
        r.nextInt(partsPerMap), f,
        UserGroupInformation.getCurrentUser().getShortUserName());
      checkRecord(rec, i);
    }

    // push oldest (bytesPerFile) out of cache
    Path f = new Path(p, Integer.toString(totalsize, 36));
    writeFile(fs, f, totalsize, partsPerMap);
    cache.getIndexInformation(Integer.toString(totalsize, 36),
        r.nextInt(partsPerMap), f,
        UserGroupInformation.getCurrentUser().getShortUserName());
    fs.delete(f, false);

    // oldest fails to read, or error
    boolean fnf = false;
    try {
      cache.getIndexInformation(Integer.toString(bytesPerFile, 36),
        r.nextInt(partsPerMap), new Path(p, Integer.toString(bytesPerFile)),
        UserGroupInformation.getCurrentUser().getShortUserName());
    } catch (IOException e) {
      if (e.getCause() == null ||
          !(e.getCause()  instanceof FileNotFoundException)) {
        throw e;
      }
      else {
        fnf = true;
      }
    }
    if (!fnf)
      fail("Failed to push out last entry");
    // should find all the other entries
    for (int i = bytesPerFile << 1; i < 1024 * 1024; i += bytesPerFile) {
      IndexRecord rec = cache.getIndexInformation(Integer.toString(i, 36),
          r.nextInt(partsPerMap), new Path(p, Integer.toString(i, 36)),
          UserGroupInformation.getCurrentUser().getShortUserName());
      checkRecord(rec, i);
    }
    IndexRecord rec = cache.getIndexInformation(Integer.toString(totalsize, 36),
      r.nextInt(partsPerMap), f,
      UserGroupInformation.getCurrentUser().getShortUserName());

    checkRecord(rec, totalsize);
  }

  @Test
  public void testBadIndex() throws Exception {
    final int parts = 30;
    fs.delete(p, true);
    conf.setInt(TTConfig.TT_INDEX_CACHE, 1);
    IndexCache cache = new IndexCache(conf);

    Path f = new Path(p, "badindex");
    FSDataOutputStream out = fs.create(f, false);
    CheckedOutputStream iout = new CheckedOutputStream(out, new CRC32());
    DataOutputStream dout = new DataOutputStream(iout);
    for (int i = 0; i < parts; ++i) {
      for (int j = 0; j < MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8; ++j) {
        if (0 == (i % 3)) {
          dout.writeLong(i);
        } else {
          out.writeLong(i);
        }
      }
    }
    out.writeLong(iout.getChecksum().getValue());
    dout.close();
    try {
      cache.getIndexInformation("badindex", 7, f,
        UserGroupInformation.getCurrentUser().getShortUserName());
      fail("Did not detect bad checksum");
    } catch (IOException e) {
      if (!(e.getCause() instanceof ChecksumException)) {
        throw e;
      }
    }
  }

  @Test
  public void testInvalidReduceNumberOrLength() throws Exception {
    fs.delete(p, true);
    conf.setInt(TTConfig.TT_INDEX_CACHE, 1);
    final int partsPerMap = 1000;
    final int bytesPerFile = partsPerMap * 24;
    IndexCache cache = new IndexCache(conf);

    // fill cache
    Path feq = new Path(p, "invalidReduceOrPartsPerMap");
    writeFile(fs, feq, bytesPerFile, partsPerMap);

    // Number of reducers should always be less than partsPerMap as reducer
    // numbers start from 0 and there cannot be more reducer than parts

    try {
      // Number of reducers equal to partsPerMap
      cache.getIndexInformation("reduceEqualPartsPerMap", 
               partsPerMap, // reduce number == partsPerMap
               feq, UserGroupInformation.getCurrentUser().getShortUserName());
      fail("Number of reducers equal to partsPerMap did not fail");
    } catch (Exception e) {
      if (!(e instanceof IOException)) {
        throw e;
      }
    }

    try {
      // Number of reducers more than partsPerMap
      cache.getIndexInformation(
      "reduceMorePartsPerMap", 
      partsPerMap + 1, // reduce number > partsPerMap
      feq, UserGroupInformation.getCurrentUser().getShortUserName());
      fail("Number of reducers more than partsPerMap did not fail");
    } catch (Exception e) {
      if (!(e instanceof IOException)) {
        throw e;
      }
    }
  }

  @Test
  public void testRemoveMap() throws Exception {
    // This test case use two thread to call getIndexInformation and 
    // removeMap concurrently, in order to construct race condition.
    // This test case may not repeatable. But on my macbook this test 
    // fails with probability of 100% on code before MAPREDUCE-2541,
    // so it is repeatable in practice.
    fs.delete(p, true);
    conf.setInt(TTConfig.TT_INDEX_CACHE, 10);
    // Make a big file so removeMapThread almost surely runs faster than 
    // getInfoThread 
    final int partsPerMap = 100000;
    final int bytesPerFile = partsPerMap * 24;
    final IndexCache cache = new IndexCache(conf);

    final Path big = new Path(p, "bigIndex");
    final String user = 
      UserGroupInformation.getCurrentUser().getShortUserName();
    writeFile(fs, big, bytesPerFile, partsPerMap);
    
    // run multiple times
    for (int i = 0; i < 20; ++i) {
      Thread getInfoThread = new Thread() {
        @Override
        public void run() {
          try {
            cache.getIndexInformation("bigIndex", partsPerMap, big, user);
          } catch (Exception e) {
            // should not be here
          }
        }
      };
      Thread removeMapThread = new Thread() {
        @Override
        public void run() {
          cache.removeMap("bigIndex");
        }
      };
      if (i%2==0) {
        getInfoThread.start();
        removeMapThread.start();        
      } else {
        removeMapThread.start();        
        getInfoThread.start();
      }
      getInfoThread.join();
      removeMapThread.join();
      assertEquals(true, cache.checkTotalMemoryUsed());
    }      
  }

  @Test
  public void testCreateRace() throws Exception {
    fs.delete(p, true);
    conf.setInt(TTConfig.TT_INDEX_CACHE, 1);
    final int partsPerMap = 1000;
    final int bytesPerFile = partsPerMap * 24;
    final IndexCache cache = new IndexCache(conf);
    
    final Path racy = new Path(p, "racyIndex");
    final String user =  
      UserGroupInformation.getCurrentUser().getShortUserName();
    writeFile(fs, racy, bytesPerFile, partsPerMap);

    // run multiple instances
    Thread[] getInfoThreads = new Thread[50];
    for (int i = 0; i < 50; i++) {
      getInfoThreads[i] = new Thread() {
        @Override
        public void run() {
          try {
            cache.getIndexInformation("racyIndex", partsPerMap, racy, user);
            cache.removeMap("racyIndex");
          } catch (Exception e) {
            // should not be here
          }
        }
      };
    }

    for (int i = 0; i < 50; i++) {
      getInfoThreads[i].start();
    }

    final Thread mainTestThread = Thread.currentThread();

    Thread timeoutThread = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(15000);
          mainTestThread.interrupt();
        } catch (InterruptedException ie) {
          // we are done;
        }
      }
    };

    for (int i = 0; i < 50; i++) {
      try {
        getInfoThreads[i].join();
      } catch (InterruptedException ie) {
        // we haven't finished in time. Potential deadlock/race.
        fail("Unexpectedly long delay during concurrent cache entry creations");
      }
    }
    // stop the timeoutThread. If we get interrupted before stopping, there
    // must be something wrong, although it wasn't a deadlock. No need to
    // catch and swallow.
    timeoutThread.interrupt();
  }

  private static void checkRecord(IndexRecord rec, long fill) {
    assertEquals(fill, rec.startOffset);
    assertEquals(fill, rec.rawLength);
    assertEquals(fill, rec.partLength);
  }

  private static void writeFile(FileSystem fs, Path f, long fill, int parts)
      throws IOException {
    FSDataOutputStream out = fs.create(f, false);
    CheckedOutputStream iout = new CheckedOutputStream(out, new CRC32());
    DataOutputStream dout = new DataOutputStream(iout);
    for (int i = 0; i < parts; ++i) {
      for (int j = 0; j < MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8; ++j) {
        dout.writeLong(fill);
      }
    }
    out.writeLong(iout.getChecksum().getValue());
    dout.close();
  }
}
