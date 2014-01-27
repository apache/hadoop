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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestFileInputStreamCache {
  static final Log LOG = LogFactory.getLog(TestFileInputStreamCache.class);

  @Test
  public void testCreateAndDestroy() throws Exception {
    FileInputStreamCache cache = new FileInputStreamCache(10, 1000);
    cache.close();
  }
  
  private static class TestFileDescriptorPair {
    TemporarySocketDirectory dir = new TemporarySocketDirectory();
    FileInputStream fis[];

    public TestFileDescriptorPair() throws IOException {
      fis = new FileInputStream[2];
      for (int i = 0; i < 2; i++) {
        String name = dir.getDir() + "/file" + i;
        FileOutputStream fos = new FileOutputStream(name);
        fos.write(1);
        fos.close();
        fis[i] = new FileInputStream(name);
      }
    }

    public FileInputStream[] getFileInputStreams() {
      return fis;
    }

    public void close() throws IOException {
      IOUtils.cleanup(LOG, fis);
      dir.close();
    }

    public boolean compareWith(FileInputStream other[]) {
      if ((other == null) || (fis == null)) {
        return other == fis;
      }
      if (fis.length != other.length) return false;
      for (int i = 0; i < fis.length; i++) {
        if (fis[i] != other[i]) return false;
      }
      return true;
    }
  }

  @Test
  public void testAddAndRetrieve() throws Exception {
    FileInputStreamCache cache = new FileInputStreamCache(1, 1000000);
    DatanodeID dnId = new DatanodeID("127.0.0.1", "localhost", 
        "xyzzy", 8080, 9090, 7070, 6060);
    ExtendedBlock block = new ExtendedBlock("poolid", 123);
    TestFileDescriptorPair pair = new TestFileDescriptorPair();
    cache.put(dnId, block, pair.getFileInputStreams());
    FileInputStream fis[] = cache.get(dnId, block);
    Assert.assertTrue(pair.compareWith(fis));
    pair.close();
    cache.close();
  }

  @Test
  public void testExpiry() throws Exception {
    FileInputStreamCache cache = new FileInputStreamCache(1, 10);
    DatanodeID dnId = new DatanodeID("127.0.0.1", "localhost", 
        "xyzzy", 8080, 9090, 7070, 6060);
    ExtendedBlock block = new ExtendedBlock("poolid", 123);
    TestFileDescriptorPair pair = new TestFileDescriptorPair();
    cache.put(dnId, block, pair.getFileInputStreams());
    Thread.sleep(cache.getExpiryTimeMs() * 100);
    FileInputStream fis[] = cache.get(dnId, block);
    Assert.assertNull(fis);
    pair.close();
    cache.close();
  }

  @Test
  public void testEviction() throws Exception {
    FileInputStreamCache cache = new FileInputStreamCache(1, 10000000);
    DatanodeID dnId = new DatanodeID("127.0.0.1", "localhost", 
        "xyzzy", 8080, 9090, 7070, 6060);
    ExtendedBlock block = new ExtendedBlock("poolid", 123);
    TestFileDescriptorPair pair = new TestFileDescriptorPair();
    cache.put(dnId, block, pair.getFileInputStreams());
    DatanodeID dnId2 = new DatanodeID("127.0.0.1", "localhost", 
        "xyzzy", 8081, 9091, 7071, 6061);
    TestFileDescriptorPair pair2 = new TestFileDescriptorPair();
    cache.put(dnId2, block, pair2.getFileInputStreams());
    FileInputStream fis[] = cache.get(dnId, block);
    Assert.assertNull(fis);
    FileInputStream fis2[] = cache.get(dnId2, block);
    Assert.assertTrue(pair2.compareWith(fis2));
    pair.close();
    cache.close();
  }
}
