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

package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;

public class TestHarFileSystem {
  @Test
  public void testHarUri() {
    final Configuration conf = new Configuration();
    checkInvalidPath("har://hdfs-/foo.har", conf);
    checkInvalidPath("har://hdfs/foo.har", conf);
    checkInvalidPath("har://-hdfs/foo.har", conf);
    checkInvalidPath("har://-/foo.har", conf);
  }

  static void checkInvalidPath(String s, Configuration conf) {
    System.out.println("\ncheckInvalidPath: " + s);
    final Path p = new Path(s);
    try {
      p.getFileSystem(conf);
      Assert.fail(p + " is an invalid path.");
    } catch (IOException e) {
      System.out.println("GOOD: Got an exception.");
      e.printStackTrace(System.out);
    }
  }

  @Test
  public void testFileChecksum() {
    final Path p = new Path("har://file-localhost/foo.har/file1");
    final HarFileSystem harfs = new HarFileSystem();
    Assert.assertEquals(null, harfs.getFileChecksum(p));
  }

  /**
   * Test how block location offsets and lengths are fixed.
   */
  @Test
  public void testFixBlockLocations() {
    // do some tests where start == 0
    {
      // case 1: range starts before current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 20, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 10);
    }
    {
      // case 2: range starts in current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 20, 15);
      assertEquals(b[0].getOffset(), 0);
      assertEquals(b[0].getLength(), 5);
    }
    {
      // case 3: range starts before current har block and ends in
      // current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 10, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 5);
    }
    {
      // case 4: range starts and ends in current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 6, 12);
      assertEquals(b[0].getOffset(), 0);
      assertEquals(b[0].getLength(), 6);
    }

    // now try a range where start == 3
    {
      // case 5: range starts before current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 20, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 10);
    }
    {
      // case 6: range starts in current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 20, 15);
      assertEquals(b[0].getOffset(), 3);
      assertEquals(b[0].getLength(), 2);
    }
    {
      // case 7: range starts before current har block and ends in
      // current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 7, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 5);
    }
    {
      // case 8: range starts and ends in current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 3, 12);
      assertEquals(b[0].getOffset(), 3);
      assertEquals(b[0].getLength(), 3);
    }

    // test case from JIRA MAPREDUCE-1752
    {
      BlockLocation[] b = { new BlockLocation(null, null, 512, 512),
                            new BlockLocation(null, null, 1024, 512) };
      HarFileSystem.fixBlockLocations(b, 0, 512, 896);
      assertEquals(b[0].getOffset(), 0);
      assertEquals(b[0].getLength(), 128);
      assertEquals(b[1].getOffset(), 128);
      assertEquals(b[1].getLength(), 384);
    }

  }
}
