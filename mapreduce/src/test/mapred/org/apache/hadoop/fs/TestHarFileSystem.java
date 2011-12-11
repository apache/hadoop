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
}
