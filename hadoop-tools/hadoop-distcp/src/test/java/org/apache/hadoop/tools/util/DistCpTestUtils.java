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

package org.apache.hadoop.tools.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;

/**
 * Utility class for DistCpTests
 */
public class DistCpTestUtils {

   /**
    * Asserts the XAttrs returned by getXAttrs for a specific path match an
    * expected set of XAttrs.
    *
    * @param path String path to check
    * @param fs FileSystem to use for the path
    * @param expectedXAttrs XAttr[] expected xAttrs
    * @throws Exception if there is any error
    */
  public static void assertXAttrs(Path path, FileSystem fs,
      Map<String, byte[]> expectedXAttrs)
      throws Exception {
    Map<String, byte[]> xAttrs = fs.getXAttrs(path);
    assertEquals(path.toString(), expectedXAttrs.size(), xAttrs.size());
    Iterator<Entry<String, byte[]>> i = expectedXAttrs.entrySet().iterator();
    while (i.hasNext()) {
      Entry<String, byte[]> e = i.next();
      String name = e.getKey();
      byte[] value = e.getValue();
      if (value == null) {
        assertTrue(xAttrs.containsKey(name) && xAttrs.get(name) == null);
      } else {
        assertArrayEquals(value, xAttrs.get(name));
      }
    }
  }

  /**
   * Runs distcp from src to dst, preserving XAttrs. Asserts the
   * expected exit code.
   *
   * @param exitCode expected exit code
   * @param src distcp src path
   * @param dst distcp destination
   * @param options distcp command line options
   * @param conf Configuration to use
   * @throws Exception if there is any error
   */
  public static void assertRunDistCp(int exitCode, String src, String dst,
      String options, Configuration conf)
      throws Exception {
    DistCp distCp = new DistCp(conf, null);
    String[] optsArr = options == null ?
        new String[] { src, dst } :
        new String[] { options, src, dst };
    assertEquals(exitCode,
        ToolRunner.run(conf, distCp, optsArr));
  }
}
