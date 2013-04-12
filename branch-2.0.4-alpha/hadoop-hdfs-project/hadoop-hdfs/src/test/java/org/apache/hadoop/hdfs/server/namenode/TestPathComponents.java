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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Test;

import com.google.common.base.Charsets;


/**
 * 
 */
public class TestPathComponents {

  @Test
  public void testBytes2ByteArray() throws Exception {
    testString("/");
    testString("/file");
    testString("/directory/");
    testString("//");
    testString("/dir//file");
    testString("/dir/dir1//");
  }

  public void testString(String str) throws Exception {
    String pathString = str;
    byte[][] oldPathComponents = INode.getPathComponents(pathString);
    byte[][] newPathComponents = 
                DFSUtil.bytes2byteArray(pathString.getBytes(Charsets.UTF_8),
                                        (byte) Path.SEPARATOR_CHAR);
    if (oldPathComponents[0] == null) {
      assertTrue(oldPathComponents[0] == newPathComponents[0]);
    } else {
      assertTrue("Path components do not match for " + pathString,
                  Arrays.deepEquals(oldPathComponents, newPathComponents));
    }
  }
}
