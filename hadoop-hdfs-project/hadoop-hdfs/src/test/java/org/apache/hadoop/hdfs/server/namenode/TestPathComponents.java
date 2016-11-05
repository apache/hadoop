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

import static org.junit.Assert.assertEquals;
import java.util.Arrays;

import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Test;

/**
 * 
 */
public class TestPathComponents {

  @Test
  public void testBytes2ByteArrayFQ() throws Exception {
    testString("/", new String[]{null});
    testString("//", new String[]{null});
    testString("/file", new String[]{"", "file"});
    testString("/dir/", new String[]{"", "dir"});
    testString("//file", new String[]{"", "file"});
    testString("/dir//file", new String[]{"", "dir", "file"});
    testString("//dir/dir1//", new String[]{"", "dir", "dir1"});
    testString("//dir//dir1//", new String[]{"", "dir", "dir1"});
    testString("//dir//dir1//file", new String[]{"", "dir", "dir1", "file"});
  }

  @Test
  public void testBytes2ByteArrayRelative() throws Exception {
    testString("file", new String[]{"file"});
    testString("dir/", new String[]{"dir"});
    testString("dir//", new String[]{"dir"});
    testString("dir//file", new String[]{"dir", "file"});
    testString("dir/dir1//", new String[]{"dir", "dir1"});
    testString("dir//dir1//", new String[]{"dir", "dir1"});
    testString("dir//dir1//file", new String[]{"dir", "dir1", "file"});
  }

  @Test
  public void testByteArray2PathStringRoot() {
    byte[][] components = DFSUtil.getPathComponents("/");
    assertEquals("", DFSUtil.byteArray2PathString(components, 0, 0));
    assertEquals("/", DFSUtil.byteArray2PathString(components, 0, 1));
  }

  @Test
  public void testByteArray2PathStringFQ() {
    byte[][] components = DFSUtil.getPathComponents("/1/2/3");
    assertEquals("/1/2/3", DFSUtil.byteArray2PathString(components));

    assertEquals("", DFSUtil.byteArray2PathString(components, 0, 0));
    assertEquals("/", DFSUtil.byteArray2PathString(components, 0, 1));
    assertEquals("/1", DFSUtil.byteArray2PathString(components, 0, 2));
    assertEquals("/1/2", DFSUtil.byteArray2PathString(components, 0, 3));
    assertEquals("/1/2/3", DFSUtil.byteArray2PathString(components, 0, 4));

    assertEquals("", DFSUtil.byteArray2PathString(components, 1, 0));
    assertEquals("1", DFSUtil.byteArray2PathString(components, 1, 1));
    assertEquals("1/2", DFSUtil.byteArray2PathString(components, 1, 2));
    assertEquals("1/2/3", DFSUtil.byteArray2PathString(components, 1, 3));
  }

  @Test
  public void testByteArray2PathStringRelative() {
    byte[][] components = DFSUtil.getPathComponents("1/2/3");
    assertEquals("1/2/3", DFSUtil.byteArray2PathString(components));

    assertEquals("", DFSUtil.byteArray2PathString(components, 0, 0));
    assertEquals("1", DFSUtil.byteArray2PathString(components, 0, 1));
    assertEquals("1/2", DFSUtil.byteArray2PathString(components, 0, 2));
    assertEquals("1/2/3", DFSUtil.byteArray2PathString(components, 0, 3));

    assertEquals("", DFSUtil.byteArray2PathString(components, 1, 0));
    assertEquals("2", DFSUtil.byteArray2PathString(components, 1, 1));
    assertEquals("2/3", DFSUtil.byteArray2PathString(components, 1, 2));
  }

  public void testString(String path, String[] expected) throws Exception {
    byte[][] components = DFSUtil.getPathComponents(path);
    String[] actual = new String[components.length];
    for (int i=0; i < components.length; i++) {
      if (components[i] != null) {
        actual[i] = DFSUtil.bytes2String(components[i]);
      }
    }
    assertEquals(Arrays.asList(expected), Arrays.asList(actual));

    // test the reconstituted path
    path = path.replaceAll("/+", "/");
    if (path.length() > 1) {
      path = path.replaceAll("/$", "");
    }
    assertEquals(path, DFSUtil.byteArray2PathString(components));
  }
}
