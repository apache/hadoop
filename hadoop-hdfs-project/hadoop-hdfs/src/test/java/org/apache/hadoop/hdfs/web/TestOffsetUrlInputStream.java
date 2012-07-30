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
package org.apache.hadoop.hdfs.web;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;

import org.junit.Test;

public class TestOffsetUrlInputStream {
  @Test
  public void testRemoveOffset() throws IOException {
    { //no offset
      String s = "http://test/Abc?Length=99";
      assertEquals(s, WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //no parameters
      String s = "http://test/Abc";
      assertEquals(s, WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //offset as first parameter
      String s = "http://test/Abc?offset=10&Length=99";
      assertEquals("http://test/Abc?Length=99",
          WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //offset as second parameter
      String s = "http://test/Abc?op=read&OFFset=10&Length=99";
      assertEquals("http://test/Abc?op=read&Length=99",
          WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //offset as last parameter
      String s = "http://test/Abc?Length=99&offset=10";
      assertEquals("http://test/Abc?Length=99",
          WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //offset as the only parameter
      String s = "http://test/Abc?offset=10";
      assertEquals("http://test/Abc",
          WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }
  }
}
