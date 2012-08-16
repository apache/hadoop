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

package org.apache.hadoop.util;

import java.io.ByteArrayInputStream;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import junit.framework.Assert;

public class TestLineReader {

  @Test
  public void testCustomDelimiter() throws Exception {
    String data = "record Bangalorrecord recorrecordrecord Kerala";
    String delimiter = "record";
    LineReader reader = new LineReader(
        new ByteArrayInputStream(data.getBytes()),
        delimiter.getBytes());
    Text line = new Text();
    reader.readLine(line);
    Assert.assertEquals("", line.toString());
    reader.readLine(line);
    Assert.assertEquals(" Bangalor", line.toString());
    reader.readLine(line);
    Assert.assertEquals(" recor", line.toString());
    reader.readLine(line);
    Assert.assertEquals("", line.toString());
    reader.readLine(line);
    Assert.assertEquals(" Kerala", line.toString());
  }
}
