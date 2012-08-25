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

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;

import junit.framework.TestCase;

public class TestAvroFSInput extends TestCase {

  private static final String INPUT_DIR = "AvroFSInput";

  private Path getInputPath() {
    String dataDir = System.getProperty("test.build.data");
    if (null == dataDir) {
      return new Path(INPUT_DIR);
    } else {
      return new Path(new Path(dataDir), INPUT_DIR);
    }
  }


  public void testAFSInput() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    Path dir = getInputPath();

    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }

    Path filePath = new Path(dir, "foo");
    if (fs.exists(filePath)) {
      fs.delete(filePath, false);
    }

    FSDataOutputStream ostream = fs.create(filePath);
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(ostream));
    w.write("0123456789");
    w.close();

    // Create the stream
    FileContext fc = FileContext.getFileContext(conf);
    AvroFSInput avroFSIn = new AvroFSInput(fc, filePath);

    assertEquals(10, avroFSIn.length());

    // Check initial position
    byte [] buf = new byte[1];
    assertEquals(0, avroFSIn.tell());

    // Check a read from that position.
    avroFSIn.read(buf, 0, 1);
    assertEquals(1, avroFSIn.tell());
    assertEquals('0', (char)buf[0]);

    // Check a seek + read
    avroFSIn.seek(4);
    assertEquals(4, avroFSIn.tell());
    avroFSIn.read(buf, 0, 1);
    assertEquals('4', (char)buf[0]);
    assertEquals(5, avroFSIn.tell());

    avroFSIn.close();
  }
}

