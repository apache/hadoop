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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

import org.junit.Test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class TestFileStatus {

  private static final Log LOG =
    LogFactory.getLog(TestFileStatus.class);

  @Test
  public void testFileStatusWritable() throws Exception {
    FileStatus[] tests = {
        new FileStatus(1,false,5,3,4,5,null,"","",new Path("/a/b")),
        new FileStatus(0,false,1,2,3,new Path("/")),
        new FileStatus(1,false,5,3,4,5,null,"","",new Path("/a/b"))
      };

    LOG.info("Writing FileStatuses to a ByteArrayOutputStream");
    // Writing input list to ByteArrayOutputStream
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    for (FileStatus fs : tests) {
      fs.write(out);
    }

    LOG.info("Creating ByteArrayInputStream object");
    DataInput in =
      new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));


    LOG.info("Testing if read objects are equal to written ones");
    FileStatus dest = new FileStatus();
    int iterator = 0;
    for (FileStatus fs : tests) {
      dest.readFields(in);
      assertEquals("Different FileStatuses in iteration " + iterator,
          dest, fs);
      iterator++;
    }
  }
}
