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

package org.apache.hadoop.fs.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class TestS3InMemoryFileSystem extends TestCase {

  private static final String TEST_PATH = "s3://test/data.txt";
  
  private static final String TEST_DATA = "Sample data for testing.";
  
  private S3InMemoryFileSystem fs;
  
  @Override
  public void setUp() throws IOException {
    fs = new S3InMemoryFileSystem();
    fs.initialize(URI.create("s3://test/"), new Configuration());
  }
 
  public void testBasicReadWriteIO() throws IOException {
    FSDataOutputStream writeStream = fs.create(new Path(TEST_PATH));
    writeStream.write(TEST_DATA.getBytes());
    writeStream.flush();
    writeStream.close();
    
    FSDataInputStream readStream = fs.open(new Path(TEST_PATH));
    BufferedReader br = new BufferedReader(new InputStreamReader(readStream));
    String line = "";
    StringBuffer stringBuffer = new StringBuffer();
    while ((line = br.readLine()) != null) {
        stringBuffer.append(line);
    }
    br.close();
    
    assert(TEST_DATA.equals(stringBuffer.toString()));
  }
  
  @Override
  public void tearDown() throws IOException {
    fs.close();  
  }
}
