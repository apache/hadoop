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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestGenericOptionsParser extends TestCase {
  File testDir;
  Configuration conf;
  FileSystem localFs;
      
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
    localFs = FileSystem.getLocal(conf);
    testDir = new File(System.getProperty("test.build.data", "/tmp"), "generic");
    if(testDir.exists())
      localFs.delete(new Path(testDir.toString()), true);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    if(testDir.exists()) {
      localFs.delete(new Path(testDir.toString()), true);
    }
  }

  /**
   * testing -fileCache option
   * @throws IOException
   */
  public void testTokenCacheOption() throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    
    File tmpFile = new File(testDir, "tokenCacheFile");
    if(tmpFile.exists()) {
      tmpFile.delete();
    }
    String[] args = new String[2];
    // pass a files option 
    args[0] = "-tokenCacheFile";
    args[1] = tmpFile.toString();
    
    // test non existing file
    Throwable th = null;
    try {
      new GenericOptionsParser(conf, args);
    } catch (Exception e) {
      th = e;
    }
    assertNotNull(th);
    
    // create file
    Path tmpPath = new Path(tmpFile.toString());
    localFs.create(tmpPath);
    new GenericOptionsParser(conf, args);
    String fileName = conf.get("tokenCacheFile");
    assertNotNull("files is null", fileName);
    assertEquals("files option does not match",
      localFs.makeQualified(tmpPath).toString(), fileName);
    
    localFs.delete(new Path(testDir.getAbsolutePath()), true);
  }
}
