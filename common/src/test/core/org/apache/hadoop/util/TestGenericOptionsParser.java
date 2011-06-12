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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

public class TestGenericOptionsParser extends TestCase {
  private static File testDir = 
    new File(System.getProperty("test.build.data", "/tmp"), "generic");
  
  public void testFilesOption() throws Exception {
    Configuration conf = new Configuration();
    File tmpFile = new File(testDir, "tmpfile");
    FileSystem localFs = FileSystem.getLocal(conf);
    Path tmpPath = new Path(tmpFile.toString());
    localFs.create(tmpPath);
    String[] args = new String[2];
    // pass a files option 
    args[0] = "-files";
    args[1] = tmpFile.toString();
    new GenericOptionsParser(conf, args);
    String files = conf.get("tmpfiles");
    assertNotNull("files is null", files);
    assertEquals("files option does not match",
      localFs.makeQualified(tmpPath).toString(), files);
    
    // pass file as uri
    Configuration conf1 = new Configuration();
    URI tmpURI = new URI(tmpFile.toString() + "#link");
    args[0] = "-files";
    args[1] = tmpURI.toString();
    new GenericOptionsParser(conf1, args);
    files = conf1.get("tmpfiles");
    assertNotNull("files is null", files);
    assertEquals("files option does not match", 
      localFs.makeQualified(new Path(tmpURI)).toString(), files);
   
    // pass a file that does not exist.
    // GenericOptionParser should throw exception
    Configuration conf2 = new Configuration();
    args[0] = "-files";
    args[1] = "file:///xyz.txt";
    Throwable th = null;
    try {
      new GenericOptionsParser(conf2, args);
    } catch (Exception e) {
      th = e;
    }
    assertNotNull("throwable is null", th);
    assertTrue("FileNotFoundException is not thrown",
      th instanceof FileNotFoundException);
    files = conf2.get("tmpfiles");
    assertNull("files is not null", files);
    testDir.delete();
  }

}
