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
package org.apache.hadoop.yarn.server.nodemanager.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.junit.Test;

public class TestProcessIdFileReader {

  
  @Test (timeout = 30000)
  public void testNullPath() {
    String pid = null;
    try {
      pid = ProcessIdFileReader.getProcessId(null);
      fail("Expected an error to be thrown for null path");
    } catch (Exception e) {
      // expected
    }
    assert(pid == null);
  }
  
  @Test (timeout = 30000)
  public void testSimpleGet() throws IOException {
    String rootDir = new File(System.getProperty(
        "test.build.data", "/tmp")).getAbsolutePath();
    File testFile = null;
    String expectedProcessId = Shell.WINDOWS ?
      "container_1353742680940_0002_01_000001" :
      "56789";
    
    try {
      testFile = new File(rootDir, "temp.txt");
      PrintWriter fileWriter = new PrintWriter(testFile);
      fileWriter.println(expectedProcessId);
      fileWriter.close();      
      String processId = null; 
                  
      processId = ProcessIdFileReader.getProcessId(
          new Path(rootDir + Path.SEPARATOR + "temp.txt"));
      Assert.assertEquals(expectedProcessId, processId);      
      
    } finally {
      if (testFile != null
          && testFile.exists()) {
        testFile.delete();
      }
    }
  }

    
  @Test (timeout = 30000)
  public void testComplexGet() throws IOException {
    String rootDir = new File(System.getProperty(
        "test.build.data", "/tmp")).getAbsolutePath();
    File testFile = null;
    String processIdInFile = Shell.WINDOWS ?
      " container_1353742680940_0002_01_000001 " :
      " 23 ";
    String expectedProcessId = processIdInFile.trim();
    try {
      testFile = new File(rootDir, "temp.txt");
      PrintWriter fileWriter = new PrintWriter(testFile);
      fileWriter.println("   ");
      fileWriter.println("");
      fileWriter.println("abc");
      fileWriter.println("-123");
      fileWriter.println("-123 ");
      fileWriter.println(processIdInFile);
      fileWriter.println("6236");
      fileWriter.close();      
      String processId = null; 
                  
      processId = ProcessIdFileReader.getProcessId(
          new Path(rootDir + Path.SEPARATOR + "temp.txt"));
      Assert.assertEquals(expectedProcessId, processId);
      
    } finally {
      if (testFile != null
          && testFile.exists()) {
        testFile.delete();
      }
    }
  }
}
