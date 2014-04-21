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

package org.apache.hadoop.io.compress;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class TestCompressorStream extends CompressorStream{
  
  private static FileOutputStream fop = null;
  private static File file = null;
  
  static {
    try {
      file = new File("tmp.txt");
      fop = new FileOutputStream(file);
      if (!file.exists()) {
        file.createNewFile();
      }
    } catch (IOException e) {
      System.out.println("Error while creating a new file " + e.getMessage());
    }
  }
  
  public TestCompressorStream() {
    super(fop);
  }
  
  /**
   * Overriding {@link CompressorStream#finish()} method in order 
   * to reproduce test case
   */
  public void finish() throws IOException {
    throw new IOException();
  }

  /**
   * In {@link CompressorStream#close()}, if 
   * {@link CompressorStream#finish()} throws an IOEXception, outputStream 
   * object was not getting closed.
   */
  @Test
  public void  testClose() {
    TestCompressorStream testCompressorStream = new TestCompressorStream();
    try {
      testCompressorStream.close(); 
    }
    catch(IOException e) {
      System.out.println("Expected IOException");
    }
    Assert.assertTrue("closed shoud be true", 
        ((CompressorStream)testCompressorStream).closed);
    //cleanup after test case
    file.delete();
  }
}
