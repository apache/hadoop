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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import junit.framework.TestCase;

/** This test makes sure that "DU" does not get to run on each call to getUsed */ 
public class TestDU extends TestCase {
  final static private File DU_DIR = new File(
      System.getProperty("test.build.data","/tmp"), "dutmp");
  final static private File DU_FILE1 = new File(DU_DIR, "tmp1");
  final static private File DU_FILE2 = new File(DU_DIR, "tmp2");
  
  /** create a file of more than 1K size */
  private void createFile( File newFile ) throws IOException {
    newFile.createNewFile();
    RandomAccessFile file = new RandomAccessFile(newFile, "rw");
    file.seek(1024);
    file.writeBytes("du test du test");
    file.getFD().sync();
    file.close();
  }
  
  /** delete a file */
  private void rmFile(File file) {
    if(file.exists()) {
      assertTrue(file.delete());
    }
  }

  /* interval is in a unit of minutes */
  private void testDU(long interval) throws IOException {
    rmFile(DU_FILE1);
    rmFile(DU_FILE2);
    DU_DIR.delete();
    assertTrue(DU_DIR.mkdirs());
    try {
      createFile(DU_FILE1);
      DU du = new DU(DU_DIR, interval*60000);
      long oldUsedSpace = du.getUsed();
      assertTrue(oldUsedSpace>0); // make sure that du is called
      createFile(DU_FILE2);
      if(interval>0) {
        assertEquals( oldUsedSpace, du.getUsed());  // du does not get called
      } else {
        assertTrue( oldUsedSpace < du.getUsed());   // du gets called again
      }
    } finally {
      rmFile(DU_FILE1);
      rmFile(DU_FILE2);
      DU_DIR.delete();
    }
  }

  public void testDU() throws Exception {
    testDU(Long.MIN_VALUE/60000);  // test a negative interval
    testDU(0L);  // test a zero interval
    testDU(10L); // interval equal to 10mins
    testDU(System.currentTimeMillis()/60000+60); // test a very big interval
  }
    
}
