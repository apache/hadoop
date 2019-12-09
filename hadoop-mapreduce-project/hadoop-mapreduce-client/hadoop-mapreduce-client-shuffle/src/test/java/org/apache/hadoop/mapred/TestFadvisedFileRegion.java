/*
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
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.WritableByteChannel;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFadvisedFileRegion {
  private final int FILE_SIZE = 16*1024*1024;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFadvisedFileRegion.class);
  
  @Test(timeout = 100000)
  public void testCustomShuffleTransfer() throws IOException {
    File absLogDir = new File("target", 
        TestFadvisedFileRegion.class.getSimpleName() + 
        "LocDir").getAbsoluteFile();
    
    String testDirPath =
        StringUtils.join(Path.SEPARATOR,
            new String[] { absLogDir.getAbsolutePath(),
                "testCustomShuffleTransfer"});
    File testDir = new File(testDirPath);
    testDir.mkdirs();
    
    System.out.println(testDir.getAbsolutePath());
    
    File inFile = new File(testDir, "fileIn.out");
    File outFile = new File(testDir, "fileOut.out");
    
    
    //Initialize input file
    byte [] initBuff = new byte[FILE_SIZE];
    Random rand = new Random();
    rand.nextBytes(initBuff);
    
    FileOutputStream out = new FileOutputStream(inFile);
    try{
      out.write(initBuff);  
    } finally {
      IOUtils.cleanupWithLogger(LOG, out);
    }
    
    
    //define position and count to read from a file region.
    int position = 2*1024*1024;
    int count = 4*1024*1024 - 1;
    
    RandomAccessFile inputFile = null;
    RandomAccessFile targetFile = null;
    WritableByteChannel target = null;
    FadvisedFileRegion fileRegion = null;
    
    try {
      inputFile = new RandomAccessFile(inFile.getAbsolutePath(), "r");
      targetFile = new RandomAccessFile(outFile.getAbsolutePath(), "rw");
      target = targetFile.getChannel();
      
      Assert.assertEquals(FILE_SIZE, inputFile.length());
      
      //create FadvisedFileRegion
      fileRegion = new FadvisedFileRegion(
          inputFile, position, count, false, 0, null, null, 1024, false);
      
      //test corner cases
      customShuffleTransferCornerCases(fileRegion, target, count);
            
      long pos = 0;
      long size;
      while((size = fileRegion.customShuffleTransfer(target, pos)) > 0) {
        pos += size; 
      }
    
      //assert size
      Assert.assertEquals(count, (int)pos);
      Assert.assertEquals(count, targetFile.length());
    } finally {
      if (fileRegion != null) {
        fileRegion.releaseExternalResources();
      }
      IOUtils.cleanupWithLogger(LOG, target);
      IOUtils.cleanupWithLogger(LOG, targetFile);
      IOUtils.cleanupWithLogger(LOG, inputFile);
    }
    
    //Read the target file and verify that copy is done correctly
    byte [] buff = new byte[FILE_SIZE];
    FileInputStream in = new FileInputStream(outFile);
    try {
      int total = in.read(buff, 0, count);
    
      Assert.assertEquals(count, total);
    
      for(int i = 0; i < count; i++) {
        Assert.assertEquals(initBuff[position+i], buff[i]);
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    
    //delete files and folders
    inFile.delete();
    outFile.delete();
    testDir.delete();
    absLogDir.delete();
  }
  
  private static void customShuffleTransferCornerCases(
      FadvisedFileRegion fileRegion, WritableByteChannel target, int count) {
    try {
      fileRegion.customShuffleTransfer(target, -1);
      Assert.fail("Expected a IllegalArgumentException");
    } catch (IllegalArgumentException ie) {
      LOG.info("Expected - illegal argument is passed.");
    } catch (Exception e) {
      Assert.fail("Expected a IllegalArgumentException");
    }

    //test corner cases
    try {
      fileRegion.customShuffleTransfer(target, count + 1);
      Assert.fail("Expected a IllegalArgumentException");
    } catch (IllegalArgumentException ie) {
      LOG.info("Expected - illegal argument is passed.");
    } catch (Exception e) {
      Assert.fail("Expected a IllegalArgumentException");
    }
  }
}
