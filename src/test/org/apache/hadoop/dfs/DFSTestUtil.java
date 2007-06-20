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

package org.apache.hadoop.dfs;

import java.io.IOException;
import java.net.URI;
import java.util.Random;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 */
public class DFSTestUtil extends TestCase {
  
  private static Random gen = new Random();
  private static String[] dirNames = {
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
  };
  private static Configuration conf = new Configuration();
  
  private int maxLevels;// = 3;
  private int maxSize;// = 8*1024;
  private int nFiles;
  private MyFile[] files;
  
  /** Creates a new instance of DFSTestUtil
   *
   * @param testName Name of the test from where this utility is used
   * @param nFiles Number of files to be created
   * @param maxLevels Maximum number of directory levels
   * @param maxSize Maximum size for file
   */
  public DFSTestUtil(String testName, int nFiles, int maxLevels, int maxSize) {
    this.nFiles = nFiles;
    this.maxLevels = maxLevels;
    this.maxSize = maxSize;
  }
  
  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  private class MyFile {
    
    private String name = "";
    private int size;
    private long seed;
    
    MyFile() {
      int nLevels = gen.nextInt(maxLevels);
      if (nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        name = sb.toString();
      }
      long fidx = -1;
      while (fidx < 0) { fidx = gen.nextLong(); }
      name = name + Long.toString(fidx);
      size = gen.nextInt(maxSize);
      seed = gen.nextLong();
    }
    
    String getName() { return name; }
    int getSize() { return size; }
    long getSeed() { return seed; }
  }
  
  /** create nFiles with random names and directory hierarchies
   * with random (but reproducible) data in them.
   */
  void createFiles(FileSystem fs, String topdir) throws IOException {
    files = new MyFile[nFiles];
    
    for (int idx = 0; idx < nFiles; idx++) {
      files[idx] = new MyFile();
    }
    
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < nFiles; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      if (!fs.mkdirs(fPath.getParent())) {
        throw new IOException("Mkdirs failed to create " + 
                              fPath.getParent().toString());
      }
      FSDataOutputStream out = fs.create(fPath);
      byte[] toWrite = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toWrite);
      out.write(toWrite);
      out.close();
      toWrite = null;
    }
  }
  
  /** check if the files have been copied correctly. */
  boolean checkFiles(FileSystem fs, String topdir) throws IOException {
    
    //Configuration conf = new Configuration();
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < nFiles; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      FSDataInputStream in = fs.open(fPath);
      byte[] toRead = new byte[files[idx].getSize()];
      byte[] toCompare = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toCompare);
      assertEquals("Cannnot read file.", toRead.length, in.read(toRead));
      in.close();
      for (int i = 0; i < toRead.length; i++) {
        if (toRead[i] != toCompare[i]) {
          return false;
        }
      }
      toRead = null;
      toCompare = null;
    }
    
    return true;
  }
  
  /** delete directory and everything underneath it.*/
  void cleanup(FileSystem fs, String topdir) throws IOException {
    Path root = new Path(topdir);
    fs.delete(root);
    files = null;
  }
}
