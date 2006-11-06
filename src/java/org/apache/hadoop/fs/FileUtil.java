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

import java.io.*;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.hadoop.conf.Configuration;

/**
 * A collection of file-processing util methods
 */
public class FileUtil {
  
  /**
   * Delete a directory and all its contents.  If
   * we return false, the directory may be partially-deleted.
   */
  public static boolean fullyDelete(File dir) throws IOException {
    File contents[] = dir.listFiles();
    if (contents != null) {
      for (int i = 0; i < contents.length; i++) {
        if (contents[i].isFile()) {
          if (! contents[i].delete()) {
            return false;
          }
        } else {
          if (! fullyDelete(contents[i])) {
            return false;
          }
        }
      }
    }
    return dir.delete();
  }


  /** Copy files between FileSystems. */
  public static boolean copy(FileSystem srcFS, Path src, 
                             FileSystem dstFS, Path dst, 
                             boolean deleteSource,
                             Configuration conf ) throws IOException {
    dst = checkDest(src.getName(), dstFS, dst);

    if (srcFS.isDirectory(src)) {
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      Path contents[] = srcFS.listPaths(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i], dstFS, new Path(dst, contents[i].getName()),
             deleteSource, conf);
      }
    } else if (srcFS.isFile(src)) {
      InputStream in = srcFS.open(src);
      try {
        copyContent(in, dstFS.create(dst), conf);
      } finally {
        in.close();
      } 
    } else {
      throw new IOException(src.toString() + ": No such file or directory");
    }
    if (deleteSource) {
      return srcFS.delete(src);
    } else {
      return true;
    }
  }
  
  /** Copy all files in a directory to one output file (merge). */
  public static boolean copyMerge(FileSystem srcFS, Path srcDir, 
                             FileSystem dstFS, Path dstFile, 
                             boolean deleteSource,
                             Configuration conf, String addString) throws IOException {
      dstFile = checkDest(srcDir.getName(), dstFS, dstFile);

    if (!srcFS.isDirectory(srcDir))
      return false;
   
    OutputStream out = dstFS.create(dstFile);
    
    try {
      Path contents[] = srcFS.listPaths(srcDir);
      for (int i = 0; i < contents.length; i++) {
        if (srcFS.isFile(contents[i])) {
          InputStream in = srcFS.open(contents[i]);
          try {
            copyContent(in, out, conf, false);
            if(addString!=null)
              out.write(addString.getBytes("UTF-8"));
                
          } finally {
            in.close();
          } 
        }
      }
    } finally {
      out.close();
    }
    

    if (deleteSource) {
      return srcFS.delete(srcDir);
    } else {
      return true;
    }
  }  

  /** Copy local files to a FileSystem. */
  public static boolean copy(File src,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             Configuration conf ) throws IOException {
    dst = checkDest(src.getName(), dstFS, dst);

    if (src.isDirectory()) {
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      File contents[] = src.listFiles();
      for (int i = 0; i < contents.length; i++) {
        copy(contents[i], dstFS, new Path(dst, contents[i].getName()),
             deleteSource, conf);
      }
    } else if (src.isFile()) {
      InputStream in = new FileInputStream(src);
      try {
        copyContent(in, dstFS.create(dst), conf);
      } finally {
        in.close();
      } 
    }
    if (deleteSource) {
      return FileUtil.fullyDelete(src);
    } else {
      return true;
    }
  }

  /** Copy FileSystem files to local files. */
  public static boolean copy(FileSystem srcFS, Path src, 
                             File dst, boolean deleteSource,
                             Configuration conf ) throws IOException {

    dst = checkDest(src.getName(), dst);

    if (srcFS.isDirectory(src)) {
      if (!dst.mkdirs()) {
        return false;
      }
      Path contents[] = srcFS.listPaths(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i], new File(dst, contents[i].getName()),
             deleteSource, conf);
      }
    } else if (srcFS.isFile(src)) {
      InputStream in = srcFS.open(src);
      try {
        copyContent(in, new FileOutputStream(dst), conf);
      } finally {
        in.close();
      } 
    }
    if (deleteSource) {
      return srcFS.delete(src);
    } else {
      return true;
    }
  }

  private static void copyContent(InputStream in, OutputStream out,
          Configuration conf) throws IOException {
    copyContent(in, out, conf, true);
  }

  
  private static void copyContent(InputStream in, OutputStream out,
                                  Configuration conf, boolean close) throws IOException {
    byte buf[] = new byte[conf.getInt("io.file.buffer.size", 4096)];
    try {
      int bytesRead = in.read(buf);
      while (bytesRead >= 0) {
        out.write(buf, 0, bytesRead);
        bytesRead = in.read(buf);
      }
    } finally {
      if(close)
        out.close();
    }
  }

  private static Path checkDest(String srcName, FileSystem dstFS, Path dst)
    throws IOException {
    if (dstFS.exists(dst)) {
      if (!dstFS.isDirectory(dst)) {
        throw new IOException("Target " + dst + " already exists");
      } else {
        dst = new Path(dst, srcName);
        if (dstFS.exists(dst)) {
          throw new IOException("Target " + dst + " already exists");
        }
      }
    }
    return dst;
  }

  private static File checkDest(String srcName, File dst)
    throws IOException {
    if (dst.exists()) {
      if (!dst.isDirectory()) {
        throw new IOException("Target " + dst + " already exists");
      } else {
        dst = new File(dst, srcName);
        if (dst.exists()) {
          throw new IOException("Target " + dst + " already exists");
        }
      }
    }
    return dst;
  }
  
  /**
   * Takes an input dir and returns the du on that local directory. Very basic
   * implementation.
   * 
   * @param dir
   *          The input dir to get the disk space of this local dir
   * @return The total disk space of the input local directory
   */
  public static long getDU(File dir) {
    long size = 0;
    if (!dir.exists())
      return 0;
    if (!dir.isDirectory()) {
      return dir.length();
    } else {
      size = dir.length();
      File[] allFiles = dir.listFiles();
      for (int i = 0; i < allFiles.length; i++) {
        size = size + getDU(allFiles[i]);
      }
      return size;
    }
  }
    
	/**
   * Given a File input it will unzip the file in a the unzip directory
   * passed as the second parameter
   * @param inFile The zip file as input
   * @param unzipDir The unzip directory where to unzip the zip file.
   * @throws IOException
   */
  public static void unZip(File inFile, File unzipDir) throws IOException {
    Enumeration entries;
    ZipFile zipFile = new ZipFile(inFile);
    ;
    try {
      entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = (ZipEntry) entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = zipFile.getInputStream(entry);
          try {
            File file = new File(unzipDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {           
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create " + 
                                      file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      zipFile.close();
    }
  }
  
  /**
   * Create a soft link between a src and destination
   * only on a local disk. HDFS does not support this
   * @param target the target for symlink 
   * @param linkname the symlink
   * @return value returned by the command
   */
  public static int symLink(String target, String linkname) throws IOException{
   String cmd = "ln -s " + target + " " + linkname;
   Process p = Runtime.getRuntime().exec( cmd, null );
   int returnVal = -1;
   try{
     returnVal = p.waitFor();
   } catch(InterruptedException e){
     //do nothing as of yet
   }
   return returnVal;
 }
}
