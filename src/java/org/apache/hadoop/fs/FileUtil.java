/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.conf.Configuration;

/**
 * A collection of file-processing util methods
 */
public class FileUtil {
  
  /** @deprecated Call {@link #fullyDelete(File)}. */
  public static boolean fullyDelete(File dir, Configuration conf)
    throws IOException {
    return fullyDelete(dir);
  }

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
      dstFS.mkdirs(dst);
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
    }
    if (deleteSource) {
      return srcFS.delete(src);
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
      dstFS.mkdirs(dst);
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
      dst.mkdirs();
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
    byte buf[] = new byte[conf.getInt("io.file.buffer.size", 4096)];
    try {
      int bytesRead = in.read(buf);
      while (bytesRead >= 0) {
        out.write(buf, 0, bytesRead);
        bytesRead = in.read(buf);
      }
    } finally {
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

}
