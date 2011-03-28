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

package org.apache.hadoop.fs.shell;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Encapsulates a Path (path), its FileStatus (stat), and its FileSystem (fs).
 * The stat field will be null if the path does not exist.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class PathData {
  public final Path path;
  public FileStatus stat;
  public final FileSystem fs;
  public boolean exists;

  /**
   * Creates an object to wrap the given parameters as fields.
   * @param theFs the FileSystem
   * @param thePath a Path
   * @param theStat the FileStatus (may be null if the path doesn't exist)
   */
  public PathData(FileSystem theFs, Path thePath, FileStatus theStat) {
    path = thePath;
    stat = theStat;
    fs = theFs;
    exists = (stat != null);
  }

  /**
   * Convenience ctor that looks up the file status for a path.  If the path
   * doesn't exist, then the status will be null
   * @param fs the FileSystem for the path
   * @param path the pathname to lookup 
   * @throws IOException 
   */
  public PathData(FileSystem fs, Path path) throws IOException {
    this(fs, path, getStat(fs, path));
  }

  // need a static method for the ctor above
  private static FileStatus getStat(FileSystem fs, Path path)
  throws IOException {  
    FileStatus status = null;
    try {
      status = fs.getFileStatus(path);
    } catch (FileNotFoundException e) {} // ignore FNF
    return status;
  }

  /**
   * Convenience ctor that extracts the path from the given file status
   * @param fs the FileSystem for the FileStatus
   * @param stat the FileStatus 
   */
  public PathData(FileSystem fs, FileStatus stat) {
    this(fs, stat.getPath(), stat);
  }
  
  /**
   * Updates the paths's file status
   * @return the updated FileStatus
   * @throws IOException if anything goes wrong...
   */
  public FileStatus refreshStatus() throws IOException {
    stat = fs.getFileStatus(path);
    return stat;
  }
  
  /**
   * Returns a list of PathData objects of the items contained in the given
   * directory.
   * @return list of PathData objects for its children
   * @throws IOException if anything else goes wrong...
   */
  public PathData[] getDirectoryContents() throws IOException {
    if (!stat.isDirectory()) {
      throw new IOException(path + ": Not a directory");
    }

    FileStatus[] stats = fs.listStatus(path);
    PathData[] items = new PathData[stats.length];
    for (int i=0; i < stats.length; i++) {
      items[i] = new PathData(fs, stats[i]);
    }
    return items;
  }

  /**
   * Returns the printable version of the path that is just the
   * filesystem path instead of the full uri
   * @return String of the path
   */
  public String toString() {
    return path.toUri().getPath();
  }
}
