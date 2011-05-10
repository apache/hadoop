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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathExceptions.PathIsNotDirectoryException;

/**
 * Encapsulates a Path (path), its FileStatus (stat), and its FileSystem (fs).
 * The stat field will be null if the path does not exist.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class PathData {
  protected String string = null;
  public final Path path;
  public FileStatus stat;
  public final FileSystem fs;
  public boolean exists;

  /**
   * Creates an object to wrap the given parameters as fields.  The string
   * used to create the path will be recorded since the Path object does not
   * return exactly the same string used to initialize it
   * @param pathString a string for a path
   * @param conf the configuration file
   * @throws IOException if anything goes wrong...
   */
  public PathData(String pathString, Configuration conf) throws IOException {
    this.string = pathString;
    this.path = new Path(pathString);
    this.fs = path.getFileSystem(conf);
    setStat(getStat(fs, path));
  }
  
  /**
   * Creates an object to wrap the given parameters as fields. 
   * @param fs the FileSystem
   * @param path a Path
   * @param stat the FileStatus (may be null if the path doesn't exist)
   */
  public PathData(FileSystem fs, Path path, FileStatus stat) {
    this.string = path.toString();
    this.path = path;
    this.fs = fs;
    setStat(stat);
  }

  /**
   * Convenience ctor that looks up the file status for a path.  If the path
   * doesn't exist, then the status will be null
   * @param fs the FileSystem for the path
   * @param path the pathname to lookup 
   * @throws IOException if anything goes wrong
   */
  public PathData(FileSystem fs, Path path) throws IOException {
    this(fs, path, getStat(fs, path));
  }

  /**
   * Creates an object to wrap the given parameters as fields.  The string
   * used to create the path will be recorded since the Path object does not
   * return exactly the same string used to initialize it.  If the FileStatus
   * is not null, then its Path will be used to initialized the path, else
   * the string of the path will be used.
   * @param fs the FileSystem
   * @param pathString a String of the path
   * @param stat the FileStatus (may be null if the path doesn't exist)
   */
  public PathData(FileSystem fs, String pathString, FileStatus stat) {
    this.string = pathString;
    this.path = (stat != null) ? stat.getPath() : new Path(pathString);
    this.fs = fs;
    setStat(stat);
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
  
  private void setStat(FileStatus theStat) {
    stat = theStat;
    exists = (stat != null);
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
    setStat(fs.getFileStatus(path));
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
      throw new PathIsNotDirectoryException(string);
    }

    FileStatus[] stats = fs.listStatus(path);
    PathData[] items = new PathData[stats.length];
    for (int i=0; i < stats.length; i++) {
      // preserve relative paths
      String basename = stats[i].getPath().getName();
      String parent = string;
      if (!parent.endsWith(Path.SEPARATOR)) parent += Path.SEPARATOR;
      items[i] = new PathData(fs, parent + basename, stats[i]);
    }
    return items;
  }

  /**
   * Expand the given path as a glob pattern.  Non-existent paths do not
   * throw an exception because creation commands like touch and mkdir need
   * to create them.  The "stat" field will be null if the path does not
   * exist.
   * @param pattern the pattern to expand as a glob
   * @param conf the hadoop configuration
   * @return list of {@link PathData} objects.  if the pattern is not a glob,
   * and does not exist, the list will contain a single PathData with a null
   * stat 
   * @throws IOException anything else goes wrong...
   */
  public static PathData[] expandAsGlob(String pattern, Configuration conf)
  throws IOException {
    Path globPath = new Path(pattern);
    FileSystem fs = globPath.getFileSystem(conf);    
    FileStatus[] stats = fs.globStatus(globPath);
    PathData[] items = null;
    
    if (stats == null) {
      // not a glob & file not found, so add the path with a null stat
      items = new PathData[]{ new PathData(fs, pattern, null) };
    } else if (
        // this is very ugly, but needed to avoid breaking hdfs tests...
        // if a path has no authority, then the FileStatus from globStatus
        // will add the "-fs" authority into the path, so we need to sub
        // it back out to satisfy the tests
        stats.length == 1
        &&
        stats[0].getPath().equals(fs.makeQualified(globPath)))
    {
      // if the fq path is identical to the pattern passed, use the pattern
      // to initialize the string value
      items = new PathData[]{ new PathData(fs, pattern, stats[0]) };
    } else {
      // convert stats to PathData
      items = new PathData[stats.length];
      int i=0;
      for (FileStatus stat : stats) {
        items[i++] = new PathData(fs, stat);
      }
    }
    return items;
  }

  /**
   * Returns the printable version of the path that is either the path
   * as given on the commandline, or the full path
   * @return String of the path
   */
  public String toString() {
    return (string != null) ? string : path.toString();
  }
}
