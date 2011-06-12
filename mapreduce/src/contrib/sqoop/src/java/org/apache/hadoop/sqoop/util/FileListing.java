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

package org.apache.hadoop.sqoop.util;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
* Recursive file listing under a specified directory.
*
* Taken from http://www.javapractices.com/topic/TopicAction.do?Id=68
* Used under the terms of the CC Attribution license:
* http://creativecommons.org/licenses/by/3.0/
*
* Method by Alex Wong (javapractices.com)
*/
public final class FileListing {

  private FileListing() { }

  /**
  * Demonstrate use.
  *
  * @param aArgs - <tt>aArgs[0]</tt> is the full name of an existing
  * directory that can be read.
  */
  public static void main(String... aArgs) throws FileNotFoundException {
    File startingDirectory = new File(aArgs[0]);
    List<File> files = FileListing.getFileListing(startingDirectory);

    //print out all file names, in the the order of File.compareTo()
    for (File file : files) {
      System.out.println(file);
    }
  }

  /**
  * Recursively walk a directory tree and return a List of all
  * Files found; the List is sorted using File.compareTo().
  *
  * @param aStartingDir is a valid directory, which can be read.
  */
  public static List<File> getFileListing(File aStartingDir) throws FileNotFoundException {
    validateDirectory(aStartingDir);
    List<File> result = getFileListingNoSort(aStartingDir);
    Collections.sort(result);
    return result;
  }

  // PRIVATE //
  private static List<File> getFileListingNoSort(File aStartingDir) throws FileNotFoundException {
    List<File> result = new ArrayList<File>();
    File[] filesAndDirs = aStartingDir.listFiles();
    List<File> filesDirs = Arrays.asList(filesAndDirs);
    for (File file : filesDirs) {
      result.add(file); //always add, even if directory
      if (!file.isFile()) {
        //must be a directory
        //recursive call!
        List<File> deeperList = getFileListingNoSort(file);
        result.addAll(deeperList);
      }
    }
    return result;
  }

  /**
  * Directory is valid if it exists, does not represent a file, and can be read.
  */
  private static void validateDirectory(File aDirectory) throws FileNotFoundException {
    if (aDirectory == null) {
      throw new IllegalArgumentException("Directory should not be null.");
    }
    if (!aDirectory.exists()) {
      throw new FileNotFoundException("Directory does not exist: " + aDirectory);
    }
    if (!aDirectory.isDirectory()) {
      throw new IllegalArgumentException("Is not a directory: " + aDirectory);
    }
    if (!aDirectory.canRead()) {
      throw new IllegalArgumentException("Directory cannot be read: " + aDirectory);
    }
  }

  /**
   * Recursively delete a directory and all its children
   * @param aStartingDir is a valid directory.
   */
  public static void recursiveDeleteDir(File dir) throws IOException {
    if (!dir.exists()) {
      throw new FileNotFoundException(dir.toString() + " does not exist");
    }

    if (dir.isDirectory()) {
      // recursively descend into all children and delete them.
      File [] children = dir.listFiles();
      for (File child : children) {
        recursiveDeleteDir(child);
      }
    }

    if (!dir.delete()) {
      throw new IOException("Could not remove: " + dir);
    }
  }
}

