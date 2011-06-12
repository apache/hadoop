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

package org.apache.hadoop.raid;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Implements depth-first traversal using a Stack object. The traversal
 * can be stopped at any time and the state of traversal is saved.
 */
public class DirectoryTraversal {
  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.raid.DirectoryTraversal");

  private FileSystem fs;
  private List<FileStatus> paths;
  private int pathIdx = 0;  // Next path to process.
  private Stack<Node> stack = new Stack<Node>();

  /**
   * Represents a directory node in directory traversal.
   */
  static class Node {
    private FileStatus path;  // Path that this node represents.
    private FileStatus[] elements;  // Elements in the node.
    private int idx = 0;

    public Node(FileStatus path, FileStatus[] elements) {
      this.path = path;
      this.elements = elements;
    }

    public boolean hasNext() {
      return idx < elements.length;
    }

    public FileStatus next() {
      return elements[idx++];
    }

    public FileStatus path() {
      return this.path;
    }
  }

  /**
   * Constructor.
   * @param fs The filesystem to use.
   * @param startPaths A list of paths that need to be traversed
   */
  public DirectoryTraversal(FileSystem fs, List<FileStatus> startPaths) {
    this.fs = fs;
    paths = startPaths;
    pathIdx = 0;
  }

  /**
   * Choose some files to RAID.
   * @param conf Configuration to use.
   * @param raidDestPrefix Prefix of the path to RAID to.
   * @param modTimePeriod Time gap before RAIDing.
   * @param limit Limit on the number of files to choose.
   * @return list of files to RAID.
   * @throws IOException
   */
  public List<FileStatus> selectFilesToRaid(
      Configuration conf, int targetRepl, Path raidDestPrefix,
      long modTimePeriod, int limit) throws IOException {
    List<FileStatus> selected = new LinkedList<FileStatus>();
    int numSelected = 0;

    long now = System.currentTimeMillis();
    while (numSelected < limit) {
      FileStatus next = getNextFile();
      if (next == null) {
        break;
      }
      // We have the next file, do we want to select it?
      // If the source file has fewer than or equal to 2 blocks, then skip it.
      long blockSize = next.getBlockSize();
      if (2 * blockSize >= next.getLen()) {
        continue;
      }

      boolean select = false;
      try {
        Object ppair = RaidNode.getParityFile(
            raidDestPrefix, next.getPath(), conf);
        // Is there is a valid parity file?
        if (ppair != null) {
          // Is the source at the target replication?
          if (next.getReplication() != targetRepl) {
            // Select the file so that its replication can be set.
            select = true;
          } else {
            // Nothing to do, don't select the file.
            select = false;
          }
        } else if (next.getModificationTime() + modTimePeriod < now) {
          // If there isn't a valid parity file, check if the file is too new.
          select = true;
        }
      } catch (java.io.FileNotFoundException e) {
        select = true; // destination file does not exist
      }
      if (select) {
        selected.add(next);
        numSelected++;
      }
    }

    return selected;
  }

  /**
   * Return the next file.
   * @throws IOException
   */
  public FileStatus getNextFile() throws IOException {
    // Check if traversal is done.
    while (!doneTraversal()) {
      // If traversal is not done, check if the stack is not empty.
      while (!stack.isEmpty()) {
        // If the stack is not empty, look at the top node.
        Node node = stack.peek();
        // Check if the top node has an element.
        if (node.hasNext()) {
          FileStatus element = node.next();
          // Is the next element a directory.
          if (!element.isDir()) {
            // It is a file, return it.
            return element;
          }
          // Next element is a directory, push it on to the stack and
          // continue
          try {
            pushNewNode(element);
          } catch (FileNotFoundException e) {
            // Ignore and move to the next element.
          }
          continue;
        } else {
          // Top node has no next element, pop it and continue.
          stack.pop();
          continue;
        }
      }
      // If the stack is empty, do we have more paths?
      while (!paths.isEmpty()) {
        FileStatus next = paths.remove(0);
        pathIdx++;
        if (!next.isDir()) {
          return next;
        }
        try {
          pushNewNode(next);
        } catch (FileNotFoundException e) {
          continue;
        }
        break;
      }
    }
    return null;
  }

  /**
   * Gets the next directory in the tree. The algorithm returns deeper directories
   * first.
   * @return A FileStatus representing the directory.
   * @throws IOException
   */
  public FileStatus getNextDirectory() throws IOException {
    // Check if traversal is done.
    while (!doneTraversal()) {
      // If traversal is not done, check if the stack is not empty.
      while (!stack.isEmpty()) {
        // If the stack is not empty, look at the top node.
        Node node = stack.peek();
        // Check if the top node has an element.
        if (node.hasNext()) {
          FileStatus element = node.next();
          // Is the next element a directory.
          if (element.isDir()) {
            // Next element is a directory, push it on to the stack and
            // continue
            try {
              pushNewNode(element);
            } catch (FileNotFoundException e) {
              // Ignore and move to the next element.
            }
            continue;
          }
        } else {
          stack.pop();
          return node.path;
        }
      }
      // If the stack is empty, do we have more paths?
      while (!paths.isEmpty()) {
        FileStatus next = paths.remove(0);
        pathIdx++;
        if (next.isDir()) {
          try {
            pushNewNode(next);
          } catch (FileNotFoundException e) {
            continue;
          }
          break;
        }
      }
    }
    return null;
  }

  private void pushNewNode(FileStatus stat) throws IOException {
    if (!stat.isDir()) {
      return;
    }
    Path p = stat.getPath();
    LOG.info("Traversing to directory " + p);
    FileStatus[] elements = fs.listStatus(p);
    Node newNode = new Node(stat, (elements == null? new FileStatus[0]: elements));
    stack.push(newNode);
  }

  public boolean doneTraversal() {
    return paths.isEmpty() && stack.isEmpty();
  }
}
