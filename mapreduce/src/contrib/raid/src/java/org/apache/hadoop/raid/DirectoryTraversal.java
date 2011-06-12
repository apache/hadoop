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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

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
  private ExecutorService executor;

  private int numThreads;

  /**
   * A FileFilter object can be used to choose files during directory traversal.
   */
  public interface FileFilter {
    /**
     * @return a boolean value indicating if the file passes the filter.
     */
    boolean check(FileStatus f) throws IOException;
  }

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
    this(fs, startPaths, 1);
  }

  public DirectoryTraversal(
    FileSystem fs, List<FileStatus> startPaths, int numThreads) {
    this.fs = fs;
    paths = startPaths;
    pathIdx = 0;
    this.numThreads = numThreads;
    executor = Executors.newFixedThreadPool(numThreads);
  }

  public List<FileStatus> getFilteredFiles(FileFilter filter, int limit) {
    List<FileStatus> filtered = new ArrayList<FileStatus>();

    // We need this semaphore to block when the number of running workitems
    // is equal to the number of threads. FixedThreadPool limits the number
    // of threads, but not the queue size. This way we will limit the memory
    // usage.
    Semaphore slots = new Semaphore(numThreads);

    while (true) {
      synchronized(filtered) {
        if (filtered.size() >= limit) break;
      }
      FilterFileWorkItem work = null;
      try {
        Node next = getNextDirectoryNode();
        if (next == null) {
          break;
        }
        work = new FilterFileWorkItem(filter, next, filtered, slots);
        slots.acquire();
      } catch (InterruptedException ie) {
        break;
      } catch (IOException e) {
        break;
      }
      executor.execute(work);
    }

    try {
      // Wait for all submitted items to finish.
      slots.acquire(numThreads);
      // If this traversal is finished, shutdown the executor.
      if (doneTraversal()) {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
      }
    } catch (InterruptedException ie) {
    }

    return filtered;
  }

  class FilterFileWorkItem implements Runnable {
    FileFilter filter;
    Node dir;
    List<FileStatus> filtered;
    Semaphore slots;

    FilterFileWorkItem(FileFilter filter, Node dir, List<FileStatus> filtered,
      Semaphore slots) {
      this.slots = slots;
      this.filter = filter;
      this.dir = dir;
      this.filtered = filtered;
    }

    @SuppressWarnings("deprecation")
    public void run() {
      try {
        LOG.info("Initiating file filtering for " + dir.path.getPath());
        for (FileStatus f: dir.elements) {
          if (!f.isFile()) {
            continue;
          }
          if (filter.check(f)) {
            synchronized(filtered) {
              filtered.add(f);
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Error in directory traversal: " 
          + StringUtils.stringifyException(e));
      } finally {
        slots.release();
      }
    }
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
    Node dirNode = getNextDirectoryNode();
    if (dirNode != null) {
      return dirNode.path;
    }
    return null;
  }

  private Node getNextDirectoryNode() throws IOException {

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
          return node;
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
    FileStatus[] elements = fs.listStatus(p);
    Node newNode = new Node(stat, (elements == null? new FileStatus[0]: elements));
    stack.push(newNode);
  }

  public boolean doneTraversal() {
    return paths.isEmpty() && stack.isEmpty();
  }
}
