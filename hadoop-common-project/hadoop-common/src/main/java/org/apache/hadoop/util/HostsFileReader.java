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

package org.apache.hadoop.util;

import java.io.*;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;

// Keeps track of which datanodes/tasktrackers are allowed to connect to the 
// namenode/jobtracker.
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class HostsFileReader {
  private Set<String> includes;
  private Set<String> excludes;
  private String includesFile;
  private String excludesFile;
  private WriteLock writeLock;
  private ReadLock readLock;
  
  private static final Log LOG = LogFactory.getLog(HostsFileReader.class);

  public HostsFileReader(String inFile, 
                         String exFile) throws IOException {
    includes = new HashSet<String>();
    excludes = new HashSet<String>();
    includesFile = inFile;
    excludesFile = exFile;
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.writeLock = rwLock.writeLock();
    this.readLock = rwLock.readLock();
    refresh();
  }

  @Private
  public HostsFileReader(String includesFile, InputStream inFileInputStream,
      String excludesFile, InputStream exFileInputStream) throws IOException {
    includes = new HashSet<String>();
    excludes = new HashSet<String>();
    this.includesFile = includesFile;
    this.excludesFile = excludesFile;
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.writeLock = rwLock.writeLock();
    this.readLock = rwLock.readLock();
    refresh(inFileInputStream, exFileInputStream);
  }

  public static void readFileToSet(String type,
      String filename, Set<String> set) throws IOException {
    File file = new File(filename);
    FileInputStream fis = new FileInputStream(file);
    readFileToSetWithFileInputStream(type, filename, fis, set);
  }

  @Private
  public static void readFileToSetWithFileInputStream(String type,
      String filename, InputStream fileInputStream, Set<String> set)
      throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(
          new InputStreamReader(fileInputStream, Charsets.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] nodes = line.split("[ \t\n\f\r]+");
        if (nodes != null) {
          for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodes[i].trim();
            if (nodes[i].startsWith("#")) {
              // Everything from now on is a comment
              break;
            }
            if (!nodes[i].isEmpty()) {
              LOG.info("Adding a node \"" + nodes[i] + "\" to the list of "
                  + type + " hosts from " + filename);
              set.add(nodes[i]);
            }
          }
        }
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
      fileInputStream.close();
    }
  }

  public void refresh() throws IOException {
    this.writeLock.lock();
    try {
      refresh(includesFile, excludesFile);
    } finally {
      this.writeLock.unlock();
    }
  }

  public void refresh(String includeFiles, String excludeFiles)
      throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    this.writeLock.lock();
    try {
      // update instance variables
      updateFileNames(includeFiles, excludeFiles);
      Set<String> newIncludes = new HashSet<String>();
      Set<String> newExcludes = new HashSet<String>();
      boolean switchIncludes = false;
      boolean switchExcludes = false;
      if (includeFiles != null && !includeFiles.isEmpty()) {
        readFileToSet("included", includeFiles, newIncludes);
        switchIncludes = true;
      }
      if (excludeFiles != null && !excludeFiles.isEmpty()) {
        readFileToSet("excluded", excludeFiles, newExcludes);
        switchExcludes = true;
      }

      if (switchIncludes) {
        // switch the new hosts that are to be included
        includes = newIncludes;
      }
      if (switchExcludes) {
        // switch the excluded hosts
        excludes = newExcludes;
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Private
  public void refresh(InputStream inFileInputStream,
      InputStream exFileInputStream) throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    this.writeLock.lock();
    try {
      Set<String> newIncludes = new HashSet<String>();
      Set<String> newExcludes = new HashSet<String>();
      boolean switchIncludes = false;
      boolean switchExcludes = false;
      if (inFileInputStream != null) {
        readFileToSetWithFileInputStream("included", includesFile,
            inFileInputStream, newIncludes);
        switchIncludes = true;
      }
      if (exFileInputStream != null) {
        readFileToSetWithFileInputStream("excluded", excludesFile,
            exFileInputStream, newExcludes);
        switchExcludes = true;
      }
      if (switchIncludes) {
        // switch the new hosts that are to be included
        includes = newIncludes;
      }
      if (switchExcludes) {
        // switch the excluded hosts
        excludes = newExcludes;
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  public Set<String> getHosts() {
    this.readLock.lock();
    try {
      return includes;
    } finally {
      this.readLock.unlock();
    }
  }

  public Set<String> getExcludedHosts() {
    this.readLock.lock();
    try {
      return excludes;
    } finally {
      this.readLock.unlock();
    }
  }

  public void getHostDetails(Set<String> includes, Set<String> excludes) {
    this.readLock.lock();
    try {
      includes.addAll(this.includes);
      excludes.addAll(this.excludes);
    } finally {
      this.readLock.unlock();
    }
  }

  public void setIncludesFile(String includesFile) {
    LOG.info("Setting the includes file to " + includesFile);
    this.includesFile = includesFile;
  }
  
  public void setExcludesFile(String excludesFile) {
    LOG.info("Setting the excludes file to " + excludesFile);
    this.excludesFile = excludesFile;
  }

  public void updateFileNames(String includeFiles, String excludeFiles) {
    this.writeLock.lock();
    try {
      setIncludesFile(includeFiles);
      setExcludesFile(excludeFiles);
    } finally {
      this.writeLock.unlock();
    }
  }
}
