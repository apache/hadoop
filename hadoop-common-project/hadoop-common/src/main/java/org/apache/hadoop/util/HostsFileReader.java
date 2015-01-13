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
  
  private static final Log LOG = LogFactory.getLog(HostsFileReader.class);

  public HostsFileReader(String inFile, 
                         String exFile) throws IOException {
    includes = new HashSet<String>();
    excludes = new HashSet<String>();
    includesFile = inFile;
    excludesFile = exFile;
    refresh();
  }

  @Private
  public HostsFileReader(String includesFile, InputStream inFileInputStream,
      String excludesFile, InputStream exFileInputStream) throws IOException {
    includes = new HashSet<String>();
    excludes = new HashSet<String>();
    this.includesFile = includesFile;
    this.excludesFile = excludesFile;
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
            if (nodes[i].trim().startsWith("#")) {
              // Everything from now on is a comment
              break;
            }
            if (!nodes[i].isEmpty()) {
              LOG.info("Adding " + nodes[i] + " to the list of " + type +
                  " hosts from " + filename);
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

  public synchronized void refresh() throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    Set<String> newIncludes = new HashSet<String>();
    Set<String> newExcludes = new HashSet<String>();
    boolean switchIncludes = false;
    boolean switchExcludes = false;
    if (!includesFile.isEmpty()) {
      readFileToSet("included", includesFile, newIncludes);
      switchIncludes = true;
    }
    if (!excludesFile.isEmpty()) {
      readFileToSet("excluded", excludesFile, newExcludes);
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
  }

  @Private
  public synchronized void refresh(InputStream inFileInputStream,
      InputStream exFileInputStream) throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
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
  }

  public synchronized Set<String> getHosts() {
    return includes;
  }

  public synchronized Set<String> getExcludedHosts() {
    return excludes;
  }

  public synchronized void setIncludesFile(String includesFile) {
    LOG.info("Setting the includes file to " + includesFile);
    this.includesFile = includesFile;
  }
  
  public synchronized void setExcludesFile(String excludesFile) {
    LOG.info("Setting the excludes file to " + excludesFile);
    this.excludesFile = excludesFile;
  }

  public synchronized void updateFileNames(String includesFile,
      String excludesFile) {
    setIncludesFile(includesFile);
    setExcludesFile(excludesFile);
  }
}
