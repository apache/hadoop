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


// Keeps track of which datanodes are allowed to connect to the namenode.
public class HostsFileReader {
  private Set<String> includes;
  private Set<String> excludes;
  private String includesFile;
  private String excludesFile;

  public HostsFileReader(String inFile, 
                         String exFile) throws IOException {
    includes = new HashSet<String>();
    excludes = new HashSet<String>();
    includesFile = inFile;
    excludesFile = exFile;
    refresh();
  }

  private void readFileToSet(String filename, Set<String> set) throws IOException {
    FileInputStream fis = new FileInputStream(new File(filename));
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fis));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] nodes = line.split("[ \t\n\f\r]+");
        if (nodes != null) {
          for (int i = 0; i < nodes.length; i++) {
            if (!nodes[i].equals("")) {
              set.add(nodes[i]);  // might need to add canonical name
            }
          }
        }
      }   
    } finally {
      if (reader != null) {
        reader.close();
      }
      fis.close();
    }  
  }

  public synchronized void refresh() throws IOException {
    includes.clear();
    excludes.clear();
    
    if (!includesFile.equals("")) {
      readFileToSet(includesFile, includes);
    }
    if (!excludesFile.equals("")) {
      readFileToSet(excludesFile, excludes);
    }
  }

  public Set<String> getHosts() {
    return includes;
  }

  public Set<String> getExcludedHosts() {
    return excludes;
  }

  public synchronized void setIncludesFile(String includesFile) {
    this.includesFile = includesFile;
  }
  
  public synchronized void setExcludesFile(String excludesFile) {
    this.excludesFile = excludesFile;
  }

  public synchronized void updateFileNames(String includesFile, 
                                           String excludesFile) 
                                           throws IOException {
    setIncludesFile(includesFile);
    setExcludesFile(excludesFile);
  }
}
