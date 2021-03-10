/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class ProtectedDirsConfigReader {

  private static final Log LOG = LogFactory.getLog(ProtectedDirsConfigReader.class);

  private Set<String> currentDirectories;

  public ProtectedDirsConfigReader(String configFile) throws IOException {
    currentDirectories = new HashSet<String>();
    loadConfig(configFile);
  }

  private void readFileToSet(String filename,
      Set<String> set) throws IOException {
    File file = new File(filename);
    InputStream fis = Files.newInputStream(file.toPath());
    readFileToSetWithFileInputStream(filename, fis, set);
  }

  private void readFileToSetWithFileInputStream(String filename,
      InputStream fileInputStream, Set<String> set)
      throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(
          new InputStreamReader(fileInputStream, StandardCharsets.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] paths = line.split("[ \t\n\f\r]+");
        if (paths != null) {
          for (int i = 0; i < paths.length; i++) {
            paths[i] = paths[i].trim();
            if (paths[i].startsWith("#")) {
              // Everything from now on is a comment
              break;
            }
            if (!paths[i].isEmpty()) {
              LOG.info("Adding " + paths[i] + " to the list of " +
                  " protected directories from " + filename);
              set.add(paths[i]);
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

  private synchronized void loadConfig(String configFile) throws IOException {
    LOG.info("Loading protected directories");
    Set<String> newDirs = new HashSet<String>();

    if (!configFile.isEmpty()) {
      readFileToSet(configFile, newDirs);
      currentDirectories = Collections.unmodifiableSet(newDirs);
    }
  }

  /**
   * Duplicate and empty values are removed
   *
   * @return currentDirectories
   */
  public synchronized Set<String> getProtectedProtectedDirs() {
    return currentDirectories;
  }

  public static Set<String> parseProtectedProtectedDirsFromConfig(String configFile) {
    try {
      ProtectedDirsConfigReader reader =
          new ProtectedDirsConfigReader(configFile);
      return reader.getProtectedProtectedDirs();
    } catch (NoSuchFileException ex) {
      LOG.warn("The protected directories config flle is not found in " +
          configFile);
      return new HashSet<String>();
    } catch (IOException ex) {
      LOG.error("Error in ProtectedDirsConfigReader.parseProtectedProtectedDirsFromConfig", ex);
      return new HashSet<String>();
    }
  }
}
