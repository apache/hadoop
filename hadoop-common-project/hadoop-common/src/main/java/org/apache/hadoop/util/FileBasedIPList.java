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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * FileBasedIPList loads a list of subnets in CIDR format and ip addresses from
 * a file.
 *
 * Given an ip address, isIn  method returns true if ip belongs to one of the
 * subnets.
 *
 * Thread safe.
 */
public class FileBasedIPList implements IPList {

  private static final Log LOG = LogFactory.getLog(FileBasedIPList.class);

  private final String fileName;
  private final MachineList addressList;

  public FileBasedIPList(String fileName) {
    this.fileName = fileName;
    String[] lines;
    try {
      lines = readLines(fileName);
    } catch (IOException e) {
      lines = null;
    }
    if (lines != null) {
      addressList = new MachineList(new HashSet<String>(Arrays.asList(lines)));
    } else {
      addressList = null;
    }
  }

  public FileBasedIPList reload() {
    return new FileBasedIPList(fileName);
  }

  @Override
  public  boolean isIn(String ipAddress) {
    if (ipAddress == null || addressList == null) {
      return false;
    }
    return addressList.includes(ipAddress);
  }

  /**
   * Reads the lines in a file.
   * @param fileName
   * @return lines in a String array; null if the file does not exist or if the
   * file name is null
   * @throws IOException
   */
  private static String[] readLines(String fileName) throws IOException {
    try {
      if (fileName != null) {
        File file = new File (fileName);
        if (file.exists()) {
          try (
              Reader fileReader = new InputStreamReader(
                  new FileInputStream(file), Charsets.UTF_8);
              BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            List<String> lines = new ArrayList<String>();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
              lines.add(line);
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Loaded IP list of size = " + lines.size() +
                  " from file = " + fileName);
            }
            return (lines.toArray(new String[lines.size()]));
          }
        } else {
          LOG.debug("Missing ip list file : "+ fileName);
        }
      }
    } catch (IOException ioe) {
      LOG.error(ioe);
      throw ioe;
    }
    return null;
  }
}
