/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.service.utils;

import org.apache.hadoop.yarn.service.conf.SliderExitCodes;
import org.apache.hadoop.yarn.service.exceptions.BadConfigException;
import org.apache.hadoop.yarn.service.exceptions.SliderException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * a scanner which can take an input string for a range or scan the lot.
 */
public class PortScanner {
  private static Pattern NUMBER_RANGE = Pattern.compile("^(\\d+)\\s*-\\s*(\\d+)$");
  private static Pattern SINGLE_NUMBER = Pattern.compile("^\\d+$");

  private List<Integer> remainingPortsToCheck;

  public PortScanner() {
  }

  public void setPortRange(String input) throws BadConfigException {
    // first split based on commas
    Set<Integer> inputPorts= new TreeSet<Integer>();
    String[] ranges = input.split(",");
    for ( String range : ranges ) {
      if (range.trim().isEmpty()) {
        continue;
      }
      Matcher m = SINGLE_NUMBER.matcher(range.trim());
      if (m.find()) {
        inputPorts.add(Integer.parseInt(m.group()));
        continue;
      }
      m = NUMBER_RANGE.matcher(range.trim());
      if (m.find()) {
        String[] boundaryValues = m.group(0).split("-");
        int start = Integer.parseInt(boundaryValues[0].trim());
        int end = Integer.parseInt(boundaryValues[1].trim());
        if (end < start) {
          throw new BadConfigException("End of port range is before start: "
              + range + " in input: " + input);
        }
        for (int i = start; i < end + 1; i++) {
          inputPorts.add(i);
        }
        continue;
      }
      throw new BadConfigException("Bad port range: " + range + " in input: "
          + input);
    }
    if (inputPorts.size() == 0) {
      throw new BadConfigException("No ports found in range: " + input);
    }
    this.remainingPortsToCheck = new ArrayList<Integer>(inputPorts);
  }

  public List<Integer> getRemainingPortsToCheck() {
    return remainingPortsToCheck;
  }

  public int getAvailablePort() throws SliderException, IOException {
    if (remainingPortsToCheck != null) {
      return getAvailablePortViaPortArray();
    } else {
      return ServiceUtils.getOpenPort();
    }
  }

  private int getAvailablePortViaPortArray() throws SliderException {
    boolean found = false;
    int availablePort = -1;
    Iterator<Integer> portsToCheck = this.remainingPortsToCheck.iterator();
    while (portsToCheck.hasNext() && !found) {
      int portToCheck = portsToCheck.next();
      found = ServiceUtils.isPortAvailable(portToCheck);
      if (found) {
        availablePort = portToCheck;
        portsToCheck.remove();
      }
    }

    if (availablePort < 0) {
      throw new SliderException(SliderExitCodes.EXIT_BAD_CONFIGURATION,
        "No available ports found in configured range {}",
        remainingPortsToCheck);
    }

    return availablePort;
  }
}
