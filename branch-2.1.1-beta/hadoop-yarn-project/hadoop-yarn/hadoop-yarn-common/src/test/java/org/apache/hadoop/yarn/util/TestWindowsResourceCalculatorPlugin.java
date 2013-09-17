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

package org.apache.hadoop.yarn.util;

import junit.framework.TestCase;
import org.junit.Test;

public class TestWindowsResourceCalculatorPlugin extends TestCase {
  
  
  class WindowsResourceCalculatorPluginTester extends WindowsResourceCalculatorPlugin {
    private String infoStr = null;
    @Override
    String getSystemInfoInfoFromShell() {
      return infoStr;
    }    
  }

  @Test (timeout = 30000)
  public void testParseSystemInfoString() {
    WindowsResourceCalculatorPluginTester tester = new WindowsResourceCalculatorPluginTester();
    // info str derived from windows shell command has \r\n termination
    tester.infoStr = "17177038848,8589467648,15232745472,6400417792,1,2805000,6261812\r\n";
    // call a method to refresh values
    tester.getAvailablePhysicalMemorySize();
    // verify information has been refreshed
    assertTrue(tester.vmemSize == 17177038848L);
    assertTrue(tester.memSize == 8589467648L);
    assertTrue(tester.vmemAvailable == 15232745472L);
    assertTrue(tester.memAvailable == 6400417792L);
    assertTrue(tester.numProcessors == 1);
    assertTrue(tester.cpuFrequencyKhz == 2805000L);
    assertTrue(tester.cumulativeCpuTimeMs == 6261812L);
    assertTrue(tester.cpuUsage == -1);
  }

  @Test (timeout = 20000)
  public void testRefreshAndCpuUsage() throws InterruptedException {
    WindowsResourceCalculatorPluginTester tester = new WindowsResourceCalculatorPluginTester();
    // info str derived from windows shell command has \r\n termination
    tester.infoStr = "17177038848,8589467648,15232745472,6400417792,1,2805000,6261812\r\n";
    tester.getAvailablePhysicalMemorySize();
    // verify information has been refreshed
    assertTrue(tester.memAvailable == 6400417792L);
    assertTrue(tester.cpuUsage == -1);
    
    tester.infoStr = "17177038848,8589467648,15232745472,5400417792,1,2805000,6261812\r\n";
    tester.getAvailablePhysicalMemorySize();
    // verify information has not been refreshed
    assertTrue(tester.memAvailable == 6400417792L);
    assertTrue(tester.cpuUsage == -1);
    
    Thread.sleep(1500);
    tester.infoStr = "17177038848,8589467648,15232745472,5400417792,1,2805000,6286812\r\n";
    tester.getAvailablePhysicalMemorySize();
    // verify information has been refreshed
    assertTrue(tester.memAvailable == 5400417792L);
    assertTrue(tester.cpuUsage >= 0.1);
  }

  @Test (timeout = 20000)
  public void testErrorInGetSystemInfo() {
    WindowsResourceCalculatorPluginTester tester = new WindowsResourceCalculatorPluginTester();
    // info str derived from windows shell command has \r\n termination
    tester.infoStr = null;
    // call a method to refresh values
    tester.getAvailablePhysicalMemorySize();    
  }

}
