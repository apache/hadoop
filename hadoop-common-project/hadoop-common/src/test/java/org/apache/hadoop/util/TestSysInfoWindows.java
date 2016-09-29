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

import org.junit.Test;
import static org.junit.Assert.*;

public class TestSysInfoWindows {


  static class SysInfoWindowsMock extends SysInfoWindows {
    private long time = SysInfoWindows.REFRESH_INTERVAL_MS + 1;
    private String infoStr = null;
    void setSysinfoString(String infoStr) {
      this.infoStr = infoStr;
    }
    void advance(long dur) {
      time += dur;
    }
    @Override
    String getSystemInfoInfoFromShell() {
      return infoStr;
    }
    @Override
    long now() {
      return time;
    }
  }

  @Test(timeout = 10000)
  public void parseSystemInfoString() {
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,6400417792,1,2805000,6261812," +
        "1234567,2345678,3456789,4567890\r\n");
    // info str derived from windows shell command has \r\n termination
    assertEquals(17177038848L, tester.getVirtualMemorySize());
    assertEquals(8589467648L, tester.getPhysicalMemorySize());
    assertEquals(15232745472L, tester.getAvailableVirtualMemorySize());
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());
    assertEquals(1, tester.getNumProcessors());
    assertEquals(1, tester.getNumCores());
    assertEquals(2805000L, tester.getCpuFrequency());
    assertEquals(6261812L, tester.getCumulativeCpuTime());
    assertEquals(1234567L, tester.getStorageBytesRead());
    assertEquals(2345678L, tester.getStorageBytesWritten());
    assertEquals(3456789L, tester.getNetworkBytesRead());
    assertEquals(4567890L, tester.getNetworkBytesWritten());
    // undef on first call
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getCpuUsagePercentage(), 0.0);
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getNumVCoresUsed(), 0.0);
  }

  @Test(timeout = 10000)
  public void refreshAndCpuUsage() throws InterruptedException {
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,6400417792,1,2805000,6261812," +
        "1234567,2345678,3456789,4567890\r\n");
    // info str derived from windows shell command has \r\n termination
    tester.getAvailablePhysicalMemorySize();
    // verify information has been refreshed
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getCpuUsagePercentage(), 0.0);
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getNumVCoresUsed(), 0.0);

    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,5400417792,1,2805000,6263012," +
        "1234567,2345678,3456789,4567890\r\n");
    tester.getAvailablePhysicalMemorySize();
    // verify information has not been refreshed
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getCpuUsagePercentage(), 0.0);
    assertEquals((float)CpuTimeTracker.UNAVAILABLE,
        tester.getNumVCoresUsed(), 0.0);

    // advance clock
    tester.advance(SysInfoWindows.REFRESH_INTERVAL_MS + 1);

    // verify information has been refreshed
    assertEquals(5400417792L, tester.getAvailablePhysicalMemorySize());
    assertEquals((6263012 - 6261812) * 100F /
                 (SysInfoWindows.REFRESH_INTERVAL_MS + 1f) / 1,
                 tester.getCpuUsagePercentage(), 0.0);
    assertEquals((6263012 - 6261812) /
                 (SysInfoWindows.REFRESH_INTERVAL_MS + 1f) / 1,
                 tester.getNumVCoresUsed(), 0.0);
  }

  @Test(timeout = 10000)
  public void refreshAndCpuUsageMulticore() throws InterruptedException {
    // test with 12 cores
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,6400417792,12,2805000,6261812," +
        "1234567,2345678,3456789,4567890\r\n");
    // verify information has been refreshed
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());

    tester.setSysinfoString(
        "17177038848,8589467648,15232745472,5400417792,12,2805000,6263012," +
        "1234567,2345678,3456789,4567890\r\n");
    // verify information has not been refreshed
    assertEquals(6400417792L, tester.getAvailablePhysicalMemorySize());

    // advance clock
    tester.advance(SysInfoWindows.REFRESH_INTERVAL_MS + 1);

    // verify information has been refreshed
    assertEquals(5400417792L, tester.getAvailablePhysicalMemorySize());
    // verify information has been refreshed
    assertEquals((6263012 - 6261812) * 100F /
                 (SysInfoWindows.REFRESH_INTERVAL_MS + 1f) / 12,
                 tester.getCpuUsagePercentage(), 0.0);
    assertEquals((6263012 - 6261812) /
                 (SysInfoWindows.REFRESH_INTERVAL_MS + 1f),
                 tester.getNumVCoresUsed(), 0.0);
  }

  @Test(timeout = 10000)
  public void errorInGetSystemInfo() {
    SysInfoWindowsMock tester = new SysInfoWindowsMock();
    // info str derived from windows shell command is null
    tester.setSysinfoString(null);
    // call a method to refresh values
    tester.getAvailablePhysicalMemorySize();

    // info str derived from windows shell command with no \r\n termination
    tester.setSysinfoString("");
    // call a method to refresh values
    tester.getAvailablePhysicalMemorySize();
  }

}
