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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestWindowsBasedProcessTree {
  private static final Log LOG = LogFactory
      .getLog(TestWindowsBasedProcessTree.class);
  
  class WindowsBasedProcessTreeTester extends WindowsBasedProcessTree {
    String infoStr = null;
    public WindowsBasedProcessTreeTester(String pid) {
      super(pid);
    }
    @Override
    String getAllProcessInfoFromShell() {
      return infoStr;
    }
  }

  @Test (timeout = 30000)
  public void tree() {
    if( !Shell.WINDOWS) {
      LOG.info("Platform not Windows. Not testing");
      return;      
    }
    assertTrue("WindowsBasedProcessTree should be available on Windows", 
               WindowsBasedProcessTree.isAvailable());
    
    
    WindowsBasedProcessTreeTester pTree = new WindowsBasedProcessTreeTester("-1");
    pTree.infoStr = "3524,1024,1024,500\r\n2844,1024,1024,500\r\n";
    pTree.updateProcessTree();
    assertTrue(pTree.getCumulativeVmem() == 2048);
    assertTrue(pTree.getCumulativeVmem(0) == 2048);
    assertTrue(pTree.getCumulativeRssmem() == 2048);
    assertTrue(pTree.getCumulativeRssmem(0) == 2048);
    assertTrue(pTree.getCumulativeCpuTime() == 1000);

    pTree.infoStr = "3524,1024,1024,1000\r\n2844,1024,1024,1000\r\n1234,1024,1024,1000\r\n";
    pTree.updateProcessTree();
    assertTrue(pTree.getCumulativeVmem() == 3072);
    assertTrue(pTree.getCumulativeVmem(1) == 2048);
    assertTrue(pTree.getCumulativeRssmem() == 3072);
    assertTrue(pTree.getCumulativeRssmem(1) == 2048);
    assertTrue(pTree.getCumulativeCpuTime() == 3000);    

    pTree.infoStr = "3524,1024,1024,1500\r\n2844,1024,1024,1500\r\n";
    pTree.updateProcessTree();
    assertTrue(pTree.getCumulativeVmem() == 2048);
    assertTrue(pTree.getCumulativeVmem(2) == 2048);
    assertTrue(pTree.getCumulativeRssmem() == 2048);
    assertTrue(pTree.getCumulativeRssmem(2) == 2048);
    assertTrue(pTree.getCumulativeCpuTime() == 4000);    
  }
}
