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
package org.apache.hadoop.fs;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;

public class TestDFVariations extends TestCase {

  public static class XXDF extends DF {
    private final String osName;
    public XXDF(String osName) throws IOException {
      super(new File(System.getProperty("test.build.data","/tmp")), 0L);
      this.osName = osName;
    }
    @Override
    public DF.OSType getOSType() {
      return DF.getOSType(osName);
    }
    @Override
    protected String[] getExecString() {
      switch(getOSType()) {
        case OS_TYPE_AIX:
          return new String[] { "echo", "IGNORE\n", "/dev/sda3",
            "453115160", "400077240", "11%", "18", "skip%", "/foo/bar", "\n" };
        default:
          return new String[] { "echo", "IGNORE\n", "/dev/sda3",
            "453115160", "53037920", "400077240", "11%", "/foo/bar", "\n" };
      }
    }
  }

  public void testOSParsing() throws Exception {
    for (DF.OSType ost : EnumSet.allOf(DF.OSType.class)) {
      XXDF df = new XXDF(ost.getId());
      assertEquals(ost.getId() + " mount", "/foo/bar", df.getMount());
    }
  }

}

