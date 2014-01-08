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

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

public class TestHdfsNativeCodeLoader {
  static final Log LOG = LogFactory.getLog(TestHdfsNativeCodeLoader.class);

  private static boolean requireTestJni() {
    String rtj = System.getProperty("require.test.libhadoop");
    if (rtj == null) return false;
    if (rtj.compareToIgnoreCase("false") == 0) return false;
    return true;
  }

  @Test
  public void testNativeCodeLoaded() {
    if (requireTestJni() == false) {
      LOG.info("TestNativeCodeLoader: libhadoop.so testing is not required.");
      return;
    }
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      String LD_LIBRARY_PATH = System.getenv().get("LD_LIBRARY_PATH");
      if (LD_LIBRARY_PATH == null) LD_LIBRARY_PATH = "";
      fail("TestNativeCodeLoader: libhadoop.so testing was required, but " +
          "libhadoop.so was not loaded.  LD_LIBRARY_PATH = " + LD_LIBRARY_PATH);
    }
    LOG.info("TestHdfsNativeCodeLoader: libhadoop.so is loaded.");
  }
}
