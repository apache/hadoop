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
package org.apache.hadoop.fs.http.server;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;
import org.junit.BeforeClass;

import java.io.File;

/**
 * Unlike {@link TestHttpFSServerWebServer}, httpfs-signature.secret doesn't
 * exist. In this case, a random secret is used.
 */
public class TestHttpFSServerWebServerWithRandomSecret extends
    TestHttpFSServerWebServer {
  @BeforeClass
  public static void beforeClass() throws Exception {
    File homeDir = GenericTestUtils.getTestDir();
    File confDir = new File(homeDir, "etc/hadoop");
    File logsDir = new File(homeDir, "logs");
    File tempDir = new File(homeDir, "temp");
    confDir.mkdirs();
    logsDir.mkdirs();
    tempDir.mkdirs();

    if (Shell.WINDOWS) {
      File binDir = new File(homeDir, "bin");
      binDir.mkdirs();
      File winutils = Shell.getWinUtilsFile();
      if (winutils.exists()) {
        FileUtils.copyFileToDirectory(winutils, binDir);
      }
    }

    System.setProperty("hadoop.home.dir", homeDir.getAbsolutePath());
    System.setProperty("hadoop.log.dir", logsDir.getAbsolutePath());
    System.setProperty("httpfs.home.dir", homeDir.getAbsolutePath());
    System.setProperty("httpfs.log.dir", logsDir.getAbsolutePath());
    System.setProperty("httpfs.config.dir", confDir.getAbsolutePath());
  }
}
