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

package org.apache.hadoop.lib.server;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.HTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class TestServerConstructor extends HTestCase {

  @Parameterized.Parameters
  public static Collection constructorFailParams() {
    return Arrays.asList(new Object[][]{
      {null, null, null, null, null, null},
      {"", null, null, null, null, null},
      {null, null, null, null, null, null},
      {"server", null, null, null, null, null},
      {"server", "", null, null, null, null},
      {"server", "foo", null, null, null, null},
      {"server", "/tmp", null, null, null, null},
      {"server", "/tmp", "", null, null, null},
      {"server", "/tmp", "foo", null, null, null},
      {"server", "/tmp", "/tmp", null, null, null},
      {"server", "/tmp", "/tmp", "", null, null},
      {"server", "/tmp", "/tmp", "foo", null, null},
      {"server", "/tmp", "/tmp", "/tmp", null, null},
      {"server", "/tmp", "/tmp", "/tmp", "", null},
      {"server", "/tmp", "/tmp", "/tmp", "foo", null}});
  }

  private String name;
  private String homeDir;
  private String configDir;
  private String logDir;
  private String tempDir;
  private Configuration conf;

  public TestServerConstructor(String name, String homeDir, String configDir, String logDir, String tempDir,
                               Configuration conf) {
    this.name = name;
    this.homeDir = homeDir;
    this.configDir = configDir;
    this.logDir = logDir;
    this.tempDir = tempDir;
    this.conf = conf;
  }


  @Test(expected = IllegalArgumentException.class)
  public void constructorFail() {
    new Server(name, homeDir, configDir, logDir, tempDir, conf);
  }

}
