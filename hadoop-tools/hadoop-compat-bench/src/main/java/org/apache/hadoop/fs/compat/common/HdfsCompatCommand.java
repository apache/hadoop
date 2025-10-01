/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat.common;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.compat.suites.HdfsCompatSuiteForAll;
import org.apache.hadoop.fs.compat.suites.HdfsCompatSuiteForShell;
import org.apache.hadoop.fs.compat.suites.HdfsCompatSuiteForTpcds;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public class HdfsCompatCommand {
  private final Path uri;
  private final String suiteName;
  private final Configuration conf;
  private HdfsCompatSuite suite;
  private HdfsCompatApiScope api;
  private HdfsCompatShellScope shell;

  public HdfsCompatCommand(String uri, String suiteName, Configuration conf) {
    this.uri = new Path(uri);
    this.suiteName = suiteName.toLowerCase();
    this.conf = conf;
  }

  public void initialize() throws ReflectiveOperationException, IOException {
    initSuite();
    HdfsCompatEnvironment env = new HdfsCompatEnvironment(uri, conf);
    env.init();
    if (hasApiCase()) {
      api = new HdfsCompatApiScope(env, suite);
    }
    if (hasShellCase()) {
      shell = new HdfsCompatShellScope(env, suite);
    }
  }

  public HdfsCompatReport apply() throws Exception {
    HdfsCompatReport report = new HdfsCompatReport(uri.toString(), suite);
    if (api != null) {
      report.merge(api.apply());
    }
    if (shell != null) {
      report.merge(shell.apply());
    }
    return report;
  }

  private void initSuite() throws ReflectiveOperationException {
    Map<String, HdfsCompatSuite> defaultSuites = getDefaultSuites();
    this.suite = defaultSuites.getOrDefault(this.suiteName, null);
    if (this.suite != null) {
      return;
    }
    String key = "hadoop.compatibility.suite." + this.suiteName + ".classname";
    final String suiteClassName = conf.get(key, null);
    if ((suiteClassName == null) || suiteClassName.isEmpty()) {
      throw new HdfsCompatIllegalArgumentException(
          "cannot get class name for suite " + this.suiteName +
              ", configuration " + key + " is not properly set.");
    }
    Constructor<?> ctor = suiteClassName.getClass().getConstructor();
    ctor.setAccessible(true);
    Object suiteObj = ctor.newInstance();
    if (suiteObj instanceof HdfsCompatSuite) {
      this.suite = (HdfsCompatSuite) suiteObj;
    } else {
      throw new HdfsCompatIllegalArgumentException(
          "class name " + suiteClassName + " must be an" +
              " implementation of " + HdfsCompatSuite.class.getName());
    }
    if (suite.getSuiteName() == null || suite.getSuiteName().isEmpty()) {
      throw new HdfsCompatIllegalArgumentException(
          "suite " + suiteClassName + " suiteName is empty");
    }
    for (HdfsCompatSuite defaultSuite : defaultSuites.values()) {
      if (suite.getSuiteName().equalsIgnoreCase(defaultSuite.getSuiteName())) {
        throw new HdfsCompatIllegalArgumentException(
            "suite " + suiteClassName + " suiteName" +
                " conflicts with default suite " + defaultSuite.getSuiteName());
      }
    }
    if (!hasApiCase() && !hasShellCase()) {
      throw new HdfsCompatIllegalArgumentException(
          "suite " + suiteClassName + " is empty for both API and SHELL");
    }
  }

  private boolean hasApiCase() {
    return (suite.getApiCases() != null) &&
        (suite.getApiCases().length > 0);
  }

  private boolean hasShellCase() {
    return (suite.getShellCases() != null) &&
        (suite.getShellCases().length > 0);
  }

  @VisibleForTesting
  protected Map<String, HdfsCompatSuite> getDefaultSuites() {
    Map<String, HdfsCompatSuite> defaultSuites = new HashMap<>();
    defaultSuites.put("all", new HdfsCompatSuiteForAll());
    defaultSuites.put("shell", new HdfsCompatSuiteForShell());
    defaultSuites.put("tpcds", new HdfsCompatSuiteForTpcds());
    return defaultSuites;
  }
}