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
package org.apache.hadoop.fs.compat.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.compat.common.HdfsCompatCommand;
import org.apache.hadoop.fs.compat.common.HdfsCompatEnvironment;
import org.apache.hadoop.fs.compat.common.HdfsCompatShellScope;
import org.apache.hadoop.fs.compat.common.HdfsCompatSuite;

import java.io.IOException;
import java.lang.reflect.Field;

public class HdfsCompatTestCommand extends HdfsCompatCommand {
  public HdfsCompatTestCommand(String uri, String suiteName, Configuration conf) {
    super(uri, suiteName, conf);
  }

  @Override
  public void initialize() throws IOException, ReflectiveOperationException {
    super.initialize();
    Field shellField = HdfsCompatCommand.class.getDeclaredField("shell");
    shellField.setAccessible(true);
    HdfsCompatShellScope shell = (HdfsCompatShellScope) shellField.get(this);
    if (shell != null) {
      Field envField = shell.getClass().getDeclaredField("env");
      envField.setAccessible(true);
      HdfsCompatEnvironment env = (HdfsCompatEnvironment) envField.get(shell);
      Field suiteField = HdfsCompatCommand.class.getDeclaredField("suite");
      suiteField.setAccessible(true);
      HdfsCompatSuite suite = (HdfsCompatSuite) suiteField.get(this);
      shellField.set(this, getShellScope(env, suite));
    }
  }

  protected HdfsCompatShellScope getShellScope(HdfsCompatEnvironment env, HdfsCompatSuite suite) {
    return new HdfsCompatTestShellScope(env, suite);
  }
}