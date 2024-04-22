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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.compat.common.HdfsCompatEnvironment;
import org.apache.hadoop.fs.compat.common.HdfsCompatShellScope;
import org.apache.hadoop.fs.compat.common.HdfsCompatSuite;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HdfsCompatTestShellScope extends HdfsCompatShellScope {
  private final HdfsCompatEnvironment env;

  public HdfsCompatTestShellScope(HdfsCompatEnvironment env, HdfsCompatSuite suite) {
    super(env, suite);
    this.env = env;
  }

  @Override
  protected String[] getEnv(File localDir, File scriptDir, File confDir)
      throws IOException {
    replace(scriptDir);
    File binDir = new File(scriptDir, "bin");
    copyToBin(binDir);
    confDir = new File(scriptDir, "hadoop-conf-ut");
    writeConf(confDir);
    File logConfFile = new File(confDir, "log4j.properties");
    copyResource("/hadoop-compat-bench-log4j.properties", logConfFile);

    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" +
        File.separator + "java";
    String classpath = confDir.getAbsolutePath() + ":" +
        System.getProperty("java.class.path");
    String pathenv = System.getenv("PATH");
    if ((pathenv == null) || pathenv.isEmpty()) {
      pathenv = binDir.getAbsolutePath();
    } else {
      pathenv = binDir.getAbsolutePath() + ":" + pathenv;
    }

    List<String> confEnv = new ArrayList<>();
    Collections.addAll(confEnv, super.getEnv(localDir, scriptDir, confDir));
    confEnv.add("HADOOP_COMPAT_JAVA_BIN=" + javaBin);
    confEnv.add("HADOOP_COMPAT_JAVA_CLASSPATH=" + classpath);
    confEnv.add("HADOOP_CONF_DIR=" + confDir.getAbsolutePath());
    confEnv.add("PATH=" + pathenv);
    return confEnv.toArray(new String[0]);
  }

  @VisibleForTesting
  protected void replace(File scriptDir) throws IOException {
  }

  private void copyToBin(File binDir) throws IOException {
    Files.createDirectories(binDir.toPath());
    File hadoop = new File(binDir, "hadoop");
    File hdfs = new File(binDir, "hdfs");
    copyResource("/hadoop-compat-bench-test-shell-hadoop.sh", hadoop);
    copyResource("/hadoop-compat-bench-test-shell-hdfs.sh", hdfs);
    if (!hadoop.setReadable(true, false) ||
        !hadoop.setWritable(true, false) ||
        !hadoop.setExecutable(true, false)) {
      throw new IOException("No permission to hadoop shell.");
    }
    if (!hdfs.setReadable(true, false) ||
        !hdfs.setWritable(true, false) ||
        !hdfs.setExecutable(true, false)) {
      throw new IOException("No permission to hdfs shell.");
    }
  }

  private void writeConf(File confDir) throws IOException {
    Files.createDirectories(confDir.toPath());
    if (!confDir.setReadable(true, false) ||
        !confDir.setWritable(true, false) ||
        !confDir.setExecutable(true, false)) {
      throw new IOException("No permission to conf dir.");
    }
    File confFile = new File(confDir, "core-site.xml");
    try (OutputStream out = new FileOutputStream(confFile)) {
      this.env.getFileSystem().getConf().writeXml(out);
    }
    if (!confFile.setReadable(true, false) ||
        !confFile.setWritable(true, false) ||
        !confFile.setExecutable(true, false)) {
      throw new IOException("No permission to conf file.");
    }
  }
}