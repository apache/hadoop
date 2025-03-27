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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class HdfsCompatShellScope {
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsCompatShellScope.class);
  private static final Random RANDOM = new Random();
  private final HdfsCompatEnvironment env;
  private final HdfsCompatSuite suite;
  private File stdoutDir = null;
  private File passList = null;
  private File failList = null;
  private File skipList = null;
  private Path snapshotPath = null;
  private String storagePolicy = null;
  private Method disallowSnapshot = null;

  public HdfsCompatShellScope(HdfsCompatEnvironment env, HdfsCompatSuite suite) {
    this.env = env;
    this.suite = suite;
  }

  public HdfsCompatReport apply() throws Exception {
    File localTmpDir = null;
    try {
      localTmpDir = new File(this.env.getLocalTmpDir());
      LOG.info("Local tmp dir: " + localTmpDir.getAbsolutePath());
      return runShell(localTmpDir);
    } finally {
      try {
        if (this.disallowSnapshot != null) {
          try {
            this.disallowSnapshot.invoke(this.env.getFileSystem(),
                this.snapshotPath);
          } catch (InvocationTargetException e) {
            LOG.error("Cannot disallow snapshot", e.getCause());
          } catch (ReflectiveOperationException e) {
            LOG.error("Disallow snapshot method is invalid", e);
          }
        }
      } finally {
        FileUtils.deleteQuietly(localTmpDir);
      }
    }
  }

  private HdfsCompatReport runShell(File localTmpDir) throws Exception {
    File localDir = new File(localTmpDir, "test");
    File scriptDir = new File(localTmpDir, "scripts");
    File confDir = new File(localTmpDir, "hadoop-conf");
    copyScriptsResource(scriptDir);
    try {
      setShellLogConf(confDir);
    } catch (Exception e) {
      LOG.error("Cannot set new conf dir", e);
      confDir = null;
    }

    prepareSnapshot();
    this.storagePolicy = getStoragePolicy();
    String[] confEnv = getEnv(localDir, scriptDir, confDir);
    ExecResult result = exec(confEnv, scriptDir);
    printLog(result);
    return export();
  }

  private void copyScriptsResource(File scriptDir) throws IOException {
    Files.createDirectories(new File(scriptDir, "cases").toPath());
    copyResource("/misc.sh", new File(scriptDir, "misc.sh"));
    String[] cases = suite.getShellCases();
    for (String res : cases) {
      copyResource("/cases/" + res, new File(scriptDir, "cases/" + res));
    }
  }

  private void setShellLogConf(File confDir) throws IOException {
    final String hadoopHome = System.getenv("HADOOP_HOME");
    final String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    if ((hadoopHome == null) || hadoopHome.isEmpty()) {
      LOG.error("HADOOP_HOME not configured");
    }
    if ((hadoopConfDir == null) || hadoopConfDir.isEmpty()) {
      throw new IOException("HADOOP_CONF_DIR not configured");
    }
    File srcDir = new File(hadoopConfDir).getAbsoluteFile();
    if (!srcDir.isDirectory()) {
      throw new IOException("HADOOP_CONF_DIR is not valid: " + srcDir);
    }

    Files.createDirectories(confDir.toPath());
    FileUtils.copyDirectory(srcDir, confDir);
    File logConfFile = new File(confDir, "log4j.properties");
    copyResource("/hadoop-compat-bench-log4j.properties", logConfFile, true);
  }

  @VisibleForTesting
  protected void copyResource(String res, File dst) throws IOException {
    copyResource(res, dst, false);
  }

  private void copyResource(String res, File dst, boolean overwrite)
      throws IOException {
    InputStream in = null;
    try {
      in = this.getClass().getResourceAsStream(res);
      if (in == null) {
        in = this.suite.getClass().getResourceAsStream(res);
      }
      if (in == null) {
        throw new IOException("Resource not found" +
            " during scripts prepare: " + res);
      }

      if (dst.exists() && !overwrite) {
        throw new IOException("Cannot overwrite existing resource file");
      }

      Files.createDirectories(dst.getParentFile().toPath());

      byte[] buf = new byte[1024];
      try (OutputStream out = new FileOutputStream(dst)) {
        int nRead = in.read(buf);
        while (nRead != -1) {
          out.write(buf, 0, nRead);
          nRead = in.read(buf);
        }
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  private void prepareSnapshot() {
    this.snapshotPath = AbstractHdfsCompatCase.getUniquePath(this.env.getBase());
    Method allowSnapshot = null;
    try {
      FileSystem fs = this.env.getFileSystem();
      fs.mkdirs(snapshotPath);
      Method allowSnapshotMethod = fs.getClass()
          .getMethod("allowSnapshot", Path.class);
      allowSnapshotMethod.setAccessible(true);
      allowSnapshotMethod.invoke(fs, snapshotPath);
      allowSnapshot = allowSnapshotMethod;

      Method disallowSnapshotMethod = fs.getClass()
          .getMethod("disallowSnapshot", Path.class);
      disallowSnapshotMethod.setAccessible(true);
      this.disallowSnapshot = disallowSnapshotMethod;
    } catch (IOException e) {
      LOG.error("Cannot prepare snapshot path", e);
    } catch (InvocationTargetException e) {
      LOG.error("Cannot allow snapshot", e.getCause());
    } catch (ReflectiveOperationException e) {
      LOG.warn("Get admin snapshot methods failed.");
    } catch (Exception e) {
      LOG.warn("Prepare snapshot failed", e);
    }
    if (allowSnapshot == null) {
      LOG.warn("No allowSnapshot method found.");
    }
    if (this.disallowSnapshot == null) {
      LOG.warn("No disallowSnapshot method found.");
    }
  }

  private String getStoragePolicy() {
    BlockStoragePolicySpi def;
    String[] policies;
    try {
      FileSystem fs = this.env.getFileSystem();
      Path base = this.env.getBase();
      fs.mkdirs(base);
      def = fs.getStoragePolicy(base);
      policies = env.getStoragePolicyNames();
    } catch (Exception e) {
      LOG.warn("Cannot get storage policy", e);
      return "Hot";
    }

    List<String> differentPolicies = new ArrayList<>();
    for (String policyName : policies) {
      if ((def == null) || !policyName.equalsIgnoreCase(def.getName())) {
        differentPolicies.add(policyName);
      }
    }
    if (differentPolicies.isEmpty()) {
      final String defPolicyName;
      if ((def == null) || (def.getName() == null)) {
        defPolicyName = "Hot";
        LOG.warn("No valid storage policy name found, use Hot.");
      } else {
        defPolicyName = def.getName();
        LOG.warn("There is only one storage policy: " + defPolicyName);
      }
      return defPolicyName;
    } else {
      return differentPolicies.get(
          RANDOM.nextInt(differentPolicies.size()));
    }
  }

  @VisibleForTesting
  protected String[] getEnv(File localDir, File scriptDir, File confDir)
      throws IOException {
    List<String> confEnv = new ArrayList<>();
    final Map<String, String> environments = System.getenv();
    for (Map.Entry<String, String> entry : environments.entrySet()) {
      confEnv.add(entry.getKey() + "=" + entry.getValue());
    }
    if (confDir != null) {
      confEnv.add("HADOOP_CONF_DIR=" + confDir.getAbsolutePath());
    }

    String timestamp = String.valueOf(System.currentTimeMillis());
    Path baseUri = new Path(this.env.getBase(), timestamp);
    File localUri = new File(localDir, timestamp).getAbsoluteFile();
    File resultDir = new File(localDir, timestamp);
    Files.createDirectories(resultDir.toPath());
    this.stdoutDir = new File(resultDir, "output").getAbsoluteFile();
    this.passList = new File(resultDir, "passed").getAbsoluteFile();
    this.failList = new File(resultDir, "failed").getAbsoluteFile();
    this.skipList = new File(resultDir, "skipped").getAbsoluteFile();
    Files.createFile(this.passList.toPath());
    Files.createFile(this.failList.toPath());
    Files.createFile(this.skipList.toPath());

    final String prefix = "HADOOP_COMPAT_";
    confEnv.add(prefix + "BASE_URI=" + baseUri);
    confEnv.add(prefix + "LOCAL_URI=" + localUri.getAbsolutePath());
    confEnv.add(prefix + "SNAPSHOT_URI=" + snapshotPath.toString());
    confEnv.add(prefix + "STORAGE_POLICY=" + storagePolicy);
    confEnv.add(prefix + "STDOUT_DIR=" + stdoutDir.getAbsolutePath());
    confEnv.add(prefix + "PASS_FILE=" + passList.getAbsolutePath());
    confEnv.add(prefix + "FAIL_FILE=" + failList.getAbsolutePath());
    confEnv.add(prefix + "SKIP_FILE=" + skipList.getAbsolutePath());
    return confEnv.toArray(new String[0]);
  }

  private ExecResult exec(String[] confEnv, File scriptDir)
      throws IOException, InterruptedException {
    Process process = Runtime.getRuntime().exec(
        "prove -r cases", confEnv, scriptDir);
    StreamPrinter out = new StreamPrinter(process.getInputStream());
    StreamPrinter err = new StreamPrinter(process.getErrorStream());
    out.start();
    err.start();
    int code = process.waitFor();
    out.join();
    err.join();
    return new ExecResult(code, out.lines, err.lines);
  }

  private void printLog(ExecResult execResult) {
    LOG.info("Shell prove\ncode: {}\nstdout:\n\t{}\nstderr:\n\t{}",
        execResult.code, String.join("\n\t", execResult.out),
        String.join("\n\t", execResult.err));
    File casesRoot = new File(stdoutDir, "cases").getAbsoluteFile();
    String[] casesDirList = casesRoot.list();
    if (casesDirList == null) {
      LOG.error("stdout/stderr root directory is invalid: " + casesRoot);
      return;
    }
    Arrays.sort(casesDirList, (o1, o2) -> {
      if (o1.length() == o2.length()) {
        return o1.compareTo(o2);
      } else {
        return o1.length() - o2.length();
      }
    });
    for (String casesDir : casesDirList) {
      printCasesLog(new File(casesRoot, casesDir).getAbsoluteFile());
    }
  }

  private void printCasesLog(File casesDir) {
    File stdout = new File(casesDir, "stdout").getAbsoluteFile();
    File stderr = new File(casesDir, "stderr").getAbsoluteFile();
    File[] stdoutFiles = stdout.listFiles();
    File[] stderrFiles = stderr.listFiles();
    Set<String> cases = new HashSet<>();
    if (stdoutFiles != null) {
      for (File c : stdoutFiles) {
        cases.add(c.getName());
      }
    }
    if (stderrFiles != null) {
      for (File c : stderrFiles) {
        cases.add(c.getName());
      }
    }
    String[] caseNames = cases.stream().sorted((o1, o2) -> {
      if (o1.length() == o2.length()) {
        return o1.compareTo(o2);
      } else {
        return o1.length() - o2.length();
      }
    }).toArray(String[]::new);
    for (String caseName : caseNames) {
      File stdoutFile = new File(stdout, caseName);
      File stderrFile = new File(stderr, caseName);
      try {
        List<String> stdoutLines = stdoutFile.exists() ?
            readLines(stdoutFile) : new ArrayList<>();
        List<String> stderrLines = stderrFile.exists() ?
            readLines(stderrFile) : new ArrayList<>();
        LOG.info("Shell case {} - #{}\nstdout:\n\t{}\nstderr:\n\t{}",
            casesDir.getName(), caseName,
            String.join("\n\t", stdoutLines),
            String.join("\n\t", stderrLines));
      } catch (Exception e) {
        LOG.warn("Read shell stdout or stderr file failed", e);
      }
    }
  }

  private HdfsCompatReport export() throws IOException {
    HdfsCompatReport report = new HdfsCompatReport();
    report.addPassedCase(readLines(this.passList));
    report.addFailedCase(readLines(this.failList));
    report.addSkippedCase(readLines(this.skipList));
    return report;
  }

  private List<String> readLines(File file) throws IOException {
    List<String> lines = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(file), StandardCharsets.UTF_8))) {
      String line = br.readLine();
      while (line != null) {
        lines.add(line);
        line = br.readLine();
      }
    }
    return lines;
  }

  private static final class StreamPrinter extends Thread {
    private final InputStream in;
    private final List<String> lines;

    private StreamPrinter(InputStream in) {
      this.in = in;
      this.lines = new ArrayList<>();
    }

    @Override
    public void run() {
      try (BufferedReader br = new BufferedReader(
          new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String line = br.readLine();
        while (line != null) {
          this.lines.add(line);
          line = br.readLine();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static final class ExecResult {
    private final int code;
    private final List<String> out;
    private final List<String> err;

    private ExecResult(int code, List<String> out, List<String> err) {
      this.code = code;
      this.out = out;
      this.err = err;
    }
  }
}

