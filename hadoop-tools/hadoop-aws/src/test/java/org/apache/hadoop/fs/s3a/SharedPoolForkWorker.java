/*
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

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * Measures scheduler threads in a fresh JVM, independently of test-runner fork reuse.
 */
public final class SharedPoolForkWorker {

  private static final String CONFIG_FILE = "configuration.xml";
  private static final String PROPERTIES_FILE = "system.properties";
  private static final String RESULT_PREFIX = "SCHEDULER_THREAD_COUNT=";
  private static final Pattern RESULT_PATTERN =
      Pattern.compile("^" + RESULT_PREFIX + "([0-9]+)$", Pattern.MULTILINE);

  private SharedPoolForkWorker() {
  }

  /**
   * Run a measurement with fresh static state and the parent test's configuration.
   * @param uri filesystem URI
   * @param conf effective test configuration
   * @param instances number of filesystem instances to create and abandon
   * @param timeoutMillis maximum child runtime
   * @param prefixes scheduler thread name prefixes to count
   * @return the child's scheduler thread count
   * @throws IOException child execution or test-context transfer failed
   */
  static int countThreads(URI uri, Configuration conf, int instances,
      long timeoutMillis, String... prefixes) throws IOException {
    java.nio.file.Path directory = Files.createTempDirectory("s3a-shared-pool-");
    java.nio.file.Path configurationFile = directory.resolve(CONFIG_FILE);
    java.nio.file.Path propertiesFile = directory.resolve(PROPERTIES_FILE);
    try {
      try (OutputStream out = Files.newOutputStream(configurationFile)) {
        conf.writeXml(out);
      }
      try (OutputStream out = Files.newOutputStream(propertiesFile)) {
        System.getProperties().store(out, null);
      }
      List<String> command = new ArrayList<>();
      command.add(new File(new File(System.getProperty("java.home"), "bin"),
          Shell.WINDOWS ? "java.exe" : "java").getPath());
      command.add("-cp");
      command.add(System.getProperty("surefire.test.class.path",
          System.getProperty("java.class.path")));
      command.add(SharedPoolForkWorker.class.getName());
      command.add(directory.toString());
      command.add(uri.toString());
      command.add(Integer.toString(instances));
      command.addAll(Arrays.asList(prefixes));
      ShellCommandExecutor executor = new ShellCommandExecutor(
          command.toArray(new String[0]), null, null, timeoutMillis);
      executor.execute();
      Matcher result = RESULT_PATTERN.matcher(executor.getOutput());
      if (!result.find()) {
        throw new IOException("Child JVM did not report a scheduler thread count:\n"
            + executor.getOutput());
      }
      return Integer.parseInt(result.group(1));
    } finally {
      Files.deleteIfExists(configurationFile);
      Files.deleteIfExists(propertiesFile);
      Files.deleteIfExists(directory);
    }
  }

  /**
   * Child entry point: restore the test context, measure and exit.
   * @param args context directory, filesystem URI, instance count and thread prefixes
   */
  public static void main(String[] args) {
    int exitCode = 0;
    try {
      File directory = new File(args[0]);
      Properties properties = new Properties();
      try (InputStream in = Files.newInputStream(
          directory.toPath().resolve(PROPERTIES_FILE))) {
        properties.load(in);
      }
      System.getProperties().putAll(properties);
      Configuration conf = new Configuration(false);
      conf.addResource(new File(directory, CONFIG_FILE).toURI().toURL());
      int count = S3ATestUtils.countSchedulerThreadsAfterAbandoningFilesystems(
          URI.create(args[1]), conf, Integer.parseInt(args[2]),
          Arrays.copyOfRange(args, 3, args.length));
      System.out.println(RESULT_PREFIX + count);
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      exitCode = 1;
    }
    // The measurement deliberately leaves clients and their threads running.
    ExitUtil.terminate(exitCode);
  }
}
