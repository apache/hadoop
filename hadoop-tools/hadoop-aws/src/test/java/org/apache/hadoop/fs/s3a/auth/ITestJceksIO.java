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

package org.apache.hadoop.fs.s3a.auth;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialShell;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH;
import static org.apache.hadoop.fs.s3a.Constants.S3A_SECURITY_CREDENTIAL_PROVIDER_PATH;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Test JCEKS file load/save on S3a.
 * Uses CredentialShell to better replicate the CLI.
 *
 * See HADOOP-17894.
 * This test is at risk of leaking FS instances in the JCEKS providers;
 * this is handled in an AfterClass operation.
 *
 */
public class ITestJceksIO extends AbstractS3ATestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestJceksIO.class);
  private static final String UTF8 = StandardCharsets.UTF_8.name();
  private PrintStream oldStdout, oldStderr;
  private ByteArrayOutputStream stdout, stderr;
  private PrintStream printStdout, printStderr;

  @Override
  public void setup() throws Exception {
    super.setup();
    oldStdout = System.out;
    oldStderr = System.err;
    stdout = new ByteArrayOutputStream();
    printStdout = new PrintStream(stdout);
    System.setOut(printStdout);

    stderr = new ByteArrayOutputStream();
    printStderr = new PrintStream(stderr);
    System.setErr(printStderr);
  }

  @Override
  public void teardown() throws Exception {
    System.setOut(oldStdout);
    System.setErr(oldStderr);
    IOUtils.cleanupWithLogger(LOG, printStdout, printStderr);
    super.teardown();
  }

  /**
   * Shut down all filesystems for this user to avoid
   * leaking those used by credential providers.
   */
  @AfterClass
  public static void closeAllFilesystems() {
    try {
      LOG.info("Closing down all filesystems for current user");
      FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
    } catch (IOException e) {
      LOG.warn("UGI.getCurrentUser()", e);
    }
  }

  /**
   * FS config with no providers. FS caching is disabled.
   * @return a new configuration.
   */
  private Configuration createNewConfiguration() {
    final Configuration conf = new Configuration(getConfiguration());
    removeBaseAndBucketOverrides(conf,
        HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH,
        S3A_SECURITY_CREDENTIAL_PROVIDER_PATH);
    disableFilesystemCaching(conf);
    return conf;

  }

  /*
   * List credentials; expect the file to be missing.
   * hadoop credential list -provider jceks://s3a@bucket/s3.jceks
   */
  @Test
  public void testListMissingJceksFile() throws Throwable {
    final Path dir = path("jceks");
    Path keystore = new Path(dir, "keystore.jceks");
    String jceksProvider = toJceksProvider(keystore);

    CredentialShell cs = new CredentialShell();

    cs.setConf(createNewConfiguration());
    run(cs, null,
        "list", "-provider", jceksProvider);
  }

  @Test
  public void testCredentialSuccessfulLifecycle() throws Exception {
    final Path dir = path("jceks");
    Path keystore = new Path(dir, "keystore.jceks");
    String jceksProvider = toJceksProvider(keystore);
    CredentialShell cs = new CredentialShell();
    cs.setConf(createNewConfiguration());
    run(cs, "credential1 has been successfully created.", "create", "credential1", "-value",
        "p@ssw0rd", "-provider",
        jceksProvider);

    assertIsFile(keystore);
    run(cs, "credential1",
        "list", "-provider", jceksProvider);

    run(cs, "credential1 has been successfully deleted.",
        "delete", "credential1", "-f", "-provider",
        jceksProvider);

    String[] args5 = {
        "list", "-provider",
        jceksProvider
    };
    String out = run(cs, null, args5);
    Assertions.assertThat(out)
        .describedAs("Command result of list")
        .doesNotContain("credential1");
  }

  private String run(CredentialShell cs, String expected, String... args)
      throws Exception {
    stdout.reset();
    int rc = cs.run(args);
    final String out = stdout.toString(UTF8);
    LOG.error("{}", stderr.toString(UTF8));
    LOG.info("{}", out);

    Assertions.assertThat(rc)
        .describedAs("Command result of %s with output %s",
            args[0], out)
        .isEqualTo(0);
    if (expected != null) {
      Assertions.assertThat(out)
          .describedAs("Command result of %s", args[0])
          .contains(expected);
    }
    return out;
  }

  /**
   * Convert a path to a jceks URI.
   * @param keystore store
   * @return string for the command line
   */
  private String toJceksProvider(Path keystore) {
    final URI uri = keystore.toUri();
    return String.format("jceks://%s@%s%s",
        uri.getScheme(), uri.getHost(), uri.getPath());
  }

}
