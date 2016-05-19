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
package org.apache.hadoop.security.alias;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;

public class TestCredShell {
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private static final File tmpDir = GenericTestUtils.getTestDir("creds");

  /* The default JCEKS provider - for testing purposes */
  private String jceksProvider;

  @Before
  public void setup() throws Exception {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
    final Path jksPath = new Path(tmpDir.toString(), "keystore.jceks");
    new File(jksPath.toString()).delete();
    jceksProvider = "jceks://file" + jksPath.toUri();
  }
  
  @Test
  public void testCredentialSuccessfulLifecycle() throws Exception {
    outContent.reset();
    String[] args1 = {"create", "credential1", "-value", "p@ssw0rd", "-provider",
        jceksProvider};
    int rc = 0;
    CredentialShell cs = new CredentialShell();
    cs.setConf(new Configuration());
    rc = cs.run(args1);
    assertEquals(outContent.toString(), 0, rc);
    assertTrue(outContent.toString().contains("credential1 has been successfully " +
    		"created."));
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_WARN));
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_INSTRUCTIONS_DOC));
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_CONT));

    outContent.reset();
    String[] args2 = {"list", "-provider",
        jceksProvider};
    rc = cs.run(args2);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("credential1"));

    outContent.reset();
    String[] args4 = {"delete", "credential1", "-f", "-provider",
        jceksProvider};
    rc = cs.run(args4);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("credential1 has been successfully " +
    		"deleted."));

    outContent.reset();
    String[] args5 = {"list", "-provider",
        jceksProvider};
    rc = cs.run(args5);
    assertEquals(0, rc);
    assertFalse(outContent.toString(), outContent.toString().contains("credential1"));
  }

  @Test
  public void testInvalidProvider() throws Exception {
    String[] args1 = {"create", "credential1", "-value", "p@ssw0rd", "-provider",
      "sdff://file/tmp/credstore.jceks"};
    
    int rc = 0;
    CredentialShell cs = new CredentialShell();
    cs.setConf(new Configuration());
    rc = cs.run(args1);
    assertEquals(1, rc);
    assertTrue(outContent.toString().contains(
        CredentialShell.NO_VALID_PROVIDERS));
  }

  @Test
  public void testTransientProviderWarning() throws Exception {
    String[] args1 = {"create", "credential1", "-value", "p@ssw0rd", "-provider",
      "user:///"};
    
    int rc = 0;
    CredentialShell cs = new CredentialShell();
    cs.setConf(new Configuration());
    rc = cs.run(args1);
    assertEquals(outContent.toString(), 0, rc);
    assertTrue(outContent.toString().contains("WARNING: you are modifying a " +
    		"transient provider."));

    String[] args2 = {"delete", "credential1", "-f", "-provider", "user:///"};
    rc = cs.run(args2);
    assertEquals(outContent.toString(), 0, rc);
    assertTrue(outContent.toString().contains("credential1 has been successfully " +
        "deleted."));
  }
  
  @Test
  public void testTransientProviderOnlyConfig() throws Exception {
    String[] args1 = {"create", "credential1"};
    
    int rc = 0;
    CredentialShell cs = new CredentialShell();
    Configuration config = new Configuration();
    config.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, "user:///");
    cs.setConf(config);
    rc = cs.run(args1);
    assertEquals(1, rc);
    assertTrue(outContent.toString().contains(
        CredentialShell.NO_VALID_PROVIDERS));
  }
  
  @Test
  public void testPromptForCredentialWithEmptyPasswd() throws Exception {
    String[] args1 = {"create", "credential1", "-provider",
        jceksProvider};
    ArrayList<String> passwords = new ArrayList<String>();
    passwords.add(null);
    passwords.add("p@ssw0rd");
    int rc = 0;
    CredentialShell shell = new CredentialShell();
    shell.setConf(new Configuration());
    shell.setPasswordReader(new MockPasswordReader(passwords));
    rc = shell.run(args1);
    assertEquals(outContent.toString(), 1, rc);
    assertTrue(outContent.toString().contains("Passwords don't match"));
  }

  @Test
  public void testPromptForCredential() throws Exception {
    String[] args1 = {"create", "credential1", "-provider",
        jceksProvider};
    ArrayList<String> passwords = new ArrayList<String>();
    passwords.add("p@ssw0rd");
    passwords.add("p@ssw0rd");
    int rc = 0;
    CredentialShell shell = new CredentialShell();
    shell.setConf(new Configuration());
    shell.setPasswordReader(new MockPasswordReader(passwords));
    rc = shell.run(args1);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("credential1 has been successfully " +
        "created."));
    
    String[] args2 = {"delete", "credential1", "-f", "-provider",
        jceksProvider};
    rc = shell.run(args2);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("credential1 has been successfully " +
        "deleted."));
  }
  
  public class MockPasswordReader extends CredentialShell.PasswordReader {
    List<String> passwords = null;
    
    public MockPasswordReader(List<String> passwds) {
      passwords = passwds;
    }

    @Override
    public char[] readPassword(String prompt) {
      if (passwords.size() == 0) return null;
      String pass = passwords.remove(0);
      return pass == null ? null : pass.toCharArray();
    }

    @Override
    public void format(String message) {
      System.out.println(message);
    }
  }

  @Test
  public void testEmptyArgList() throws Exception {
    CredentialShell shell = new CredentialShell();
    shell.setConf(new Configuration());
    assertEquals(1, shell.init(new String[0]));
  }

  @Test
  public void testCommandHelpExitsNormally() throws Exception {
    for (String cmd : Arrays.asList("create", "list", "delete")) {
      CredentialShell shell = new CredentialShell();
      shell.setConf(new Configuration());
      assertEquals("Expected help argument on " + cmd + " to return 0",
              0, shell.init(new String[] {cmd, "-help"}));
    }
  }

  @Test
  public void testEmptyArgForCommands() throws Exception {
    CredentialShell shell = new CredentialShell();
    String[] command = { "list", "-provider" };
    assertEquals("Expected empty argument on " + command + " to return 1", 1,
        shell.init(command));

    for (String cmd : Arrays.asList("create", "delete")) {
      shell.setConf(new Configuration());
      assertEquals("Expected empty argument on " + cmd + " to return 1", 1,
          shell.init(new String[] { cmd }));
    }
  }

  @Test
  public void testStrict() throws Exception {
    outContent.reset();
    String[] args1 = {"create", "credential1", "-value", "p@ssw0rd",
        "-provider", jceksProvider, "-strict"};
    int rc = 1;
    CredentialShell cs = new CredentialShell();
    cs.setConf(new Configuration());
    rc = cs.run(args1);
    assertEquals(outContent.toString(), 1, rc);
    assertFalse(outContent.toString().contains("credential1 has been " +
        "successfully created."));
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_ERROR));
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_INSTRUCTIONS_DOC));
  }

  @Test
  public void testHelp() throws Exception {
    outContent.reset();
    String[] args1 = {"-help"};
    int rc = 0;
    CredentialShell cs = new CredentialShell();
    cs.setConf(new Configuration());
    rc = cs.run(args1);
    assertEquals(outContent.toString(), 0, rc);
    assertTrue(outContent.toString().contains("Usage"));
  }

  @Test
  public void testHelpCreate() throws Exception {
    outContent.reset();
    String[] args1 = {"create", "-help"};
    int rc = 0;
    CredentialShell cs = new CredentialShell();
    cs.setConf(new Configuration());
    rc = cs.run(args1);
    assertEquals(outContent.toString(), 0, rc);
    assertTrue(outContent.toString().contains("Usage"));
  }
}
