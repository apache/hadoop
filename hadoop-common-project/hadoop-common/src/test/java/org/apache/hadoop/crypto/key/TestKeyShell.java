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
package org.apache.hadoop.crypto.key;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestKeyShell {
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

  private PrintStream initialStdOut;
  private PrintStream initialStdErr;

  /* The default JCEKS provider - for testing purposes */
  private String jceksProvider;

  @Before
  public void setup() throws Exception {
    outContent.reset();
    errContent.reset();
    final File tmpDir = GenericTestUtils.getTestDir(UUID.randomUUID()
        .toString());
    if (!tmpDir.mkdirs()) {
      throw new IOException("Unable to create " + tmpDir);
    }
    final Path jksPath = new Path(tmpDir.toString(), "keystore.jceks");
    jceksProvider = "jceks://file" + jksPath.toUri();
    initialStdOut = System.out;
    initialStdErr = System.err;
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @After
  public void cleanUp() throws Exception {
    System.setOut(initialStdOut);
    System.setErr(initialStdErr);
  }

  /**
   * Delete a key from the default jceksProvider
   * @param ks The KeyShell instance
   * @param keyName The key to delete
   * @throws Exception
   */
  private void deleteKey(KeyShell ks, String keyName) throws Exception {
    int rc;
    outContent.reset();
    final String[] delArgs =
        {"delete", keyName, "-f", "-provider", jceksProvider};
    rc = ks.run(delArgs);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains(keyName + " has been " +
            "successfully deleted."));
  }

  /**
   * Lists the keys in the jceksProvider
   * @param ks The KeyShell instance
   * @param wantMetadata True if you want metadata returned with the keys
   * @return The output from the "list" call
   * @throws Exception
   */
  private String listKeys(KeyShell ks, boolean wantMetadata) throws Exception {
    int rc;
    outContent.reset();
    final String[] listArgs = {"list", "-provider", jceksProvider };
    final String[] listArgsM = {"list", "-metadata", "-provider", jceksProvider };
    rc = ks.run(wantMetadata ? listArgsM : listArgs);
    assertEquals(0, rc);
    return outContent.toString();
  }

  @Test
  public void testKeySuccessfulKeyLifecycle() throws Exception {
    int rc = 0;
    String keyName = "key1";

    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());

    outContent.reset();
    final String[] args1 = {"create", keyName, "-provider", jceksProvider};
    rc = ks.run(args1);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains(keyName + " has been " +
            "successfully created"));
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_WARN));
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_INSTRUCTIONS_DOC));
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_CONT));

    String listOut = listKeys(ks, false);
    assertTrue(listOut.contains(keyName));

    listOut = listKeys(ks, true);
    assertTrue(listOut.contains(keyName));
    assertTrue(listOut.contains("description"));
    assertTrue(listOut.contains("created"));

    outContent.reset();
    final String[] args2 = {"roll", keyName, "-provider", jceksProvider};
    rc = ks.run(args2);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("key1 has been successfully " +
        "rolled."));

    // jceks provider's invalidate is a no-op.
    outContent.reset();
    final String[] args3 =
        {"invalidateCache", keyName, "-provider", jceksProvider};
    rc = ks.run(args3);
    assertEquals(0, rc);
    assertTrue(outContent.toString()
        .contains("key1 has been successfully " + "invalidated."));

    deleteKey(ks, keyName);

    listOut = listKeys(ks, false);
    assertFalse(listOut, listOut.contains(keyName));
  }
  
  /* HADOOP-10586 KeyShell didn't allow -description. */
  @Test
  public void testKeySuccessfulCreationWithDescription() throws Exception {
    outContent.reset();
    final String[] args1 = {"create", "key1", "-provider", jceksProvider,
                      "-description", "someDescription"};
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("key1 has been successfully " +
        "created"));

    String listOut = listKeys(ks, true);
    assertTrue(listOut.contains("description"));
    assertTrue(listOut.contains("someDescription"));
  }

  @Test
  public void testInvalidKeySize() throws Exception {
    final String[] args1 = {"create", "key1", "-size", "56", "-provider",
            jceksProvider};

    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(1, rc);
    assertTrue(outContent.toString().contains("key1 has not been created."));
  }

  @Test
  public void testInvalidCipher() throws Exception {
    final String[] args1 = {"create", "key1", "-cipher", "LJM", "-provider",
            jceksProvider};

    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(1, rc);
    assertTrue(outContent.toString().contains("key1 has not been created."));
  }

  @Test
  public void testInvalidProvider() throws Exception {
    final String[] args1 = {"create", "key1", "-cipher", "AES", "-provider",
      "sdff://file/tmp/keystore.jceks"};
    
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(1, rc);
    assertTrue(outContent.toString().contains(KeyShell.NO_VALID_PROVIDERS));
  }

  @Test
  public void testTransientProviderWarning() throws Exception {
    final String[] args1 = {"create", "key1", "-cipher", "AES", "-provider",
      "user:///"};
    
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("WARNING: you are modifying a " +
        "transient provider."));
  }
  
  @Test
  public void testTransientProviderOnlyConfig() throws Exception {
    final String[] args1 = {"create", "key1"};
    
    int rc = 0;
    KeyShell ks = new KeyShell();
    Configuration config = new Configuration();
    config.set(KeyProviderFactory.KEY_PROVIDER_PATH, "user:///");
    ks.setConf(config);
    rc = ks.run(args1);
    assertEquals(1, rc);
    assertTrue(outContent.toString().contains(KeyShell.NO_VALID_PROVIDERS));
  }

  @Test
  public void testStrict() throws Exception {
    outContent.reset();
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    final String[] args1 = {"create", "hello", "-provider", jceksProvider,
        "-strict"};
    rc = ks.run(args1);
    assertEquals(1, rc);
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_ERROR));
    assertTrue(outContent.toString()
        .contains(ProviderUtils.NO_PASSWORD_INSTRUCTIONS_DOC));
  }

  @Test
  public void testFullCipher() throws Exception {
    final String keyName = "key1";
    final String[] args1 = {"create", keyName, "-cipher", "AES/CBC/pkcs5Padding",
        "-provider", jceksProvider};
    
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains(keyName + " has been " +
            "successfully created"));

    deleteKey(ks, keyName);
  }

  @Test
  public void testAttributes() throws Exception {
    int rc;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());

    /* Simple creation test */
    final String[] args1 = {"create", "keyattr1", "-provider", jceksProvider,
            "-attr", "foo=bar"};
    rc = ks.run(args1);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("keyattr1 has been " +
            "successfully created"));

    /* ...and list to see that we have the attr */
    String listOut = listKeys(ks, true);
    assertTrue(listOut.contains("keyattr1"));
    assertTrue(listOut.contains("attributes: [foo=bar]"));

    /* Negative tests: no attribute */
    outContent.reset();
    final String[] args2 = {"create", "keyattr2", "-provider", jceksProvider,
            "-attr", "=bar"};
    rc = ks.run(args2);
    assertEquals(1, rc);

    /* Not in attribute = value form */
    outContent.reset();
    args2[5] = "foo";
    rc = ks.run(args2);
    assertEquals(1, rc);

    /* No attribute or value */
    outContent.reset();
    args2[5] = "=";
    rc = ks.run(args2);
    assertEquals(1, rc);

    /* Legal: attribute is a, value is b=c */
    outContent.reset();
    args2[5] = "a=b=c";
    rc = ks.run(args2);
    assertEquals(0, rc);

    listOut = listKeys(ks, true);
    assertTrue(listOut.contains("keyattr2"));
    assertTrue(listOut.contains("attributes: [a=b=c]"));

    /* Test several attrs together... */
    outContent.reset();
    final String[] args3 = {"create", "keyattr3", "-provider", jceksProvider,
            "-attr", "foo = bar",
            "-attr", " glarch =baz  ",
            "-attr", "abc=def"};
    rc = ks.run(args3);
    assertEquals(0, rc);

    /* ...and list to ensure they're there. */
    listOut = listKeys(ks, true);
    assertTrue(listOut.contains("keyattr3"));
    assertTrue(listOut.contains("[foo=bar]"));
    assertTrue(listOut.contains("[glarch=baz]"));
    assertTrue(listOut.contains("[abc=def]"));

    /* Negative test - repeated attributes should fail */
    outContent.reset();
    final String[] args4 = {"create", "keyattr4", "-provider", jceksProvider,
            "-attr", "foo=bar",
            "-attr", "foo=glarch"};
    rc = ks.run(args4);
    assertEquals(1, rc);

    /* Clean up to be a good citizen */
    deleteKey(ks, "keyattr1");
    deleteKey(ks, "keyattr2");
    deleteKey(ks, "keyattr3");
  }
}
