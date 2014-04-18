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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestKeyShell {
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

  private static File tmpDir;

  private PrintStream initialStdOut;
  private PrintStream initialStdErr;

  @Before
  public void setup() throws Exception {
    outContent.reset();
    errContent.reset();
    tmpDir = new File(System.getProperty("test.build.data", "target"),
        UUID.randomUUID().toString());
    tmpDir.mkdirs();
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

  @Test
  public void testKeySuccessfulKeyLifecycle() throws Exception {
    outContent.reset();
    String[] args1 = {"create", "key1", "--provider", 
        "jceks://file" + tmpDir + "/keystore.jceks"};
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("key1 has been successfully " +
    		"created."));

    outContent.reset();
    String[] args2 = {"list", "--provider",
        "jceks://file" + tmpDir + "/keystore.jceks"};
    rc = ks.run(args2);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("key1"));

    outContent.reset();
    String[] args2a = {"list", "--metadata", "--provider",
                      "jceks://file" + tmpDir + "/keystore.jceks"};
    rc = ks.run(args2a);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("key1"));
    assertTrue(outContent.toString().contains("description"));
    assertTrue(outContent.toString().contains("created"));

    outContent.reset();
    String[] args3 = {"roll", "key1", "--provider", 
        "jceks://file" + tmpDir + "/keystore.jceks"};
    rc = ks.run(args3);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("key1 has been successfully " +
    		"rolled."));

    outContent.reset();
    String[] args4 = {"delete", "key1", "--provider", 
        "jceks://file" + tmpDir + "/keystore.jceks"};
    rc = ks.run(args4);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("key1 has been successfully " +
    		"deleted."));

    outContent.reset();
    String[] args5 = {"list", "--provider", 
        "jceks://file" + tmpDir + "/keystore.jceks"};
    rc = ks.run(args5);
    assertEquals(0, rc);
    assertFalse(outContent.toString(), outContent.toString().contains("key1"));
  }
  
  @Test
  public void testInvalidKeySize() throws Exception {
    String[] args1 = {"create", "key1", "--size", "56", "--provider", 
        "jceks://file" + tmpDir + "/keystore.jceks"};
    
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(-1, rc);
    assertTrue(outContent.toString().contains("key1 has NOT been created."));
  }

  @Test
  public void testInvalidCipher() throws Exception {
    String[] args1 = {"create", "key1", "--cipher", "LJM", "--provider", 
        "jceks://file" + tmpDir + "/keystore.jceks"};
    
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(-1, rc);
    assertTrue(outContent.toString().contains("key1 has NOT been created."));
  }

  @Test
  public void testInvalidProvider() throws Exception {
    String[] args1 = {"create", "key1", "--cipher", "AES", "--provider", 
      "sdff://file/tmp/keystore.jceks"};
    
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(-1, rc);
    assertTrue(outContent.toString().contains("There are no valid " +
    		"KeyProviders configured."));
  }

  @Test
  public void testTransientProviderWarning() throws Exception {
    String[] args1 = {"create", "key1", "--cipher", "AES", "--provider", 
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
    String[] args1 = {"create", "key1"};
    
    int rc = 0;
    KeyShell ks = new KeyShell();
    Configuration config = new Configuration();
    config.set(KeyProviderFactory.KEY_PROVIDER_PATH, "user:///");
    ks.setConf(config);
    rc = ks.run(args1);
    assertEquals(-1, rc);
    assertTrue(outContent.toString().contains("There are no valid " +
    		"KeyProviders configured."));
  }

  @Test
  public void testFullCipher() throws Exception {
    String[] args1 = {"create", "key1", "--cipher", "AES/CBC/pkcs5Padding", 
        "--provider", "jceks://file" + tmpDir + "/keystore.jceks"};
    
    int rc = 0;
    KeyShell ks = new KeyShell();
    ks.setConf(new Configuration());
    rc = ks.run(args1);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("key1 has been successfully " +
    		"created."));

    outContent.reset();
    String[] args2 = {"delete", "key1", "--provider", 
        "jceks://file" + tmpDir + "/keystore.jceks"};
    rc = ks.run(args2);
    assertEquals(0, rc);
    assertTrue(outContent.toString().contains("key1 has been successfully " +
    		"deleted."));
  }
}
