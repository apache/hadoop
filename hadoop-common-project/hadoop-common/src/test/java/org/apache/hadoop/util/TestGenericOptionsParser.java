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
package org.apache.hadoop.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestGenericOptionsParser {
  File testDir;
  Configuration conf;
  FileSystem localFs;

  @Test
  public void testFilesOption() throws Exception {
    File tmpFile = new File(testDir, "tmpfile");
    Path tmpPath = new Path(tmpFile.toString());
    localFs.create(tmpPath);
    String[] args = new String[2];
    // pass a files option 
    args[0] = "-files";
    // Convert a file to a URI as File.toString() is not a valid URI on
    // all platforms and GenericOptionsParser accepts only valid URIs
    args[1] = tmpFile.toURI().toString();
    new GenericOptionsParser(conf, args);
    String files = conf.get("tmpfiles");
    assertNotNull("files is null", files);
    assertEquals("files option does not match",
      localFs.makeQualified(tmpPath).toString(), files);
    
    // pass file as uri
    Configuration conf1 = new Configuration();
    URI tmpURI = new URI(tmpFile.toURI().toString() + "#link");
    args[0] = "-files";
    args[1] = tmpURI.toString();
    new GenericOptionsParser(conf1, args);
    files = conf1.get("tmpfiles");
    assertNotNull("files is null", files);
    assertEquals("files option does not match", 
      localFs.makeQualified(new Path(tmpURI)).toString(), files);
   
    // pass a file that does not exist.
    // GenericOptionParser should throw exception
    Configuration conf2 = new Configuration();
    args[0] = "-files";
    args[1] = "file:///xyz.txt";
    Throwable th = null;
    try {
      new GenericOptionsParser(conf2, args);
    } catch (Exception e) {
      th = e;
    }
    assertNotNull("throwable is null", th);
    assertTrue("FileNotFoundException is not thrown",
      th instanceof FileNotFoundException);
    files = conf2.get("tmpfiles");
    assertNull("files is not null", files);
  }

  @Test
  public void testLibjarsOption() throws Exception {
    File tmpJar = new File(testDir, "tmp.jar");
    Path tmpJarPath = new Path(tmpJar.toString());
    localFs.create(tmpJarPath);
    String[] args = new String[2];
    // pass a libjars option
    // first, pass the jar directly
    args[0] = "-libjars";
    // Convert a file to a URI as File.toString() is not a valid URI on
    // all platforms and GenericOptionsParser accepts only valid URIs
    args[1] = tmpJar.toURI().toString();
    new GenericOptionsParser(conf, args);
    String libjars = conf.get("tmpjars");
    assertNotNull("libjars is null", libjars);
    assertEquals("libjars does not match",
        localFs.makeQualified(tmpJarPath).toString(), libjars);

    // now test the wildcard
    args[1] = testDir.toURI().toString() + "*";
    new GenericOptionsParser(conf, args);
    libjars = conf.get("tmpjars");
    assertNotNull("libjars is null", libjars);
    assertEquals("libjars does not match",
        localFs.makeQualified(tmpJarPath).toString(), libjars);
  }

  /**
   * Test the case where the libjars, files and archives arguments
   * contains an empty token, which should create an IllegalArgumentException.
   */
  @Test
  public void testEmptyFilenames() throws Exception {
    List<Pair<String, String>> argsAndConfNames = new ArrayList<Pair<String, String>>();
    argsAndConfNames.add(new Pair<String, String>("-libjars", "tmpjars"));
    argsAndConfNames.add(new Pair<String, String>("-files", "tmpfiles"));
    argsAndConfNames.add(new Pair<String, String>("-archives", "tmparchives"));
    for (Pair<String, String> argAndConfName : argsAndConfNames) {
      String arg = argAndConfName.getFirst();

      File tmpFileOne = new File(testDir, "tmpfile1");
      Path tmpPathOne = new Path(tmpFileOne.toString());
      File tmpFileTwo = new File(testDir, "tmpfile2");
      Path tmpPathTwo = new Path(tmpFileTwo.toString());
      localFs.create(tmpPathOne);
      localFs.create(tmpPathTwo);
      String[] args = new String[2];
      args[0] = arg;
      // create an empty path in between two valid files,
      // which prior to HADOOP-10820 used to result in the
      // working directory being added to "tmpjars" (or equivalent)
      args[1] = String.format("%s,,%s",
          tmpFileOne.toURI().toString(), tmpFileTwo.toURI().toString());
      try {
        new GenericOptionsParser(conf, args);
        fail("Expected exception for empty filename");
      } catch (IllegalArgumentException e) {
        // expect to receive an IllegalArgumentException
        GenericTestUtils.assertExceptionContains("File name can't be"
            + " empty string", e);
      }

      // test zero file list length - it should create an exception
      args[1] = ",,";
      try {
        new GenericOptionsParser(conf, args);
        fail("Expected exception for zero file list length");
      } catch (IllegalArgumentException e) {
        // expect to receive an IllegalArgumentException
        GenericTestUtils.assertExceptionContains("File name can't be"
            + " empty string", e);
      }

      // test filename with space character
      // it should create exception from parser in URI class
      // due to URI syntax error
      args[1] = String.format("%s, ,%s",
          tmpFileOne.toURI().toString(), tmpFileTwo.toURI().toString());
      try {
        new GenericOptionsParser(conf, args);
        fail("Expected exception for filename with space character");
      } catch (IllegalArgumentException e) {
        // expect to receive an IllegalArgumentException
        GenericTestUtils.assertExceptionContains("URISyntaxException", e);
      }
    }
  }

  /**
   * Test that options passed to the constructor are used.
   */
  @SuppressWarnings("static-access")
  @Test
  public void testCreateWithOptions() throws Exception {
    // Create new option newOpt
    Option opt = OptionBuilder.withArgName("int")
    .hasArg()
    .withDescription("A new option")
    .create("newOpt");
    Options opts = new Options();
    opts.addOption(opt);

    // Check newOpt is actually used to parse the args
    String[] args = new String[2];
    args[0] = "--newOpt";
    args[1] = "7";
    GenericOptionsParser g = new GenericOptionsParser(opts, args);
    assertEquals("New option was ignored",
      "7", g.getCommandLine().getOptionValues("newOpt")[0]);
  }

  /**
   * Test that multiple conf arguments can be used.
   */
  @Test
  public void testConfWithMultipleOpts() throws Exception {
    String[] args = new String[2];
    args[0] = "--conf=foo";
    args[1] = "--conf=bar";
    GenericOptionsParser g = new GenericOptionsParser(args);
    assertEquals("1st conf param is incorrect",
      "foo", g.getCommandLine().getOptionValues("conf")[0]);
    assertEquals("2st conf param is incorrect",
      "bar", g.getCommandLine().getOptionValues("conf")[1]);
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    localFs = FileSystem.getLocal(conf);
    testDir = GenericTestUtils.getTestDir("generic");
    if(testDir.exists())
      localFs.delete(new Path(testDir.toString()), true);
  }

  @After
  public void tearDown() throws Exception {
    if(testDir.exists()) {
      localFs.delete(new Path(testDir.toString()), true);
    }
  }

  /**
   * testing -fileCache option
   * @throws IOException
   */
  @Test
  public void testTokenCacheOption() throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    
    File tmpFile = new File(testDir, "tokenCacheFile");
    if(tmpFile.exists()) {
      tmpFile.delete();
    }
    String[] args = new String[2];
    // pass a files option 
    args[0] = "-tokenCacheFile";
    args[1] = tmpFile.toURI().toString();
    
    // test non existing file
    Throwable th = null;
    try {
      new GenericOptionsParser(conf, args);
    } catch (Exception e) {
      th = e;
    }
    assertNotNull(th);
    assertTrue("FileNotFoundException is not thrown",
        th instanceof FileNotFoundException);
    
    // create file
    Path tmpPath = localFs.makeQualified(new Path(tmpFile.toString()));
    Token<?> token = new Token<AbstractDelegationTokenIdentifier>(
        "identifier".getBytes(), "password".getBytes(),
        new Text("token-kind"), new Text("token-service"));
    Credentials creds = new Credentials();
    creds.addToken(new Text("token-alias"), token);
    creds.writeTokenStorageFile(tmpPath, conf);

    new GenericOptionsParser(conf, args);
    String fileName = conf.get("mapreduce.job.credentials.binary");
    assertNotNull("files is null", fileName);
    assertEquals("files option does not match", tmpPath.toString(), fileName);
    
    Credentials ugiCreds =
        UserGroupInformation.getCurrentUser().getCredentials();
    assertEquals(1, ugiCreds.numberOfTokens());
    Token<?> ugiToken = ugiCreds.getToken(new Text("token-alias"));
    assertNotNull(ugiToken);
    assertEquals(token, ugiToken);
    
    localFs.delete(new Path(testDir.getAbsolutePath()), true);
  }

  /** Test -D parsing */
  @Test
  public void testDOptionParsing() throws Exception {
    String[] args;
    Map<String,String> expectedMap;
    String[] expectedRemainingArgs;

    args = new String[]{};
    expectedRemainingArgs = new String[]{};
    expectedMap = Maps.newHashMap();
    assertDOptionParsing(args, expectedMap, expectedRemainingArgs);

    args = new String[]{"-Dkey1=value1"};
    expectedRemainingArgs = new String[]{};
    expectedMap = Maps.newHashMap();
    expectedMap.put("key1", "value1");
    assertDOptionParsing(args, expectedMap, expectedRemainingArgs);

    args = new String[]{"-fs", "hdfs://somefs/", "-Dkey1=value1", "arg1"};
    expectedRemainingArgs = new String[]{"arg1"};
    assertDOptionParsing(args, expectedMap, expectedRemainingArgs);

    args = new String[]{"-fs", "hdfs://somefs/", "-D", "key1=value1", "arg1"};
    assertDOptionParsing(args, expectedMap, expectedRemainingArgs);

    if (Shell.WINDOWS) {
      args = new String[]{"-fs", "hdfs://somefs/", "-D", "key1",
        "value1", "arg1"};
      assertDOptionParsing(args, expectedMap, expectedRemainingArgs);

      args = new String[]{"-fs", "hdfs://somefs/", "-Dkey1", "value1", "arg1"};
      assertDOptionParsing(args, expectedMap, expectedRemainingArgs);

      args = new String[]{"-fs", "hdfs://somefs/", "-D", "key1", "value1",
        "-fs", "someother", "-D", "key2", "value2", "arg1", "arg2"};
      expectedRemainingArgs = new String[]{"arg1", "arg2"};
      expectedMap = Maps.newHashMap();
      expectedMap.put("key1", "value1");
      expectedMap.put("key2", "value2");
      assertDOptionParsing(args, expectedMap, expectedRemainingArgs);

      args = new String[]{"-fs", "hdfs://somefs/", "-D", "key1", "value1",
        "-fs", "someother", "-D", "key2", "value2"};
      expectedRemainingArgs = new String[]{};
      assertDOptionParsing(args, expectedMap, expectedRemainingArgs);

      args = new String[]{"-fs", "hdfs://somefs/", "-D", "key1", "value1",
        "-fs", "someother", "-D", "key2"};
      expectedMap = Maps.newHashMap();
      expectedMap.put("key1", "value1");
      expectedMap.put("key2", null); // we expect key2 not set
      assertDOptionParsing(args, expectedMap, expectedRemainingArgs);
    }

    args = new String[]{"-fs", "hdfs://somefs/", "-D", "key1=value1",
      "-fs", "someother", "-Dkey2"};
    expectedRemainingArgs = new String[]{};
    expectedMap = Maps.newHashMap();
    expectedMap.put("key1", "value1");
    expectedMap.put("key2", null); // we expect key2 not set
    assertDOptionParsing(args, expectedMap, expectedRemainingArgs);

    args = new String[]{"-fs", "hdfs://somefs/", "-D"};
    expectedMap = Maps.newHashMap();
    assertDOptionParsing(args, expectedMap, expectedRemainingArgs);
  }

  private void assertDOptionParsing(String[] args,
      Map<String,String> expectedMap, String[] expectedRemainingArgs)
      throws Exception {
    for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
      assertNull(conf.get(entry.getKey()));
    }

    Configuration conf = new Configuration();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = parser.getRemainingArgs();

    for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
      assertEquals(entry.getValue(), conf.get(entry.getKey()));
    }

    assertArrayEquals(
      Arrays.toString(remainingArgs) + Arrays.toString(expectedRemainingArgs),
      expectedRemainingArgs, remainingArgs);
  }

  /** Test passing null as args. Some classes still call
   * Tool interface from java passing null.
   */
  @Test
  public void testNullArgs() throws IOException {
    GenericOptionsParser parser = new GenericOptionsParser(conf, null);
    parser.getRemainingArgs();
  }
}
