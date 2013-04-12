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
package org.apache.hadoop.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.EnumSet;
import java.util.Random;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestDFVariations {

  public static class XXDF extends DF {
    private final String osName;
    public XXDF(String osName) throws IOException {
      super(new File(System.getProperty("test.build.data","/tmp")), 0L);
      this.osName = osName;
    }
    @Override
    public DF.OSType getOSType() {
      return DF.getOSType(osName);
    }
    @Override
    protected String[] getExecString() {
      return new String[] { "echo", "IGNORE\n", 
        "/dev/sda3", "453115160", "53037920", "400077240", "11%", "/foo/bar\n"};
    }
  }

  @Test(timeout=5000)
  public void testOSParsing() throws Exception {
    for (DF.OSType ost : EnumSet.allOf(DF.OSType.class)) {
      XXDF df = new XXDF(ost.getId());
      assertEquals(ost.getId() + " mount", "/foo/bar", df.getMount());
    }
  }
  
  @Test(timeout=5000)
  public void testDFInvalidPath() throws Exception {
    // Generate a path that doesn't exist
    Random random = new Random(0xDEADBEEFl);
    File file = null;
    byte[] bytes = new byte[64];
    while (file == null) {
      random.nextBytes(bytes);
      final String invalid = new String("/" + bytes);
      final File invalidFile = new File(invalid);
      if (!invalidFile.exists()) {
        file = invalidFile;
      }
    }
    DF df = new DF(file, 0l);
    try {
      df.getMount();
    } catch (FileNotFoundException e) {
      // expected, since path does not exist
      GenericTestUtils.assertExceptionContains(file.getName(), e);
    }
  }
  
  @Test(timeout=5000)
  public void testDFMalformedOutput() throws Exception {
    DF df = new DF(new File("/"), 0l);
    BufferedReader reader = new BufferedReader(new StringReader(
        "Filesystem     1K-blocks     Used Available Use% Mounted on\n" +
        "/dev/sda5       19222656 10597036   7649060  59% /"));
    df.parseExecResult(reader);
    df.parseOutput();
    
    reader = new BufferedReader(new StringReader(
        "Filesystem     1K-blocks     Used Available Use% Mounted on"));
    df.parseExecResult(reader);
    try {
      df.parseOutput();
      fail("Expected exception with missing line!");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Fewer lines of output than expected", e);
      System.out.println(e.toString());
    }
    
    reader = new BufferedReader(new StringReader(
        "Filesystem     1K-blocks     Used Available Use% Mounted on\n" +
        " "));
    df.parseExecResult(reader);
    try {
      df.parseOutput();
      fail("Expected exception with empty line!");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Unexpected empty line", e);
      System.out.println(e.toString());
    }
    
    reader = new BufferedReader(new StringReader(
        "Filesystem     1K-blocks     Used Available Use% Mounted on\n" +
        "       19222656 10597036   7649060  59% /"));
    df.parseExecResult(reader);
    try {
      df.parseOutput();
      fail("Expected exception with missing field!");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Could not parse line: ", e);
      System.out.println(e.toString());
    }
  }

  @Test(timeout=5000)
  public void testGetMountCurrentDirectory() throws Exception {
    File currentDirectory = new File(".");
    String workingDir = currentDirectory.getAbsoluteFile().getCanonicalPath();
    DF df = new DF(new File(workingDir), 0L);
    String mountPath = df.getMount();
    File mountDir = new File(mountPath);
    assertTrue("Mount dir ["+mountDir.getAbsolutePath()+"] should exist.", 
        mountDir.exists());
    assertTrue("Mount dir ["+mountDir.getAbsolutePath()+"] should be directory.", 
        mountDir.isDirectory());
    assertTrue("Working dir ["+workingDir+"] should start with ["+mountPath+"].",
        workingDir.startsWith(mountPath));
  }
}

