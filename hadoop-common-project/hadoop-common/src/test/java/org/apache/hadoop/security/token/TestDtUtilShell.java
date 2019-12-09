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
package org.apache.hadoop.security.token;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.test.GenericTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;

public class TestDtUtilShell {
  private static byte[] IDENTIFIER = {
      0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72};
  private static byte[] PASSWORD = {
      0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64};
  private static Text KIND = new Text("testTokenKind");
  private static Text SERVICE = new Text("testTokenService");
  private static Text SERVICE2 = new Text("ecivreSnekoTtset");
  private static Configuration defaultConf = new Configuration();
  private static FileSystem localFs = null;
  private final String alias = "proxy_ip:1234";
  private final String getUrl = SERVICE_GET.toString() + "://localhost:9000/";
  private final String getUrl2 = "http://localhost:9000/";
  public static Text SERVICE_GET = new Text("testTokenServiceGet");
  public static Text KIND_GET = new Text("testTokenKindGet");
  public static Token<?> MOCK_TOKEN =
      new Token(IDENTIFIER, PASSWORD, KIND_GET, SERVICE_GET);
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final Path workDir = new Path(
      GenericTestUtils.getTestDir("TestDtUtilShell").getAbsolutePath());
  private final Path tokenFile = new Path(workDir, "testPrintTokenFile");
  private final Path tokenFile2 = new Path(workDir, "testPrintTokenFile2");
  private final Path tokenLegacyFile = new Path(workDir, "testPrintTokenFile3");
  private final Path tokenFileGet = new Path(workDir, "testGetTokenFile");
  private final String tokenFilename = tokenFile.toString();
  private final String tokenFilename2 = tokenFile2.toString();
  private final String tokenFilenameGet = tokenFileGet.toString();
  private String[] args = null;
  private DtUtilShell dt = null;
  private int rc = 0;

  @Before
  public void setup() throws Exception {
    localFs.mkdirs(localFs.makeQualified(workDir));
    makeTokenFile(tokenFile, false, null);
    makeTokenFile(tokenFile2, false, SERVICE2);
    makeTokenFile(tokenLegacyFile, true, null);
    dt = new DtUtilShell();
    dt.setConf(new Configuration());
    dt.setOut(new PrintStream(outContent));
    outContent.reset();
    rc = 0;
  }

  @After
  public void teardown() throws Exception {
    localFs.delete(localFs.makeQualified(workDir), true);
  }

  public void makeTokenFile(Path tokenPath, boolean legacy, Text service)
        throws IOException {
    if (service == null) {
      service = SERVICE;
    }
    Credentials creds = new Credentials();
    Token<? extends TokenIdentifier> tok = (Token<? extends TokenIdentifier>)
        new Token(IDENTIFIER, PASSWORD, KIND, service);
    creds.addToken(tok.getService(), tok);
    Credentials.SerializedFormat format =
        Credentials.SerializedFormat.PROTOBUF;
    if (legacy) {
      format = Credentials.SerializedFormat.WRITABLE;
    }
    creds.writeTokenStorageFile(tokenPath, defaultConf, format);
  }

  @Test
  public void testPrint() throws Exception {
    args = new String[] {"print", tokenFilename};
    rc = dt.run(args);
    assertEquals("test simple print exit code", 0, rc);
    assertTrue("test simple print output kind:\n" + outContent.toString(),
               outContent.toString().contains(KIND.toString()));
    assertTrue("test simple print output service:\n" + outContent.toString(),
               outContent.toString().contains(SERVICE.toString()));

    outContent.reset();
    args = new String[] {"print", tokenLegacyFile.toString()};
    rc = dt.run(args);
    assertEquals("test legacy print exit code", 0, rc);
    assertTrue("test simple print output kind:\n" + outContent.toString(),
               outContent.toString().contains(KIND.toString()));
    assertTrue("test simple print output service:\n" + outContent.toString(),
               outContent.toString().contains(SERVICE.toString()));

    outContent.reset();
    args = new String[] {
        "print", "-alias", SERVICE.toString(), tokenFilename};
    rc = dt.run(args);
    assertEquals("test alias print exit code", 0, rc);
    assertTrue("test simple print output kind:\n" + outContent.toString(),
               outContent.toString().contains(KIND.toString()));
    assertTrue("test simple print output service:\n" + outContent.toString(),
               outContent.toString().contains(SERVICE.toString()));

    outContent.reset();
    args = new String[] {
        "print", "-alias", "not-a-serivce", tokenFilename};
    rc = dt.run(args);
    assertEquals("test no alias print exit code", 0, rc);
    assertFalse("test no alias print output kind:\n" + outContent.toString(),
                outContent.toString().contains(KIND.toString()));
    assertFalse("test no alias print output service:\n" + outContent.toString(),
                outContent.toString().contains(SERVICE.toString()));
  }

  @Test
  public void testEdit() throws Exception {
    String oldService = SERVICE2.toString();
    String newAlias = "newName:12345";
    args = new String[] {"edit",
        "-service", oldService, "-alias", newAlias, tokenFilename2};
    rc = dt.run(args);
    assertEquals("test simple edit exit code", 0, rc);
    args = new String[] {"print", "-alias", oldService, tokenFilename2};
    rc = dt.run(args);
    assertEquals("test simple edit print old exit code", 0, rc);
    assertTrue("test simple edit output kind old:\n" + outContent.toString(),
               outContent.toString().contains(KIND.toString()));
    assertTrue("test simple edit output service old:\n" + outContent.toString(),
               outContent.toString().contains(oldService));
    args = new String[] {"print", "-alias", newAlias, tokenFilename2};
    rc = dt.run(args);
    assertEquals("test simple edit print new exit code", 0, rc);
    assertTrue("test simple edit output kind new:\n" + outContent.toString(),
               outContent.toString().contains(KIND.toString()));
    assertTrue("test simple edit output service new:\n" + outContent.toString(),
               outContent.toString().contains(newAlias));
  }

  @Test
  public void testAppend() throws Exception {
    args = new String[] {"append", tokenFilename, tokenFilename2};
    rc = dt.run(args);
    assertEquals("test simple append exit code", 0, rc);
    args = new String[] {"print", tokenFilename2};
    rc = dt.run(args);
    assertEquals("test simple append print exit code", 0, rc);
    assertTrue("test simple append output kind:\n" + outContent.toString(),
               outContent.toString().contains(KIND.toString()));
    assertTrue("test simple append output service:\n" + outContent.toString(),
               outContent.toString().contains(SERVICE.toString()));
    assertTrue("test simple append output service:\n" + outContent.toString(),
               outContent.toString().contains(SERVICE2.toString()));
  }

  @Test
  public void testRemove() throws Exception {
    args = new String[] {"remove", "-alias", SERVICE.toString(), tokenFilename};
    rc = dt.run(args);
    assertEquals("test simple remove exit code", 0, rc);
    args = new String[] {"print", tokenFilename};
    rc = dt.run(args);
    assertEquals("test simple remove print exit code", 0, rc);
    assertFalse("test simple remove output kind:\n" + outContent.toString(),
                outContent.toString().contains(KIND.toString()));
    assertFalse("test simple remove output service:\n" + outContent.toString(),
                outContent.toString().contains(SERVICE.toString()));
  }

  @Test
  public void testGet() throws Exception {
    args = new String[] {"get", getUrl, tokenFilenameGet};
    rc = dt.run(args);
    assertEquals("test mocked get exit code", 0, rc);
    args = new String[] {"print", tokenFilenameGet};
    rc = dt.run(args);
    String oc = outContent.toString();
    assertEquals("test print after get exit code", 0, rc);
    assertTrue("test print after get output kind:\n" + oc,
               oc.contains(KIND_GET.toString()));
    assertTrue("test print after get output service:\n" + oc,
               oc.contains(SERVICE_GET.toString()));
  }

  @Test
  public void testGetWithServiceFlag() throws Exception {
    args = new String[] {"get", getUrl2, "-service", SERVICE_GET.toString(),
                         tokenFilenameGet};
    rc = dt.run(args);
    assertEquals("test mocked get with service flag exit code", 0, rc);
    args = new String[] {"print", tokenFilenameGet};
    rc = dt.run(args);
    String oc = outContent.toString();
    assertEquals("test print after get with service flag exit code", 0, rc);
    assertTrue("test print after get with service flag output kind:\n" + oc,
               oc.contains(KIND_GET.toString()));
    assertTrue("test print after get with service flag output service:\n" + oc,
               oc.contains(SERVICE_GET.toString()));
  }

  @Test
  public void testGetWithAliasFlag() throws Exception {
    args = new String[] {"get", getUrl, "-alias", alias, tokenFilenameGet};
    rc = dt.run(args);
    assertEquals("test mocked get with alias flag exit code", 0, rc);
    args = new String[] {"print", tokenFilenameGet};
    rc = dt.run(args);
    String oc = outContent.toString();
    assertEquals("test print after get with alias flag exit code", 0, rc);
    assertTrue("test print after get with alias flag output kind:\n" + oc,
               oc.contains(KIND_GET.toString()));
    assertTrue("test print after get with alias flag output alias:\n" + oc,
               oc.contains(alias));
    assertFalse("test print after get with alias flag output old service:\n" +
                oc, oc.contains(SERVICE_GET.toString()));
  }

  @Test
  public void testFormatJavaFlag() throws Exception {
    args = new String[] {"get", getUrl, "-format", "java", tokenFilenameGet};
    rc = dt.run(args);
    assertEquals("test mocked get with java format flag exit code", 0, rc);
    Credentials creds = new Credentials();
    Credentials spyCreds = Mockito.spy(creds);
    DataInputStream in = new DataInputStream(
        new FileInputStream(tokenFilenameGet));
    spyCreds.readTokenStorageStream(in);
    Mockito.verify(spyCreds).readFields(in);
  }

  @Test
  public void testFormatProtoFlag() throws Exception {
    args = new String[] {
        "get", getUrl, "-format", "protobuf", tokenFilenameGet};
    rc = dt.run(args);
    assertEquals("test mocked get with protobuf format flag exit code", 0, rc);
    Credentials creds = new Credentials();
    Credentials spyCreds = Mockito.spy(creds);
    DataInputStream in = new DataInputStream(
        new FileInputStream(tokenFilenameGet));
    spyCreds.readTokenStorageStream(in);
    Mockito.verify(spyCreds, Mockito.never()).readFields(in);
  }
}
