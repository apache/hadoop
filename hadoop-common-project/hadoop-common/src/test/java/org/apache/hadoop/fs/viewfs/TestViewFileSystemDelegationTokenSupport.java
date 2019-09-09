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
package org.apache.hadoop.fs.viewfs;

import static org.junit.Assert.*;
import static org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.getChildFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test ViewFileSystem's support for having delegation tokens fetched and cached
 * for the file system.
 * 
 * Currently this class just ensures that getCanonicalServiceName() always
 * returns <code>null</code> for ViewFileSystem instances.
 */
public class TestViewFileSystemDelegationTokenSupport {
  
  private static final String MOUNT_TABLE_NAME = "vfs-cluster";
  static Configuration conf;
  static FileSystem viewFs;
  static FakeFileSystem fs1;
  static FakeFileSystem fs2;

  @BeforeClass
  public static void setup() throws Exception {
    conf = ViewFileSystemTestSetup.createConfig();
    setupFileSystem(new URI("fs1:///"), FakeFileSystem.class);
    setupFileSystem(new URI("fs2:///"), FakeFileSystem.class);
    viewFs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    fs1 = (FakeFileSystem) getChildFileSystem((ViewFileSystem) viewFs,
        new URI("fs1:///"));
    fs2 = (FakeFileSystem) getChildFileSystem((ViewFileSystem) viewFs,
        new URI("fs2:///"));
  }

  static void setupFileSystem(URI uri, Class<? extends FileSystem> clazz)
      throws Exception {
    String scheme = uri.getScheme();
    conf.set("fs."+scheme+".impl", clazz.getName());
    FakeFileSystem fs = (FakeFileSystem)FileSystem.get(uri, conf);
    // mount each fs twice, will later ensure 1 token/fs
    ConfigUtil.addLink(conf, "/mounts/"+scheme+"-one", fs.getUri());
    ConfigUtil.addLink(conf, "/mounts/"+scheme+"-two", fs.getUri());
  }

  /**
   * Regression test for HADOOP-8408.
   */
  @Test
  public void testGetCanonicalServiceNameWithNonDefaultMountTable()
      throws URISyntaxException, IOException {
    
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, MOUNT_TABLE_NAME, "/user", new URI("file:///"));
    
    FileSystem viewFs = FileSystem.get(new URI(FsConstants.VIEWFS_SCHEME +
        "://" + MOUNT_TABLE_NAME), conf);
    
    String serviceName = viewFs.getCanonicalServiceName();
    assertNull(serviceName);
  }
  
  @Test
  public void testGetCanonicalServiceNameWithDefaultMountTable()
      throws URISyntaxException, IOException {
    
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, "/user", new URI("file:///"));
    
    FileSystem viewFs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    
    String serviceName = viewFs.getCanonicalServiceName();
    assertNull(serviceName);
  }

  @Test
  public void testGetChildFileSystems() throws Exception {
    assertNull(fs1.getChildFileSystems());
    assertNull(fs2.getChildFileSystems());    
    List<FileSystem> children = Arrays.asList(viewFs.getChildFileSystems());
    assertEquals(2, children.size());
    assertTrue(children.contains(fs1));
    assertTrue(children.contains(fs2));
  }
  
  @Test
  public void testAddDelegationTokens() throws Exception {
    Credentials creds = new Credentials();
    Token<?> fs1Tokens[] = addTokensWithCreds(fs1, creds);
    assertEquals(1, fs1Tokens.length);
    assertEquals(1, creds.numberOfTokens());
    Token<?> fs2Tokens[] = addTokensWithCreds(fs2, creds);
    assertEquals(1, fs2Tokens.length);
    assertEquals(2, creds.numberOfTokens());
    
    Credentials savedCreds = creds;
    creds = new Credentials();
    
    // should get the same set of tokens as explicitly fetched above
    Token<?> viewFsTokens[] = viewFs.addDelegationTokens("me", creds);
    assertEquals(2, viewFsTokens.length);
    assertTrue(creds.getAllTokens().containsAll(savedCreds.getAllTokens()));
    assertEquals(savedCreds.numberOfTokens(), creds.numberOfTokens()); 
    // should get none, already have all tokens
    viewFsTokens = viewFs.addDelegationTokens("me", creds);
    assertEquals(0, viewFsTokens.length);
    assertTrue(creds.getAllTokens().containsAll(savedCreds.getAllTokens()));
    assertEquals(savedCreds.numberOfTokens(), creds.numberOfTokens());
  }

  Token<?>[] addTokensWithCreds(FileSystem fs, Credentials creds) throws Exception {
    Credentials savedCreds;
    
    savedCreds = new Credentials(creds);
    Token<?> tokens[] = fs.addDelegationTokens("me", creds);
    // test that we got the token we wanted, and that creds were modified
    assertEquals(1, tokens.length);
    assertEquals(fs.getCanonicalServiceName(), tokens[0].getService().toString());
    assertTrue(creds.getAllTokens().contains(tokens[0]));
    assertTrue(creds.getAllTokens().containsAll(savedCreds.getAllTokens()));
    assertEquals(savedCreds.numberOfTokens()+1, creds.numberOfTokens());
    
    // shouldn't get any new tokens since already in creds
    savedCreds = new Credentials(creds);
    Token<?> tokenRefetch[] = fs.addDelegationTokens("me", creds);
    assertEquals(0, tokenRefetch.length);
    assertTrue(creds.getAllTokens().containsAll(savedCreds.getAllTokens()));
    assertEquals(savedCreds.numberOfTokens(), creds.numberOfTokens()); 

    return tokens;
  }

  static class FakeFileSystem extends RawLocalFileSystem {
    URI uri;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
      this.uri = name;
    }

    @Override
    public Path getInitialWorkingDirectory() {
      return new Path("/"); // ctor calls getUri before the uri is inited...
    }
    
    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public String getCanonicalServiceName() {
      return String.valueOf(this.getUri()+"/"+this.hashCode());
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
      Token<?> token = new Token<TokenIdentifier>();
      token.setService(new Text(getCanonicalServiceName()));
      return token;
    }

    @Override
    public void close() {}
  }
}
