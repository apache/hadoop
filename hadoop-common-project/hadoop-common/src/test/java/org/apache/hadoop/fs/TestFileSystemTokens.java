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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestFileSystemTokens {
  private static String renewer = "renewer!";

  @Test
  public void testFsWithNoToken() throws Exception {
    MockFileSystem fs = createFileSystemForServiceName(null);  
    Credentials credentials = new Credentials();
    
    fs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(fs, false);
    assertEquals(0, credentials.numberOfTokens());
  }
  
  @Test
  public void testFsWithToken() throws Exception {
    Text service = new Text("singleTokenFs");
    MockFileSystem fs = createFileSystemForServiceName(service);
    Credentials credentials = new Credentials();
    
    fs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(fs, true);
    
    assertEquals(1, credentials.numberOfTokens());
    assertNotNull(credentials.getToken(service));
  }

  @Test
  public void testFsWithTokenExists() throws Exception {
    Credentials credentials = new Credentials();
    Text service = new Text("singleTokenFs");
    MockFileSystem fs = createFileSystemForServiceName(service);
    Token<?> token = mock(Token.class);
    credentials.addToken(service, token);
    
    fs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(fs, false);
    
    assertEquals(1, credentials.numberOfTokens());
    assertSame(token, credentials.getToken(service));
  }
  
  @Test
  public void testFsWithChildTokens() throws Exception {
    Credentials credentials = new Credentials();
    Text service1 = new Text("singleTokenFs1");
    Text service2 = new Text("singleTokenFs2");

    MockFileSystem fs1 = createFileSystemForServiceName(service1);
    MockFileSystem fs2 = createFileSystemForServiceName(service2);
    MockFileSystem fs3 = createFileSystemForServiceName(null);
    MockFileSystem multiFs = 
        createFileSystemForServiceName(null, fs1, fs2, fs3);
    
    multiFs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(multiFs, false); // has no tokens of own, only child tokens
    verifyTokenFetch(fs1, true);
    verifyTokenFetch(fs2, true);
    verifyTokenFetch(fs3, false);
    
    assertEquals(2, credentials.numberOfTokens());
    assertNotNull(credentials.getToken(service1));
    assertNotNull(credentials.getToken(service2));
  }

  @Test
  public void testFsWithDuplicateChildren() throws Exception {
    Credentials credentials = new Credentials();
    Text service = new Text("singleTokenFs1");

    MockFileSystem fs = createFileSystemForServiceName(service);
    MockFileSystem multiFs =
        createFileSystemForServiceName(null, fs, new FilterFileSystem(fs));
    
    multiFs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(multiFs, false);
    verifyTokenFetch(fs, true);
    
    assertEquals(1, credentials.numberOfTokens());
    assertNotNull(credentials.getToken(service));
  }

  @Test
  public void testFsWithDuplicateChildrenTokenExists() throws Exception {
    Credentials credentials = new Credentials();
    Text service = new Text("singleTokenFs1");
    Token<?> token = mock(Token.class);
    credentials.addToken(service, token);

    MockFileSystem fs = createFileSystemForServiceName(service);
    MockFileSystem multiFs =
        createFileSystemForServiceName(null, fs, new FilterFileSystem(fs));
    
    multiFs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(multiFs, false);
    verifyTokenFetch(fs, false);
    
    assertEquals(1, credentials.numberOfTokens());
    assertSame(token, credentials.getToken(service));
  }

  @Test
  public void testFsWithChildTokensOneExists() throws Exception {
    Credentials credentials = new Credentials();
    Text service1 = new Text("singleTokenFs1");
    Text service2 = new Text("singleTokenFs2");
    Token<?> token = mock(Token.class);
    credentials.addToken(service2, token);

    MockFileSystem fs1 = createFileSystemForServiceName(service1);
    MockFileSystem fs2 = createFileSystemForServiceName(service2);
    MockFileSystem fs3 = createFileSystemForServiceName(null);
    MockFileSystem multiFs = createFileSystemForServiceName(null, fs1, fs2, fs3);
    
    multiFs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(multiFs, false);
    verifyTokenFetch(fs1, true);
    verifyTokenFetch(fs2, false); // we had added its token to credentials
    verifyTokenFetch(fs3, false);
    
    assertEquals(2, credentials.numberOfTokens());
    assertNotNull(credentials.getToken(service1));
    assertSame(token, credentials.getToken(service2));
  }

  @Test
  public void testFsWithMyOwnAndChildTokens() throws Exception {
    Credentials credentials = new Credentials();
    Text service1 = new Text("singleTokenFs1");
    Text service2 = new Text("singleTokenFs2");
    Text myService = new Text("multiTokenFs");
    Token<?> token = mock(Token.class);
    credentials.addToken(service2, token);

    MockFileSystem fs1 = createFileSystemForServiceName(service1);
    MockFileSystem fs2 = createFileSystemForServiceName(service2);
    MockFileSystem multiFs = createFileSystemForServiceName(myService, fs1, fs2);
    
    multiFs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(multiFs, true); // its own token and also of its children
    verifyTokenFetch(fs1, true);
    verifyTokenFetch(fs2, false);  // we had added its token to credentials 
    
    assertEquals(3, credentials.numberOfTokens());
    assertNotNull(credentials.getToken(myService));
    assertNotNull(credentials.getToken(service1));
    assertNotNull(credentials.getToken(service2));
  }


  @Test
  public void testFsWithMyOwnExistsAndChildTokens() throws Exception {
    Credentials credentials = new Credentials();
    Text service1 = new Text("singleTokenFs1");
    Text service2 = new Text("singleTokenFs2");
    Text myService = new Text("multiTokenFs");
    Token<?> token = mock(Token.class);
    credentials.addToken(myService, token);

    MockFileSystem fs1 = createFileSystemForServiceName(service1);
    MockFileSystem fs2 = createFileSystemForServiceName(service2);
    MockFileSystem multiFs = createFileSystemForServiceName(myService, fs1, fs2);
    
    multiFs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(multiFs, false);  // we had added its token to credentials
    verifyTokenFetch(fs1, true);
    verifyTokenFetch(fs2, true);
    
    assertEquals(3, credentials.numberOfTokens());
    assertSame(token, credentials.getToken(myService));
    assertNotNull(credentials.getToken(service1));
    assertNotNull(credentials.getToken(service2));
  }

  @Test
  public void testFsWithNestedDuplicatesChildren() throws Exception {
    Credentials credentials = new Credentials();
    Text service1 = new Text("singleTokenFs1");
    Text service2 = new Text("singleTokenFs2");
    Text service4 = new Text("singleTokenFs4");
    Text multiService = new Text("multiTokenFs");
    Token<?> token2 = mock(Token.class);
    credentials.addToken(service2, token2);
    
    MockFileSystem fs1 = createFileSystemForServiceName(service1);
    MockFileSystem fs1B = createFileSystemForServiceName(service1);
    MockFileSystem fs2 = createFileSystemForServiceName(service2);
    MockFileSystem fs3 = createFileSystemForServiceName(null);
    MockFileSystem fs4 = createFileSystemForServiceName(service4);
    // now let's get dirty!  ensure dup tokens aren't fetched even when
    // repeated and dupped in a nested fs.  fs4 is a real test of the drill
    // down: multi-filter-multi-filter-filter-fs4.
    MockFileSystem multiFs = createFileSystemForServiceName(multiService,
        fs1, fs1B, fs2, fs2, new FilterFileSystem(fs3),
        new FilterFileSystem(new FilterFileSystem(fs4)));
    MockFileSystem superMultiFs = createFileSystemForServiceName(null,
        fs1, fs1B, fs1, new FilterFileSystem(fs3), new FilterFileSystem(multiFs));
    superMultiFs.addDelegationTokens(renewer, credentials);
    verifyTokenFetch(superMultiFs, false); // does not have its own token
    verifyTokenFetch(multiFs, true); // has its own token
    verifyTokenFetch(fs1, true);
    verifyTokenFetch(fs2, false); // we had added its token to credentials
    verifyTokenFetch(fs3, false); // has no tokens
    verifyTokenFetch(fs4, true);
    
    assertEquals(4, credentials.numberOfTokens()); //fs1+fs2+fs4+multifs (fs3=0)
    assertNotNull(credentials.getToken(service1));
    assertNotNull(credentials.getToken(service2));
    assertSame(token2, credentials.getToken(service2));
    assertNotNull(credentials.getToken(multiService));
    assertNotNull(credentials.getToken(service4));
  }

  public static MockFileSystem createFileSystemForServiceName(
      final Text service, final FileSystem... children) throws IOException {
    final MockFileSystem fs = new MockFileSystem();
    final MockFileSystem mockFs = fs.getRawFileSystem();
    if (service != null) {
      when(mockFs.getCanonicalServiceName()).thenReturn(service.toString());
      when(mockFs.getDelegationToken(any(String.class))).thenAnswer(
        new Answer<Token<?>>() {
          @Override
          public Token<?> answer(InvocationOnMock invocation) throws Throwable {
            Token<?> token = new Token<TokenIdentifier>();
            token.setService(service);
            return token;
          }
        });
    }
    when(mockFs.getChildFileSystems()).thenReturn(children);
    return fs;
  }

  // check that canonical name was requested, if renewer is not null that
  // a token was requested, and that child fs was invoked
  private void verifyTokenFetch(MockFileSystem fs, boolean expected) throws IOException {
    verify(fs.getRawFileSystem(), atLeast(1)).getCanonicalServiceName();
    if (expected) {
      verify(fs.getRawFileSystem()).getDelegationToken(renewer);    
    } else {
      verify(fs.getRawFileSystem(), never()).getDelegationToken(any(String.class));
    }
    verify(fs.getRawFileSystem(), atLeast(1)).getChildFileSystems();
  }
}
