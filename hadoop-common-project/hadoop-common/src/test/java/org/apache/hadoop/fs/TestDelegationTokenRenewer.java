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

import java.io.IOException;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegationTokenRenewer.Renewable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestDelegationTokenRenewer {
  public abstract class RenewableFileSystem extends FileSystem
  implements Renewable { }
  
  private static final long RENEW_CYCLE = 1000;
  
  private DelegationTokenRenewer renewer;
  Configuration conf;
  FileSystem fs;
  
  @Before
  public void setup() {
    DelegationTokenRenewer.renewCycle = RENEW_CYCLE;
    DelegationTokenRenewer.reset();
    renewer = DelegationTokenRenewer.getInstance();
  }
  
  @Test
  public void testAddRemoveRenewAction() throws IOException,
      InterruptedException {
    Text service = new Text("myservice");
    Configuration conf = mock(Configuration.class);

    Token<?> token = mock(Token.class);
    doReturn(service).when(token).getService();
    doAnswer(new Answer<Long>() {
      public Long answer(InvocationOnMock invocation) {
        return Time.now() + RENEW_CYCLE;
      }
    }).when(token).renew(any(Configuration.class));

    RenewableFileSystem fs = mock(RenewableFileSystem.class);
    doReturn(conf).when(fs).getConf();
    doReturn(token).when(fs).getRenewToken();

    renewer.addRenewAction(fs);
    
    assertEquals("FileSystem not added to DelegationTokenRenewer", 1,
        renewer.getRenewQueueLength());
    
    Thread.sleep(RENEW_CYCLE*2);
    verify(token, atLeast(2)).renew(eq(conf));
    verify(token, atMost(3)).renew(eq(conf));
    verify(token, never()).cancel(any(Configuration.class));
    renewer.removeRenewAction(fs);
    verify(token).cancel(eq(conf));

    verify(fs, never()).getDelegationToken(null);
    verify(fs, never()).setDelegationToken(any());
    
    assertEquals("FileSystem not removed from DelegationTokenRenewer", 0,
        renewer.getRenewQueueLength());
  }

  @Test
  public void testAddRenewActionWithNoToken() throws IOException,
      InterruptedException {
    Configuration conf = mock(Configuration.class);

    RenewableFileSystem fs = mock(RenewableFileSystem.class);
    doReturn(conf).when(fs).getConf();
    doReturn(null).when(fs).getRenewToken();

    renewer.addRenewAction(fs);
    
    verify(fs).getRenewToken();
    assertEquals(0, renewer.getRenewQueueLength());
  }

  @Test
  public void testGetNewTokenOnRenewFailure() throws IOException,
      InterruptedException {
    Text service = new Text("myservice");
    Configuration conf = mock(Configuration.class);
    
    final Token<?> token1 = mock(Token.class);
    doReturn(service).when(token1).getService();
    doThrow(new IOException("boom")).when(token1).renew(eq(conf));
    
    final Token<?> token2 = mock(Token.class);
    doReturn(service).when(token2).getService();
    doAnswer(new Answer<Long>() {
      public Long answer(InvocationOnMock invocation) {
        return Time.now() + RENEW_CYCLE;
      }
    }).when(token2).renew(eq(conf));    

    RenewableFileSystem fs = mock(RenewableFileSystem.class);
    doReturn(conf).when(fs).getConf();
    doReturn(token1).doReturn(token2).when(fs).getRenewToken();
    doReturn(token2).when(fs).getDelegationToken(null);
    
    doAnswer(new Answer<Token<?>[]>() {
      public Token<?>[] answer(InvocationOnMock invocation) {
        return new Token<?>[]{token2};
      }
    }).when(fs).addDelegationTokens(null, null);
        
    renewer.addRenewAction(fs);
    assertEquals(1, renewer.getRenewQueueLength());
    
    Thread.sleep(RENEW_CYCLE);
    verify(fs).getRenewToken();
    verify(token1, atLeast(1)).renew(eq(conf));
    verify(token1, atMost(2)).renew(eq(conf));
    verify(fs).addDelegationTokens(null, null);
    verify(fs).setDelegationToken(eq(token2));
    assertEquals(1, renewer.getRenewQueueLength());
    
    renewer.removeRenewAction(fs);
    verify(token2).cancel(eq(conf));
    assertEquals(0, renewer.getRenewQueueLength());
  }

  @Test
  public void testStopRenewalWhenFsGone() throws IOException,
      InterruptedException {
    Configuration conf = mock(Configuration.class);
    
    Token<?> token = mock(Token.class);
    doReturn(new Text("myservice")).when(token).getService();
    doAnswer(new Answer<Long>() {
      public Long answer(InvocationOnMock invocation) {
        return Time.now() + RENEW_CYCLE;
      }
    }).when(token).renew(any(Configuration.class));

    RenewableFileSystem fs = mock(RenewableFileSystem.class);
    doReturn(conf).when(fs).getConf();
    doReturn(token).when(fs).getRenewToken();

    renewer.addRenewAction(fs);
    assertEquals(1, renewer.getRenewQueueLength());

    Thread.sleep(RENEW_CYCLE);
    verify(token, atLeast(1)).renew(eq(conf));
    verify(token, atMost(2)).renew(eq(conf));
    // drop weak ref
    fs = null;
    System.gc(); System.gc(); System.gc();
    // next renew should detect the fs as gone
    Thread.sleep(RENEW_CYCLE);
    verify(token, atLeast(1)).renew(eq(conf));
    verify(token, atMost(2)).renew(eq(conf));
    assertEquals(0, renewer.getRenewQueueLength());
  }
  
  @Test(timeout=4000)
  public void testMultipleTokensDoNotDeadlock() throws IOException,
      InterruptedException {
    Configuration conf = mock(Configuration.class);
    FileSystem fs = mock(FileSystem.class);
    doReturn(conf).when(fs).getConf();
    
    long distantFuture = Time.now() + 3600 * 1000; // 1h
    Token<?> token1 = mock(Token.class);
    doReturn(new Text("myservice1")).when(token1).getService();
    doReturn(distantFuture).when(token1).renew(eq(conf));
    
    Token<?> token2 = mock(Token.class);
    doReturn(new Text("myservice2")).when(token2).getService();
    doReturn(distantFuture).when(token2).renew(eq(conf));

    RenewableFileSystem fs1 = mock(RenewableFileSystem.class);
    doReturn(conf).when(fs1).getConf();
    doReturn(token1).when(fs1).getRenewToken();

    RenewableFileSystem fs2 = mock(RenewableFileSystem.class);
    doReturn(conf).when(fs2).getConf();
    doReturn(token2).when(fs2).getRenewToken();

    renewer.addRenewAction(fs1);
    renewer.addRenewAction(fs2);
    assertEquals(2, renewer.getRenewQueueLength());
    
    renewer.removeRenewAction(fs1);
    assertEquals(1, renewer.getRenewQueueLength());
    renewer.removeRenewAction(fs2);
    assertEquals(0, renewer.getRenewQueueLength());
    
    verify(token1).cancel(eq(conf));
    verify(token2).cancel(eq(conf));
  }
}
