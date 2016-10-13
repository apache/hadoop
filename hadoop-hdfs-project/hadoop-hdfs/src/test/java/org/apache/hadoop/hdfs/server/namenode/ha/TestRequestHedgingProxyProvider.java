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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider.ProxyFactory;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.retry.MultiException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

public class TestRequestHedgingProxyProvider {

  private Configuration conf;
  private URI nnUri;
  private String ns;

  @BeforeClass
  public static void setupClass() throws Exception {
    GenericTestUtils.setLogLevel(RequestHedgingProxyProvider.LOG, Level.TRACE);
  }

  @Before
  public void setup() throws URISyntaxException {
    ns = "mycluster-" + Time.monotonicNow();
    nnUri = new URI("hdfs://" + ns);
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, ns);
    conf.set(
        DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns, "nn1,nn2");
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn1",
        "machine1.foo.bar:9820");
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn2",
        "machine2.foo.bar:9820");
  }

  @Test
  public void testHedgingWhenOneFails() throws Exception {
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1000);
        return new long[]{1};
      }
    });
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(badMock, goodMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
  }

  @Test
  public void testHedgingWhenOneIsSlow() throws Exception {
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1000);
        return new long[]{1};
      }
    });
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(goodMock, badMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
  }

  @Test
  public void testHedgingWhenBothFail() throws Exception {
    NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));
    NamenodeProtocols worseMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(worseMock.getStats()).thenThrow(
            new IOException("Worse mock !!"));

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
            createFactory(badMock, worseMock));
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since both namenodes throw IOException !!");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof MultiException);
    }
    Mockito.verify(badMock).getStats();
    Mockito.verify(worseMock).getStats();
  }

  @Test
  public void testPerformFailover() throws Exception {
    final AtomicInteger counter = new AtomicInteger(0);
    final int[] isGood = {1};
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 1) {
          Thread.sleep(1000);
          return new long[]{1};
        }
        throw new IOException("Was Good mock !!");
      }
    });
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 2) {
          Thread.sleep(1000);
          return new long[]{2};
        }
        throw new IOException("Bad mock !!");
      }
    });
    RequestHedgingProxyProvider<NamenodeProtocols> provider =
            new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
                    createFactory(goodMock, badMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Assert.assertEquals(2, counter.get());
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    // Ensure only the previous successful one is invoked
    Mockito.verifyNoMoreInteractions(badMock);
    Assert.assertEquals(3, counter.get());

    // Flip to standby.. so now this should fail
    isGood[0] = 2;
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since previously successful proxy now fails ");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }

    Assert.assertEquals(4, counter.get());

    provider.performFailover(provider.getProxy().proxy);
    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(2, stats[0]);

    // Counter shuodl update only once
    Assert.assertEquals(5, counter.get());

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(2, stats[0]);

    // Counter updates only once now
    Assert.assertEquals(6, counter.get());

    // Flip back to old active.. so now this should fail
    isGood[0] = 1;
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since previously successful proxy now fails ");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }

    Assert.assertEquals(7, counter.get());

    provider.performFailover(provider.getProxy().proxy);
    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    // Ensure correct proxy was called
    Assert.assertEquals(1, stats[0]);
  }

  @Test
  public void testPerformFailoverWith3Proxies() throws Exception {
    conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns,
            "nn1,nn2,nn3");
    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn3",
            "machine3.foo.bar:9820");

    final AtomicInteger counter = new AtomicInteger(0);
    final int[] isGood = {1};
    final NamenodeProtocols goodMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 1) {
          Thread.sleep(1000);
          return new long[]{1};
        }
        throw new IOException("Was Good mock !!");
      }
    });
    final NamenodeProtocols badMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(badMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 2) {
          Thread.sleep(1000);
          return new long[]{2};
        }
        throw new IOException("Bad mock !!");
      }
    });
    final NamenodeProtocols worseMock = Mockito.mock(NamenodeProtocols.class);
    Mockito.when(worseMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        if (isGood[0] == 3) {
          Thread.sleep(1000);
          return new long[]{3};
        }
        throw new IOException("Worse mock !!");
      }
    });

    RequestHedgingProxyProvider<NamenodeProtocols> provider =
            new RequestHedgingProxyProvider<>(conf, nnUri, NamenodeProtocols.class,
                    createFactory(goodMock, badMock, worseMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Assert.assertEquals(3, counter.get());
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
    Mockito.verify(worseMock).getStats();

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    // Ensure only the previous successful one is invoked
    Mockito.verifyNoMoreInteractions(badMock);
    Mockito.verifyNoMoreInteractions(worseMock);
    Assert.assertEquals(4, counter.get());

    // Flip to standby.. so now this should fail
    isGood[0] = 2;
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since previously successful proxy now fails ");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }

    Assert.assertEquals(5, counter.get());

    provider.performFailover(provider.getProxy().proxy);
    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(2, stats[0]);

    // Counter updates twice since both proxies are tried on failure
    Assert.assertEquals(7, counter.get());

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(2, stats[0]);

    // Counter updates only once now
    Assert.assertEquals(8, counter.get());

    // Flip to Other standby.. so now this should fail
    isGood[0] = 3;
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since previously successful proxy now fails ");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }

    // Counter should ipdate only 1 time
    Assert.assertEquals(9, counter.get());

    provider.performFailover(provider.getProxy().proxy);
    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);

    // Ensure correct proxy was called
    Assert.assertEquals(3, stats[0]);

    // Counter updates twice since both proxies are tried on failure
    Assert.assertEquals(11, counter.get());

    stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(3, stats[0]);

    // Counter updates only once now
    Assert.assertEquals(12, counter.get());
  }

  private ProxyFactory<NamenodeProtocols> createFactory(
      NamenodeProtocols... protos) {
    final Iterator<NamenodeProtocols> iterator =
        Lists.newArrayList(protos).iterator();
    return new ProxyFactory<NamenodeProtocols>() {
      @Override
      public NamenodeProtocols createProxy(Configuration conf,
          InetSocketAddress nnAddr, Class<NamenodeProtocols> xface,
          UserGroupInformation ugi, boolean withRetries,
          AtomicBoolean fallbackToSimpleAuth) throws IOException {
        return iterator.next();
      }
    };
  }
}
