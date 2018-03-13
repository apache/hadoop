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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.retry.MultiException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
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
    conf.set(HdfsClientConfigKeys.DFS_NAMESERVICES, ns);
    conf.set(
        HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns, "nn1,nn2");
    conf.set(
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn1",
        "machine1.foo.bar:8020");
    conf.set(
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn2",
        "machine2.foo.bar:8020");
  }

  @Test
  public void testHedgingWhenOneFails() throws Exception {
    final ClientProtocol goodMock = Mockito.mock(ClientProtocol.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1000);
        return new long[]{1};
      }
    });
    final ClientProtocol badMock = Mockito.mock(ClientProtocol.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));

    RequestHedgingProxyProvider<ClientProtocol> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, ClientProtocol.class,
            createFactory(badMock, goodMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
  }

  @Test
  public void testHedgingWhenOneIsSlow() throws Exception {
    final ClientProtocol goodMock = Mockito.mock(ClientProtocol.class);
    Mockito.when(goodMock.getStats()).thenAnswer(new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1000);
        return new long[]{1};
      }
    });
    final ClientProtocol badMock = Mockito.mock(ClientProtocol.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));

    RequestHedgingProxyProvider<ClientProtocol> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, ClientProtocol.class,
            createFactory(goodMock, badMock));
    long[] stats = provider.getProxy().proxy.getStats();
    Assert.assertTrue(stats.length == 1);
    Assert.assertEquals(1, stats[0]);
    Mockito.verify(badMock).getStats();
    Mockito.verify(goodMock).getStats();
  }

  @Test
  public void testHedgingWhenBothFail() throws Exception {
    ClientProtocol badMock = Mockito.mock(ClientProtocol.class);
    Mockito.when(badMock.getStats()).thenThrow(new IOException("Bad mock !!"));
    ClientProtocol worseMock = Mockito.mock(ClientProtocol.class);
    Mockito.when(worseMock.getStats()).thenThrow(
            new IOException("Worse mock !!"));

    RequestHedgingProxyProvider<ClientProtocol> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri, ClientProtocol.class,
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
    final ClientProtocol goodMock = Mockito.mock(ClientProtocol.class);
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
    final ClientProtocol badMock = Mockito.mock(ClientProtocol.class);
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
    RequestHedgingProxyProvider<ClientProtocol> provider =
            new RequestHedgingProxyProvider<>(conf, nnUri, ClientProtocol.class,
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

    // Counter should update only once
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
  public void testFileNotFoundExceptionWithSingleProxy() throws Exception {
    ClientProtocol active = Mockito.mock(ClientProtocol.class);
    Mockito
        .when(active.getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong()))
        .thenThrow(new RemoteException("java.io.FileNotFoundException",
            "File does not exist!"));

    ClientProtocol standby = Mockito.mock(ClientProtocol.class);
    Mockito
        .when(standby.getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong()))
        .thenThrow(
            new RemoteException("org.apache.hadoop.ipc.StandbyException",
                "Standby NameNode"));

    RequestHedgingProxyProvider<ClientProtocol> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri,
            ClientProtocol.class, createFactory(standby, active));
    try {
      provider.getProxy().proxy.getBlockLocations("/tmp/test.file", 0L, 20L);
      Assert.fail("Should fail since the active namenode throws"
          + " FileNotFoundException!");
    } catch (MultiException me) {
      for (Exception ex : me.getExceptions().values()) {
        Exception rEx = ((RemoteException) ex).unwrapRemoteException();
        if (rEx instanceof StandbyException) {
          continue;
        }
        Assert.assertTrue(rEx instanceof FileNotFoundException);
      }
    }
    //Perform failover now, there will only be one active proxy now
    provider.performFailover(active);
    try {
      provider.getProxy().proxy.getBlockLocations("/tmp/test.file", 0L, 20L);
      Assert.fail("Should fail since the active namenode throws"
          + " FileNotFoundException!");
    } catch (RemoteException ex) {
      Exception rEx = ex.unwrapRemoteException();
      if (rEx instanceof StandbyException) {
        Mockito.verify(active).getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong());
        Mockito.verify(standby, Mockito.times(2))
            .getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong());
      } else {
        Assert.assertTrue(rEx instanceof FileNotFoundException);
        Mockito.verify(active, Mockito.times(2))
            .getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong());
        Mockito.verify(standby).getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong());
      }
    }
  }

  @Test
  public void testSingleProxyFailover() throws Exception {
    String singleNS = "mycluster-" + Time.monotonicNow();
    URI singleNNUri = new URI("hdfs://" + singleNS);
    Configuration singleConf = new Configuration();
    singleConf.set(HdfsClientConfigKeys.DFS_NAMESERVICES, singleNS);
    singleConf.set(HdfsClientConfigKeys.
        DFS_HA_NAMENODES_KEY_PREFIX + "." + singleNS, "nn1");

    singleConf.set(HdfsClientConfigKeys.
            DFS_NAMENODE_RPC_ADDRESS_KEY + "." + singleNS + ".nn1",
        RandomStringUtils.randomAlphabetic(8) + ".foo.bar:9820");
    ClientProtocol active = Mockito.mock(ClientProtocol.class);
    Mockito
        .when(active.getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong()))
        .thenThrow(new RemoteException("java.io.FileNotFoundException",
            "File does not exist!"));

    RequestHedgingProxyProvider<ClientProtocol> provider =
        new RequestHedgingProxyProvider<>(singleConf, singleNNUri,
            ClientProtocol.class, createFactory(active));
    try {
      provider.getProxy().proxy.getBlockLocations("/tmp/test.file", 0L, 20L);
      Assert.fail("Should fail since the active namenode throws"
          + " FileNotFoundException!");
    } catch (RemoteException ex) {
      Exception rEx = ex.unwrapRemoteException();
      Assert.assertTrue(rEx instanceof FileNotFoundException);
    }
    //Perform failover now, there will be no active proxies now
    provider.performFailover(active);
    try {
      provider.getProxy().proxy.getBlockLocations("/tmp/test.file", 0L, 20L);
      Assert.fail("Should fail since the active namenode throws"
          + " FileNotFoundException!");
    } catch (RemoteException ex) {
      Exception rEx = ex.unwrapRemoteException();
      Assert.assertTrue(rEx instanceof IOException);
      Assert.assertTrue(rEx.getMessage().equals("No valid proxies left."
          + " All NameNode proxies have failed over."));
    }
  }

  @Test
  public void testPerformFailoverWith3Proxies() throws Exception {
    conf.set(HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns,
            "nn1,nn2,nn3");
    conf.set(HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns + ".nn3",
            "machine3.foo.bar:8020");

    final AtomicInteger counter = new AtomicInteger(0);
    final int[] isGood = {1};
    final ClientProtocol goodMock = Mockito.mock(ClientProtocol.class);
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
    final ClientProtocol badMock = Mockito.mock(ClientProtocol.class);
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
    final ClientProtocol worseMock = Mockito.mock(ClientProtocol.class);
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

    RequestHedgingProxyProvider<ClientProtocol> provider =
            new RequestHedgingProxyProvider<>(conf, nnUri, ClientProtocol.class,
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

  @Test
  public void testHedgingWhenFileNotFoundException() throws Exception {
    ClientProtocol active = Mockito.mock(ClientProtocol.class);
    Mockito
        .when(active.getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong()))
        .thenThrow(new RemoteException("java.io.FileNotFoundException",
            "File does not exist!"));

    ClientProtocol standby = Mockito.mock(ClientProtocol.class);
    Mockito
        .when(standby.getBlockLocations(Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong()))
        .thenThrow(
            new RemoteException("org.apache.hadoop.ipc.StandbyException",
            "Standby NameNode"));

    RequestHedgingProxyProvider<ClientProtocol> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri,
          ClientProtocol.class, createFactory(active, standby));
    try {
      provider.getProxy().proxy.getBlockLocations("/tmp/test.file", 0L, 20L);
      Assert.fail("Should fail since the active namenode throws"
          + " FileNotFoundException!");
    } catch (MultiException me) {
      for (Exception ex : me.getExceptions().values()) {
        Exception rEx = ((RemoteException) ex).unwrapRemoteException();
        if (rEx instanceof StandbyException) {
          continue;
        }
        Assert.assertTrue(rEx instanceof FileNotFoundException);
      }
    }
    Mockito.verify(active).getBlockLocations(Matchers.anyString(),
        Matchers.anyLong(), Matchers.anyLong());
    Mockito.verify(standby).getBlockLocations(Matchers.anyString(),
        Matchers.anyLong(), Matchers.anyLong());
  }

  @Test
  public void testHedgingWhenConnectException() throws Exception {
    ClientProtocol active = Mockito.mock(ClientProtocol.class);
    Mockito.when(active.getStats()).thenThrow(new ConnectException());

    ClientProtocol standby = Mockito.mock(ClientProtocol.class);
    Mockito.when(standby.getStats())
        .thenThrow(
            new RemoteException("org.apache.hadoop.ipc.StandbyException",
            "Standby NameNode"));

    RequestHedgingProxyProvider<ClientProtocol> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri,
          ClientProtocol.class, createFactory(active, standby));
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since the active namenode throws"
          + " ConnectException!");
    } catch (MultiException me) {
      for (Exception ex : me.getExceptions().values()) {
        if (ex instanceof RemoteException) {
          Exception rEx = ((RemoteException) ex)
              .unwrapRemoteException();
          Assert.assertTrue("Unexpected RemoteException: " + rEx.getMessage(),
              rEx instanceof StandbyException);
        } else {
          Assert.assertTrue(ex instanceof ConnectException);
        }
      }
    }
    Mockito.verify(active).getStats();
    Mockito.verify(standby).getStats();
  }

  @Test
  public void testHedgingWhenConnectAndEOFException() throws Exception {
    ClientProtocol active = Mockito.mock(ClientProtocol.class);
    Mockito.when(active.getStats()).thenThrow(new EOFException());

    ClientProtocol standby = Mockito.mock(ClientProtocol.class);
    Mockito.when(standby.getStats()).thenThrow(new ConnectException());

    RequestHedgingProxyProvider<ClientProtocol> provider =
        new RequestHedgingProxyProvider<>(conf, nnUri,
          ClientProtocol.class, createFactory(active, standby));
    try {
      provider.getProxy().proxy.getStats();
      Assert.fail("Should fail since both active and standby namenodes throw"
          + " Exceptions!");
    } catch (MultiException me) {
      for (Exception ex : me.getExceptions().values()) {
        if (!(ex instanceof ConnectException) &&
            !(ex instanceof EOFException)) {
          Assert.fail("Unexpected Exception " + ex.getMessage());
        }
      }
    }
    Mockito.verify(active).getStats();
    Mockito.verify(standby).getStats();
  }

  private HAProxyFactory<ClientProtocol> createFactory(
      ClientProtocol... protos) {
    final Iterator<ClientProtocol> iterator =
        Lists.newArrayList(protos).iterator();
    return new HAProxyFactory<ClientProtocol>() {
      @Override
      public ClientProtocol createProxy(Configuration conf,
          InetSocketAddress nnAddr, Class<ClientProtocol> xface,
          UserGroupInformation ugi, boolean withRetries,
          AtomicBoolean fallbackToSimpleAuth) throws IOException {
        return iterator.next();
      }

      @Override
      public ClientProtocol createProxy(Configuration conf,
          InetSocketAddress nnAddr, Class<ClientProtocol> xface,
          UserGroupInformation ugi, boolean withRetries) throws IOException {
        return iterator.next();
      }
    };
  }
}
