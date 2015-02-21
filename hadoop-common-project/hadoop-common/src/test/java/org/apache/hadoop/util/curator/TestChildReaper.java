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
package org.apache.hadoop.util.curator;

import org.apache.curator.framework.recipes.locks.Reaper;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.BindException;
import java.util.Random;

/**
 * This is a copy of Curator 2.7.1's TestChildReaper class, with minor
 * modifications to make it work with JUnit (some setup code taken from
 * Curator's BaseClassForTests).  This is to ensure that the ChildReaper
 * class we modified is still correct.
 */
public class TestChildReaper
{
  protected TestingServer server;

  @Before
  public void setup() throws Exception {
    while(this.server == null) {
      try {
        this.server = new TestingServer();
      } catch (BindException var2) {
        System.err.println("Getting bind exception - retrying to allocate server");
        this.server = null;
      }
    }
  }

  @After
  public void teardown() throws Exception {
    this.server.close();
    this.server = null;
  }

  @Test
  public void     testSomeNodes() throws Exception
  {

    Timing                  timing = new Timing();
    ChildReaper             reaper = null;
    CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
    try
    {
      client.start();

      Random              r = new Random();
      int                 nonEmptyNodes = 0;
      for ( int i = 0; i < 10; ++i )
      {
        client.create().creatingParentsIfNeeded().forPath("/test/" + Integer.toString(i));
        if ( r.nextBoolean() )
        {
          client.create().forPath("/test/" + Integer.toString(i) + "/foo");
          ++nonEmptyNodes;
        }
      }

      reaper = new ChildReaper(client, "/test", Reaper.Mode.REAP_UNTIL_DELETE, 1);
      reaper.start();

      timing.forWaiting().sleepABit();

      Stat    stat = client.checkExists().forPath("/test");
      Assert.assertEquals(stat.getNumChildren(), nonEmptyNodes);
    }
    finally
    {
      CloseableUtils.closeQuietly(reaper);
      CloseableUtils.closeQuietly(client);
    }
  }

  @Test
  public void     testSimple() throws Exception
  {
    Timing                  timing = new Timing();
    ChildReaper             reaper = null;
    CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
    try
    {
      client.start();

      for ( int i = 0; i < 10; ++i )
      {
        client.create().creatingParentsIfNeeded().forPath("/test/" + Integer.toString(i));
      }

      reaper = new ChildReaper(client, "/test", Reaper.Mode.REAP_UNTIL_DELETE, 1);
      reaper.start();

      timing.forWaiting().sleepABit();

      Stat    stat = client.checkExists().forPath("/test");
      Assert.assertEquals(stat.getNumChildren(), 0);
    }
    finally
    {
      CloseableUtils.closeQuietly(reaper);
      CloseableUtils.closeQuietly(client);
    }
  }

  @Test
  public void     testMultiPath() throws Exception
  {
    Timing                  timing = new Timing();
    ChildReaper             reaper = null;
    CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
    try
    {
      client.start();

      for ( int i = 0; i < 10; ++i )
      {
        client.create().creatingParentsIfNeeded().forPath("/test1/" + Integer.toString(i));
        client.create().creatingParentsIfNeeded().forPath("/test2/" + Integer.toString(i));
        client.create().creatingParentsIfNeeded().forPath("/test3/" + Integer.toString(i));
      }

      reaper = new ChildReaper(client, "/test2", Reaper.Mode.REAP_UNTIL_DELETE, 1);
      reaper.start();
      reaper.addPath("/test1");

      timing.forWaiting().sleepABit();

      Stat    stat = client.checkExists().forPath("/test1");
      Assert.assertEquals(stat.getNumChildren(), 0);
      stat = client.checkExists().forPath("/test2");
      Assert.assertEquals(stat.getNumChildren(), 0);
      stat = client.checkExists().forPath("/test3");
      Assert.assertEquals(stat.getNumChildren(), 10);
    }
    finally
    {
      CloseableUtils.closeQuietly(reaper);
      CloseableUtils.closeQuietly(client);
    }
  }

  @Test
  public void     testNamespace() throws Exception
  {
    Timing                  timing = new Timing();
    ChildReaper             reaper = null;
    CuratorFramework        client = CuratorFrameworkFactory.builder()
        .connectString(server.getConnectString())
        .sessionTimeoutMs(timing.session())
        .connectionTimeoutMs(timing.connection())
        .retryPolicy(new RetryOneTime(1))
        .namespace("foo")
        .build();
    try
    {
      client.start();

      for ( int i = 0; i < 10; ++i )
      {
        client.create().creatingParentsIfNeeded().forPath("/test/" + Integer.toString(i));
      }

      reaper = new ChildReaper(client, "/test", Reaper.Mode.REAP_UNTIL_DELETE, 1);
      reaper.start();

      timing.forWaiting().sleepABit();

      Stat    stat = client.checkExists().forPath("/test");
      Assert.assertEquals(stat.getNumChildren(), 0);

      stat = client.usingNamespace(null).checkExists().forPath("/foo/test");
      Assert.assertNotNull(stat);
      Assert.assertEquals(stat.getNumChildren(), 0);
    }
    finally
    {
      CloseableUtils.closeQuietly(reaper);
      CloseableUtils.closeQuietly(client);
    }
  }
}
