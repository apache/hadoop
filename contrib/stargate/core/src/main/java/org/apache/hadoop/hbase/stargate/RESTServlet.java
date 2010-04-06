/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.stargate;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.stargate.auth.Authenticator;
import org.apache.hadoop.hbase.stargate.auth.HBCAuthenticator;
import org.apache.hadoop.hbase.stargate.auth.HTableAuthenticator;
import org.apache.hadoop.hbase.stargate.auth.JDBCAuthenticator;
import org.apache.hadoop.hbase.stargate.auth.ZooKeeperAuthenticator;
import org.apache.hadoop.hbase.stargate.metrics.StargateMetrics;
import org.apache.hadoop.hbase.stargate.util.HTableTokenBucket;
import org.apache.hadoop.hbase.stargate.util.SoftUserData;
import org.apache.hadoop.hbase.stargate.util.UserData;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

import org.apache.hadoop.util.StringUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import org.json.JSONStringer;

import com.sun.jersey.server.impl.container.servlet.ServletAdaptor;

/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
public class RESTServlet extends ServletAdaptor 
    implements Constants, Watcher {

  private static final Log LOG = LogFactory.getLog(RESTServlet.class);
  private static final long serialVersionUID = 1L;

  private static RESTServlet instance;

  class StatusReporter extends Chore {

    public StatusReporter(int period, AtomicBoolean stopping) {
      super(period, stopping);
    }

    @Override
    protected void chore() {
      if (wrapper != null) try {
        JSONStringer status = new JSONStringer();
        status.object();
        status.key("requests").value(metrics.getRequests());
        status.key("connectors").array();
        for (Pair<String,Integer> e: connectors) {
          status.object()
            .key("host").value(e.getFirst())
            .key("port").value(e.getSecond())
            .endObject();
        }
        status.endArray();
        status.endObject();
        updateNode(wrapper, znode, CreateMode.EPHEMERAL, 
          Bytes.toBytes(status.toString()));
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
      }
    }

  }

  final String znode = INSTANCE_ZNODE_ROOT + "/" + System.currentTimeMillis();
  transient final Configuration conf;
  transient final HTablePool pool;
  transient volatile ZooKeeperWrapper wrapper;
  transient Chore statusReporter;
  transient Authenticator authenticator;
  AtomicBoolean stopping = new AtomicBoolean(false);
  boolean multiuser;
  Map<String,Integer> maxAgeMap = 
    Collections.synchronizedMap(new HashMap<String,Integer>());
  List<Pair<String,Integer>> connectors = 
    Collections.synchronizedList(new ArrayList<Pair<String,Integer>>());
  StargateMetrics metrics = new StargateMetrics();

  /**
   * @return the RESTServlet singleton instance
   * @throws IOException
   */
  public synchronized static RESTServlet getInstance() throws IOException {
    if (instance == null) {
      instance = new RESTServlet();
    }
    return instance;
  }

  static boolean ensureExists(final ZooKeeperWrapper zkw, final String znode,
      final CreateMode mode) throws IOException {
    ZooKeeper zk = zkw.getZooKeeper();
    try {
      Stat stat = zk.exists(znode, false);
      if (stat != null) {
        return true;
      }
      zk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE, mode);
      LOG.debug("Created ZNode " + znode);
      return true;
    } catch (KeeperException.NodeExistsException e) {
      return true;      // ok, move on.
    } catch (KeeperException.NoNodeException e) {
      return ensureParentExists(zkw, znode, mode) && 
        ensureExists(zkw, znode, mode);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  static boolean ensureParentExists(final ZooKeeperWrapper zkw,
      final String znode, final CreateMode mode) throws IOException {
    int index = znode.lastIndexOf("/");
    if (index <= 0) {   // Parent is root, which always exists.
      return true;
    }
    return ensureExists(zkw, znode.substring(0, index), mode);
  }

  static void updateNode(final ZooKeeperWrapper zkw, final String znode, 
        final CreateMode mode, final byte[] data) throws IOException  {
    ensureExists(zkw, znode, mode);
    ZooKeeper zk = zkw.getZooKeeper();
    try {
      zk.setData(znode, data, -1);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  ZooKeeperWrapper initZooKeeperWrapper() throws IOException {
    return new ZooKeeperWrapper(conf, this);
  }

  /**
   * Constructor
   * @throws IOException
   */
  public RESTServlet() throws IOException {
    this.conf = HBaseConfiguration.create();
    this.pool = new HTablePool(conf, 10);
    this.wrapper = initZooKeeperWrapper();
    this.statusReporter = new StatusReporter(
      conf.getInt(STATUS_REPORT_PERIOD_KEY, 1000 * 60), stopping);
    this.multiuser = conf.getBoolean("stargate.multiuser", false);
    if (this.multiuser) {
      LOG.info("multiuser mode enabled");
      getAuthenticator();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    LOG.debug(("ZooKeeper.Watcher event " + event.getType() + " with path " +
      event.getPath()));
    // handle disconnection (or manual delete to test disconnection scenario)
    if (event.getState() == KeeperState.Expired || 
        (event.getType().equals(EventType.NodeDeleted) && 
            event.getPath().equals(znode))) {
      wrapper.close();
      wrapper = null;
      while (!stopping.get()) try {
        wrapper = initZooKeeperWrapper();
        break;
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        try {
          Thread.sleep(10 * 1000);
        } catch (InterruptedException ex) {
        }
      }
    }
  }

  HTablePool getTablePool() {
    return pool;
  }

  ZooKeeperWrapper getZooKeeperWrapper() {
    return wrapper;
  }

  Configuration getConfiguration() {
    return conf;
  }

  StargateMetrics getMetrics() {
    return metrics;
  }

  void addConnectorAddress(String host, int port) {
    connectors.add(new Pair<String,Integer>(host, port));
  }

  /**
   * @param tableName the table name
   * @return the maximum cache age suitable for use with this table, in
   *  seconds 
   * @throws IOException
   */
  public int getMaxAge(String tableName) throws IOException {
    Integer i = maxAgeMap.get(tableName);
    if (i != null) {
      return i.intValue();
    }
    HTableInterface table = pool.getTable(tableName);
    try {
      int maxAge = DEFAULT_MAX_AGE;
      for (HColumnDescriptor family : 
          table.getTableDescriptor().getFamilies()) {
        int ttl = family.getTimeToLive();
        if (ttl < 0) {
          continue;
        }
        if (ttl < maxAge) {
          maxAge = ttl;
        }
      }
      maxAgeMap.put(tableName, maxAge);
      return maxAge;
    } finally {
      pool.putTable(table);
    }
  }

  /**
   * Signal that a previously calculated maximum cache age has been
   * invalidated by a schema change.
   * @param tableName the table name
   */
  public void invalidateMaxAge(String tableName) {
    maxAgeMap.remove(tableName);
  }

  /**
   * @return true if the servlet should operate in multiuser mode
   */
  public boolean isMultiUser() {
    return multiuser;
  }

  /**
   * @param flag true if the servlet should operate in multiuser mode 
   */
  public void setMultiUser(boolean multiuser) {
    this.multiuser = multiuser;
  }

  /**
   * @return an authenticator
   */
  public Authenticator getAuthenticator() {
    if (authenticator == null) {
      String className = conf.get(AUTHENTICATOR_KEY,
        HBCAuthenticator.class.getCanonicalName());
      try {
        Class<?> c = getClass().getClassLoader().loadClass(className);
        if (className.endsWith(HBCAuthenticator.class.getName()) ||
            className.endsWith(HTableAuthenticator.class.getName()) ||
            className.endsWith(JDBCAuthenticator.class.getName())) {
          Constructor<?> cons = c.getConstructor(Configuration.class);
          authenticator = (Authenticator)
            cons.newInstance(new Object[] { conf });
        } else if (className.endsWith(ZooKeeperAuthenticator.class.getName())) {
          Constructor<?> cons = c.getConstructor(Configuration.class,
            ZooKeeperWrapper.class);
          authenticator = (Authenticator)
            cons.newInstance(new Object[] { conf, wrapper });
        } else {
          authenticator = (Authenticator)c.newInstance();
        }
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
      }
      if (authenticator == null) {
        authenticator = new HBCAuthenticator(conf);
      }
      LOG.info("using authenticator " + authenticator);
    }
    return authenticator;
  }

  /**
   * @param authenticator
   */
  public void setAuthenticator(Authenticator authenticator) {
    this.authenticator = authenticator;
  }

  /**
   * Check if the user has exceeded their request token limit within the
   * current interval
   * @param user the user
   * @param want the number of tokens desired
   * @throws IOException
   */
  public boolean userRequestLimit(final User user, int want)
      throws IOException {
    if (multiuser) {
      UserData ud = SoftUserData.get(user);
      HTableTokenBucket tb = (HTableTokenBucket) ud.get(UserData.TOKENBUCKET);
      if (tb == null) {
        tb = new HTableTokenBucket(conf, Bytes.toBytes(user.getToken()));
        ud.put(UserData.TOKENBUCKET, tb);
      }
      if (tb.available() < want) {
        return false;
      }
      tb.remove(want);
    }
    return true;
  }

}
