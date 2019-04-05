/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.protocolPB
    .StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Collections.unmodifiableList;
import static org.apache.hadoop.hdds.scm.HddsServerUtil
    .getScmRpcTimeOutInMilliseconds;

/**
 * SCMConnectionManager - Acts as a class that manages the membership
 * information of the SCMs that we are working with.
 */
public class SCMConnectionManager
    implements Closeable, SCMConnectionManagerMXBean {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMConnectionManager.class);

  private final ReadWriteLock mapLock;
  private final Map<InetSocketAddress, EndpointStateMachine> scmMachines;

  private final int rpcTimeout;
  private final Configuration conf;
  private ObjectName jmxBean;

  public SCMConnectionManager(Configuration conf) {
    this.mapLock = new ReentrantReadWriteLock();
    Long timeOut = getScmRpcTimeOutInMilliseconds(conf);
    this.rpcTimeout = timeOut.intValue();
    this.scmMachines = new HashMap<>();
    this.conf = conf;
    jmxBean = MBeans.register("HddsDatanode",
        "SCMConnectionManager",
        this);
  }


  /**
   * Returns Config.
   *
   * @return ozoneConfig.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Get RpcTimeout.
   *
   * @return - Return RPC timeout.
   */
  public int getRpcTimeout() {
    return rpcTimeout;
  }


  /**
   * Takes a read lock.
   */
  public void readLock() {
    this.mapLock.readLock().lock();
  }

  /**
   * Releases the read lock.
   */
  public void readUnlock() {
    this.mapLock.readLock().unlock();
  }

  /**
   * Takes the write lock.
   */
  public void writeLock() {
    this.mapLock.writeLock().lock();
  }

  /**
   * Releases the write lock.
   */
  public void writeUnlock() {
    this.mapLock.writeLock().unlock();
  }

  /**
   * adds a new SCM machine to the target set.
   *
   * @param address - Address of the SCM machine to send heatbeat to.
   * @throws IOException
   */
  public void addSCMServer(InetSocketAddress address) throws IOException {
    writeLock();
    try {
      if (scmMachines.containsKey(address)) {
        LOG.warn("Trying to add an existing SCM Machine to Machines group. " +
            "Ignoring the request.");
        return;
      }
      RPC.setProtocolEngine(conf, StorageContainerDatanodeProtocolPB.class,
          ProtobufRpcEngine.class);
      long version =
          RPC.getProtocolVersion(StorageContainerDatanodeProtocolPB.class);

      StorageContainerDatanodeProtocolPB rpcProxy = RPC.getProxy(
          StorageContainerDatanodeProtocolPB.class, version,
          address, UserGroupInformation.getCurrentUser(), conf,
          NetUtils.getDefaultSocketFactory(conf), getRpcTimeout());

      StorageContainerDatanodeProtocolClientSideTranslatorPB rpcClient =
          new StorageContainerDatanodeProtocolClientSideTranslatorPB(rpcProxy);

      EndpointStateMachine endPoint =
          new EndpointStateMachine(address, rpcClient, conf);
      scmMachines.put(address, endPoint);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Removes a  SCM machine for the target set.
   *
   * @param address - Address of the SCM machine to send heatbeat to.
   * @throws IOException
   */
  public void removeSCMServer(InetSocketAddress address) throws IOException {
    writeLock();
    try {
      if (!scmMachines.containsKey(address)) {
        LOG.warn("Trying to remove a non-existent SCM machine. " +
            "Ignoring the request.");
        return;
      }

      EndpointStateMachine endPoint = scmMachines.get(address);
      endPoint.close();
      scmMachines.remove(address);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Returns all known RPCEndpoints.
   *
   * @return - List of RPC Endpoints.
   */
  public Collection<EndpointStateMachine> getValues() {
    readLock();
    try {
      return unmodifiableList(new ArrayList<>(scmMachines.values()));
    } finally {
      readUnlock();
    }
  }

  @Override
  public void close() throws IOException {
    getValues().forEach(endpointStateMachine
        -> IOUtils.cleanupWithLogger(LOG, endpointStateMachine));
    if (jmxBean != null) {
      MBeans.unregister(jmxBean);
      jmxBean = null;
    }
  }

  @Override
  public List<EndpointStateMachineMBean> getSCMServers() {
    readLock();
    try {
      return unmodifiableList(new ArrayList<>(scmMachines.values()));
    } finally {
      readUnlock();
    }
  }
}
