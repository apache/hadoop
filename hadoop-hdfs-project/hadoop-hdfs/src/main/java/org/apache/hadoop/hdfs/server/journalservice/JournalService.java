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
package org.apache.hadoop.hdfs.server.journalservice;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalProtocolService;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.FenceResponse;
import org.apache.hadoop.hdfs.server.protocol.FencedException;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

/**
 * This class interfaces with the namenode using {@link JournalProtocol} over
 * RPC. It has two modes: <br>
 * <ul>
 * <li>Mode where an RPC.Server is provided from outside, on which it
 * {@link JournalProtocol} is registered. The RPC.Server stop and start is
 * managed outside by the application.</li>
 * <li>Stand alone mode where an RPC.Server is started and managed by the
 * JournalListener.</li>
 * </ul>
 * 
 * The received journal operations are sent to a listener over callbacks. The
 * listener implementation can handle the callbacks based on the application
 * requirement.
 */
public class JournalService implements JournalProtocol {
  public static final Log LOG = LogFactory.getLog(JournalService.class.getName());

  private final JournalListener listener;
  private final InetSocketAddress nnAddress;
  private final NamenodeRegistration registration;
  private final NamenodeProtocol namenode;
  private final StateHandler stateHandler = new StateHandler();
  private final RPC.Server rpcServer;
  private long epoch = 0;
  private String fencerInfo;
  
  enum State {
    /** The service is initialized and ready to start. */
    INIT(false, false),
    /**
     * RPC server is started.
     * The service is ready to receive requests from namenode.
     */
    STARTED(false, false),
    /** The service is fenced by a namenode and waiting for roll. */
    WAITING_FOR_ROLL(false, true),
    /**
     * The existing log is syncing with another source
     * and it accepts journal from Namenode.
     */
    SYNCING(true, true),
    /** The existing log is in sync and it accepts journal from Namenode. */
    IN_SYNC(true, true),
    /** The service is stopped. */
    STOPPED(false, false);

    final boolean isJournalAllowed;
    final boolean isStartLogSegmentAllowed;
    
    State(boolean isJournalAllowed, boolean isStartLogSegmentAllowed) {
      this.isJournalAllowed = isJournalAllowed;
      this.isStartLogSegmentAllowed = isStartLogSegmentAllowed;
    }
  }
  
  static class StateHandler {
    State current = State.INIT;
    
    synchronized void start() {
      if (current != State.INIT) {
        throw new IllegalStateException("Service cannot be started in "
            + current + " state.");
      }
      current = State.STARTED;
    }

    synchronized void waitForRoll() {
      if (current != State.STARTED) {
        throw new IllegalStateException("Cannot wait-for-roll in " + current
            + " state.");
      }
      current = State.WAITING_FOR_ROLL;
    }

    synchronized void startLogSegment() {
      if (current == State.WAITING_FOR_ROLL) {
        current = State.SYNCING;
      }
    }

    synchronized void isStartLogSegmentAllowed() throws IOException {
      if (!current.isStartLogSegmentAllowed) {
        throw new IOException("Cannot start log segment in " + current
            + " state.");
      }
    }

    synchronized void isJournalAllowed() throws IOException {
      if (!current.isJournalAllowed) {
        throw new IOException("Cannot journal in " + current + " state.");
      }
    }

    synchronized boolean isStopped() {
      if (current == State.STOPPED) {
        LOG.warn("Ignore stop request since the service is in " + current
            + " state.");
        return true;
      }
      current = State.STOPPED;
      return false;
    }
  }
  
  /**
   * Constructor to create {@link JournalService} where an RPC server is
   * created by this service.
   * @param conf Configuration
   * @param nnAddr host:port for the active Namenode's RPC server
   * @param serverAddress address to start RPC server to receive
   *          {@link JournalProtocol} requests. This can be null, if
   *          {@code server} is a valid server that is managed out side this
   *          service.
   * @param listener call-back interface to listen to journal activities
   * @throws IOException on error
   */
  JournalService(Configuration conf, InetSocketAddress nnAddr,
      InetSocketAddress serverAddress, JournalListener listener)
      throws IOException {
    this.nnAddress = nnAddr;
    this.listener = listener;
    this.namenode = NameNodeProxies.createNonHAProxy(conf, nnAddr,
        NamenodeProtocol.class, UserGroupInformation.getCurrentUser(), true)
        .getProxy();
    this.rpcServer = createRpcServer(conf, serverAddress, this);

    String addr = NetUtils.getHostPortString(rpcServer.getListenerAddress());
    StorageInfo storage = new StorageInfo(
        LayoutVersion.getCurrentLayoutVersion(), 0, "", 0);
    registration = new NamenodeRegistration(addr, "", storage,
        NamenodeRole.BACKUP);
  }
  
  /**
   * Start the service.
   */
  public void start() {
    stateHandler.start();

    // Start the RPC server
    LOG.info("Starting rpc server");
    rpcServer.start();

    for(boolean registered = false, handshakeComplete = false; ; ) {
      try {
        // Perform handshake
        if (!handshakeComplete) {
          handshake();
          handshakeComplete = true;
          LOG.info("handshake completed");
        }
        
        // Register with the namenode
        if (!registered) {
          registerWithNamenode();
          registered = true;
          LOG.info("Registration completed");
          break;
        }
      } catch (IOException ioe) {
        LOG.warn("Encountered exception ", ioe);
      } catch (Exception e) {
        LOG.warn("Encountered exception ", e);
      }
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        LOG.warn("Encountered exception ", ie);
      }
    }

    stateHandler.waitForRoll();
    try {
      namenode.rollEditLog();
    } catch (IOException e) {
      LOG.warn("Encountered exception ", e);
    }
  }

  /**
   * Stop the service. For application with RPC Server managed outside, the
   * RPC Server must be stopped the application.
   */
  public void stop() {
    if (!stateHandler.isStopped()) {
      rpcServer.stop();
    }
  }

  @Override
  public void journal(JournalInfo journalInfo, long epoch, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received journal " + firstTxnId + " " + numTxns);
    }
    stateHandler.isJournalAllowed();
    verify(epoch, journalInfo);
    listener.journal(this, firstTxnId, numTxns, records);
  }

  @Override
  public void startLogSegment(JournalInfo journalInfo, long epoch, long txid)
      throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received startLogSegment " + txid);
    }
    stateHandler.isStartLogSegmentAllowed();
    verify(epoch, journalInfo);
    listener.startLogSegment(this, txid);
    stateHandler.startLogSegment();
  }

  @Override
  public FenceResponse fence(JournalInfo journalInfo, long epoch,
      String fencerInfo) throws IOException {
    LOG.info("Fenced by " + fencerInfo + " with epoch " + epoch);
    verifyFence(epoch, fencerInfo);
    verify(journalInfo);
    long previousEpoch = epoch;
    this.epoch = epoch;
    this.fencerInfo = fencerInfo;
    
    // TODO:HDFS-3092 set lastTransId and inSync
    return new FenceResponse(previousEpoch, 0, false);
  }

  /** Create an RPC server. */
  private static RPC.Server createRpcServer(Configuration conf,
      InetSocketAddress address, JournalProtocol impl) throws IOException {
    RPC.setProtocolEngine(conf, JournalProtocolPB.class,
        ProtobufRpcEngine.class);
    JournalProtocolServerSideTranslatorPB xlator = 
        new JournalProtocolServerSideTranslatorPB(impl);
    BlockingService service = 
        JournalProtocolService.newReflectiveBlockingService(xlator);
    return new RPC.Builder(conf).setProtocol(JournalProtocolPB.class)
        .setInstance(service).setBindAddress(address.getHostName())
        .setPort(address.getPort()).setNumHandlers(1).setVerbose(false).build();
  }
  
  private void verifyEpoch(long e) throws FencedException {
    if (epoch != e) {
      String errorMsg = "Epoch " + e + " is not valid. "
          + "Resource has already been fenced by " + fencerInfo
          + " with epoch " + epoch;
      LOG.warn(errorMsg);
      throw new FencedException(errorMsg);
    }
  }
  
  private void verifyFence(long e, String fencer) throws FencedException {
    if (e <= epoch) {
      String errorMsg = "Epoch " + e + " from fencer " + fencer
          + " is not valid. " + "Resource has already been fenced by "
          + fencerInfo + " with epoch " + epoch;
      LOG.warn(errorMsg);
      throw new FencedException(errorMsg);
    }
  }
  
  /** 
   * Verifies a journal request
   */
  private void verify(JournalInfo journalInfo) throws IOException {
    String errorMsg = null;
    int expectedNamespaceID = registration.getNamespaceID();
    if (journalInfo.getNamespaceId() != expectedNamespaceID) {
      errorMsg = "Invalid namespaceID in journal request - expected " + expectedNamespaceID
          + " actual " + journalInfo.getNamespaceId();
      LOG.warn(errorMsg);
      throw new UnregisteredNodeException(journalInfo);
    } 
    if (!journalInfo.getClusterId().equals(registration.getClusterID())) {
      errorMsg = "Invalid clusterId in journal request - expected "
          + journalInfo.getClusterId() + " actual " + registration.getClusterID();
      LOG.warn(errorMsg);
      throw new UnregisteredNodeException(journalInfo);
    }
  }
  
  /** 
   * Verifies a journal request
   */
  private void verify(long e, JournalInfo journalInfo) throws IOException {
    verifyEpoch(e);
    verify(journalInfo);
  }
  
  /**
   * Register this service with the active namenode.
   */
  private void registerWithNamenode() throws IOException {
    NamenodeRegistration nnReg = namenode.register(registration);
    String msg = null;
    if(nnReg == null) { // consider as a rejection
      msg = "Registration rejected by " + nnAddress;
    } else if(!nnReg.isRole(NamenodeRole.NAMENODE)) {
      msg = " Name-node " + nnAddress + " is not active";
    }
    if(msg != null) {
      LOG.error(msg);
      throw new IOException(msg); // stop the node
    }
  }
  
  private void handshake() throws IOException {
    NamespaceInfo nsInfo = namenode.versionRequest();
    listener.verifyVersion(this, nsInfo);
    registration.setStorageInfo(nsInfo);
  }

  @VisibleForTesting
  long getEpoch() {
    return epoch;
  }
}
