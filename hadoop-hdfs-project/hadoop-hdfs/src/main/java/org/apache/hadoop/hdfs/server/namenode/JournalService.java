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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalProtocolService;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

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
  private final boolean internalRpcServer;
  private final InetSocketAddress nnAddress;
  private final NamenodeRegistration registration;
  private final NamenodeProtocol namenode;
  private volatile State state = State.INIT;
  private RPC.Server rpcServer;
  
  enum State {
    INIT,
    STARTING_UP,
    RUNNING,
    STOPPED;
  }
  
  /**
   * JournalListener is a callback interface to handle journal records
   * received from the namenode.
   */
  public interface JournalListener {
    /**
     * Check the namespace information returned by a namenode
     * @param service service that is making the callback
     * @param info returned namespace information from the namenode
     * 
     * The application using {@link JournalService} can stop the service if
     * {@code info} validation fails.
     */
    public void verifyVersion(JournalService service, NamespaceInfo info);
    
    /**
     * Process the received Journal record
     * @param service {@link JournalService} making the callback
     * @param firstTxnId first transaction Id in the journal
     * @param numTxns number of records
     * @param records journal records
     * @throws IOException on error
     * 
     * Any IOException thrown from the listener is thrown back in 
     * {@link JournalProtocol#journal}
     */
    public void journal(JournalService service, long firstTxnId, int numTxns,
        byte[] records) throws IOException;
    
    /**
     * Roll the editlog
     * @param service {@link JournalService} making the callback
     * @param txid transaction ID to roll at
     * 
     * Any IOException thrown from the listener is thrown back in 
     * {@link JournalProtocol#startLogSegment}
     */
    public void rollLogs(JournalService service, long txid) throws IOException;
  }
  
  /**
   * Constructor to create {@link JournalService} based on an existing RPC server.
   * After creating the service, the caller needs to start the RPC server.
   * 
   * @param conf Configuration
   * @param nnAddr host:port for the active Namenode's RPC server
   * @param listener call-back interface to listen to journal activities
   * @param rpcServer RPC server if the application has already one, which can be
   *          reused. If this is null, then the RPC server is started by
   *          {@link JournalService}
   * @param reg namenode registration information if there is one already, say
   *          if you are using this service in namenode. If it is null, then the
   *          service creates a new registration.
   * @throws IOException on error
   */
  JournalService(Configuration conf, InetSocketAddress nnAddr,
      JournalListener listener, RPC.Server rpcServer, NamenodeRegistration reg)
      throws IOException {
    this.nnAddress = nnAddr;
    this.listener = listener;
    this.registration = reg;
    this.internalRpcServer = false;
    this.namenode = NameNodeProxies.createNonHAProxy(conf, nnAddr,
        NamenodeProtocol.class, UserGroupInformation.getCurrentUser(), true)
        .getProxy();
    initRpcServer(conf, rpcServer);
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
    this.internalRpcServer = true;
    this.namenode = NameNodeProxies.createNonHAProxy(conf, nnAddr,
        NamenodeProtocol.class, UserGroupInformation.getCurrentUser(), true)
        .getProxy();
    initRpcServer(conf, serverAddress);
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
    synchronized(this) {
      if (state != State.INIT) {
        LOG.info("Service cannot be started in state - " + state);
        return;
      }
      state = State.STARTING_UP;
    }
    // Start the RPC server
    if (internalRpcServer) {
      LOG.info("Starting rpc server");
      rpcServer.start();
    }
    
    boolean registered = false;
    boolean handshakeComplete = false;
    boolean rollEdits = false;
    while (state == State.STARTING_UP) {
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
        }
        
        if (!rollEdits) {
          namenode.rollEditLog();
          rollEdits = true;
          LOG.info("Editlog roll completed");
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
    synchronized(this) {
      state = State.RUNNING;
    }
    
  }

  /**
   * Stop the service. For application with RPC Server managed outside, the
   * RPC Server must be stopped the application.
   */
  public void stop() {
    synchronized (this) {
      if (state == State.STOPPED) {
        return;
      }
      state = State.STOPPED;
    }
    if (internalRpcServer && rpcServer != null) {
      rpcServer.stop();
      rpcServer = null;
    }
  }

  @Override
  public void journal(NamenodeRegistration registration, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received journal " + firstTxnId + " " + numTxns);
    }
    verify(registration);
    listener.journal(this, firstTxnId, numTxns, records);
  }

  @Override
  public void startLogSegment(NamenodeRegistration registration, long txid)
      throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received startLogSegment " + txid);
    }
    verify(registration);
    listener.rollLogs(this, txid);
  }

  /** 
   * Stand alone mode where RPC Server is created and managed by this service 
   */
  private void initRpcServer(Configuration conf, InetSocketAddress serverAddress)
      throws IOException {
    RPC.setProtocolEngine(conf, JournalProtocolPB.class,
        ProtobufRpcEngine.class);
    JournalProtocolServerSideTranslatorPB xlator = 
        new JournalProtocolServerSideTranslatorPB(this);
    BlockingService service = 
        JournalProtocolService.newReflectiveBlockingService(xlator);
    rpcServer = RPC.getServer(JournalProtocolPB.class, service,
        serverAddress.getHostName(), serverAddress.getPort(), 1, false, conf,
        null);
  }

  /**
   * RPC Server is created and managed by the application - used by this service
   */
  private void initRpcServer(Configuration conf, RPC.Server server)
      throws IOException {
    rpcServer = server;
    JournalProtocolServerSideTranslatorPB xlator = 
        new JournalProtocolServerSideTranslatorPB(this);
    BlockingService service = 
        JournalProtocolService.newReflectiveBlockingService(xlator);
    DFSUtil.addPBProtocol(conf, JournalProtocolPB.class, service, rpcServer);
  }
  
  private void verify(NamenodeRegistration reg) throws IOException {
    if (!registration.getRegistrationID().equals(reg.getRegistrationID())) {
      LOG.warn("Invalid registrationID - expected: "
          + registration.getRegistrationID() + " received: "
          + reg.getRegistrationID());
      throw new UnregisteredNodeException(reg);
    }
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
}