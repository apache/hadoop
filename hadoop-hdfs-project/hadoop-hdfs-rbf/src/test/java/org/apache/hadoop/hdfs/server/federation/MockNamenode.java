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
package org.apache.hadoop.hdfs.server.federation;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.NamenodeProtocolService;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.protobuf.BlockingService;


/**
 * Mock for the network interfaces (e.g., RPC and HTTP) of a Namenode. This is
 * used by the Routers in a mock cluster.
 */
public class MockNamenode {

  private static final Logger LOG =
      LoggerFactory.getLogger(MockNamenode.class);


  /** Mock implementation of the Namenode. */
  private final NamenodeProtocols mockNn;

  /** Name service identifier (subcluster). */
  private String nsId;
  /** HA state of the Namenode. */
  private HAServiceState haState = HAServiceState.STANDBY;
  /** Datanodes registered in this Namenode. */
  private List<DatanodeInfo> dns = new ArrayList<>();

  /** RPC server of the Namenode that redirects calls to the mock. */
  private Server rpcServer;
  /** HTTP server of the Namenode that redirects calls to the mock. */
  private HttpServer2 httpServer;


  public MockNamenode(final String nsIdentifier) throws IOException {
    this(nsIdentifier, new HdfsConfiguration());
  }

  public MockNamenode(final String nsIdentifier, final Configuration conf)
      throws IOException {
    this.nsId = nsIdentifier;
    this.mockNn = mock(NamenodeProtocols.class);
    setupMock();
    setupRPCServer(conf);
    setupHTTPServer(conf);
  }

  /**
   * Setup the mock of the Namenode. It offers the basic functionality for
   * Routers to get the status.
   * @throws IOException If the mock cannot be setup.
   */
  protected void setupMock() throws IOException {
    NamespaceInfo nsInfo = new NamespaceInfo(1, this.nsId, this.nsId, 1);
    when(mockNn.versionRequest()).thenReturn(nsInfo);

    when(mockNn.getServiceStatus()).thenAnswer(new Answer<HAServiceStatus>() {
      @Override
      public HAServiceStatus answer(InvocationOnMock invocation)
          throws Throwable {
        HAServiceStatus haStatus = new HAServiceStatus(getHAServiceState());
        haStatus.setNotReadyToBecomeActive("");
        return haStatus;
      }
    });
  }

  /**
   * Setup the RPC server of the Namenode that redirects calls to the mock.
   * @param conf Configuration of the server.
   * @throws IOException If the RPC server cannot be setup.
   */
  private void setupRPCServer(final Configuration conf) throws IOException {
    RPC.setProtocolEngine(
        conf, ClientNamenodeProtocolPB.class, ProtobufRpcEngine2.class);
    ClientNamenodeProtocolServerSideTranslatorPB
        clientNNProtoXlator =
            new ClientNamenodeProtocolServerSideTranslatorPB(mockNn);
    BlockingService clientNNPbService =
        ClientNamenodeProtocol.newReflectiveBlockingService(
            clientNNProtoXlator);

    int numHandlers = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_DEFAULT);

    rpcServer = new RPC.Builder(conf)
        .setProtocol(ClientNamenodeProtocolPB.class)
        .setInstance(clientNNPbService)
        .setBindAddress("0.0.0.0")
        .setPort(0)
        .setNumHandlers(numHandlers)
        .build();

    NamenodeProtocolServerSideTranslatorPB nnProtoXlator =
        new NamenodeProtocolServerSideTranslatorPB(mockNn);
    BlockingService nnProtoPbService =
        NamenodeProtocolService.newReflectiveBlockingService(
            nnProtoXlator);
    DFSUtil.addPBProtocol(
        conf, NamenodeProtocolPB.class, nnProtoPbService, rpcServer);

    DatanodeProtocolServerSideTranslatorPB dnProtoPbXlator =
        new DatanodeProtocolServerSideTranslatorPB(mockNn, 1000);
    BlockingService dnProtoPbService =
        DatanodeProtocolService.newReflectiveBlockingService(
            dnProtoPbXlator);
    DFSUtil.addPBProtocol(
        conf, DatanodeProtocolPB.class, dnProtoPbService, rpcServer);

    HAServiceProtocolServerSideTranslatorPB haServiceProtoXlator =
        new HAServiceProtocolServerSideTranslatorPB(mockNn);
    BlockingService haProtoPbService =
        HAServiceProtocolService.newReflectiveBlockingService(
            haServiceProtoXlator);
    DFSUtil.addPBProtocol(
        conf, HAServiceProtocolPB.class, haProtoPbService, rpcServer);

    this.rpcServer.addTerseExceptions(
        RemoteException.class,
        SafeModeException.class,
        FileNotFoundException.class,
        FileAlreadyExistsException.class,
        AccessControlException.class,
        LeaseExpiredException.class,
        NotReplicatedYetException.class,
        IOException.class,
        ConnectException.class,
        StandbyException.class);

    rpcServer.start();
  }

  /**
   * Setup the HTTP server of the Namenode that redirects calls to the mock.
   * @param conf Configuration of the server.
   * @throws IOException If the HTTP server cannot be setup.
   */
  private void setupHTTPServer(Configuration conf) throws IOException {
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setName("hdfs")
        .setConf(conf)
        .setACL(new AccessControlList(conf.get(DFS_ADMIN, " ")))
        .addEndpoint(URI.create("http://0.0.0.0:0"));
    httpServer = builder.build();
    httpServer.start();
  }

  /**
   * Get the RPC port for the Mock Namenode.
   * @return RPC port.
   */
  public int getRPCPort() {
    return rpcServer.getListenerAddress().getPort();
  }

  /**
   * Get the HTTP port for the Mock Namenode.
   * @return HTTP port.
   */
  public int getHTTPPort() {
    return httpServer.getConnectorAddress(0).getPort();
  }

  /**
   * Get the Mock core. This is used to extend the mock.
   * @return Mock Namenode protocol to be extended.
   */
  public NamenodeProtocols getMock() {
    return mockNn;
  }

  /**
   * Get the name service id (subcluster) of the Mock Namenode.
   * @return Name service identifier.
   */
  public String getNameserviceId() {
    return nsId;
  }

  /**
   * Get the HA state of the Mock Namenode.
   * @return HA state (ACTIVE or STANDBY).
   */
  public HAServiceState getHAServiceState() {
    return haState;
  }

  /**
   * Show the Mock Namenode as Active.
   */
  public void transitionToActive() {
    this.haState = HAServiceState.ACTIVE;
  }

  /**
   * Show the Mock Namenode as Standby.
   */
  public void transitionToStandby() {
    this.haState = HAServiceState.STANDBY;
  }

  /**
   * Get the datanodes that this NN will return.
   * @return The datanodes that this NN will return.
   */
  public List<DatanodeInfo> getDatanodes() {
    return this.dns;
  }

  /**
   * Stop the Mock Namenode. It stops all the servers.
   * @throws Exception If it cannot stop the Namenode.
   */
  public void stop() throws Exception {
    if (rpcServer != null) {
      rpcServer.stop();
      rpcServer = null;
    }
    if (httpServer != null) {
      httpServer.stop();
      httpServer = null;
    }
  }

  /**
   * Add the mock for the FileSystem calls in ClientProtocol.
   * @throws IOException If it cannot be setup.
   */
  public void addFileSystemMock() throws IOException {
    final SortedMap<String, String> fs =
        new ConcurrentSkipListMap<String, String>();

    DirectoryListing l = mockNn.getListing(anyString(), any(), anyBoolean());
    when(l).thenAnswer(invocation -> {
      String src = getSrc(invocation);
      LOG.info("{} getListing({})", nsId, src);
      if (fs.get(src) == null) {
        throw new FileNotFoundException("File does not exist " + src);
      }
      if (!src.endsWith("/")) {
        src += "/";
      }
      Map<String, String> files =
          fs.subMap(src, src + Character.MAX_VALUE);
      List<HdfsFileStatus> list = new ArrayList<>();
      for (String file : files.keySet()) {
        if (file.substring(src.length()).indexOf('/') < 0) {
          HdfsFileStatus fileStatus =
              getMockHdfsFileStatus(file, fs.get(file));
          list.add(fileStatus);
        }
      }
      HdfsFileStatus[] array = list.toArray(
          new HdfsFileStatus[list.size()]);
      return new DirectoryListing(array, 0);
    });
    when(mockNn.getFileInfo(anyString())).thenAnswer(invocation -> {
      String src = getSrc(invocation);
      LOG.info("{} getFileInfo({})", nsId, src);
      return getMockHdfsFileStatus(src, fs.get(src));
    });
    HdfsFileStatus c = mockNn.create(anyString(), any(), anyString(), any(),
        anyBoolean(), anyShort(), anyLong(), any(), any(), any());
    when(c).thenAnswer(invocation -> {
      String src = getSrc(invocation);
      LOG.info("{} create({})", nsId, src);
      boolean createParent = (boolean)invocation.getArgument(4);
      if (createParent) {
        Path path = new Path(src).getParent();
        while (!path.isRoot()) {
          LOG.info("{} create parent {}", nsId, path);
          fs.put(path.toString(), "DIRECTORY");
          path = path.getParent();
        }
      }
      fs.put(src, "FILE");
      return getMockHdfsFileStatus(src, "FILE");
    });
    LocatedBlocks b = mockNn.getBlockLocations(
        anyString(), anyLong(), anyLong());
    when(b).thenAnswer(invocation -> {
      String src = getSrc(invocation);
      LOG.info("{} getBlockLocations({})", nsId, src);
      if (!fs.containsKey(src)) {
        LOG.error("{} cannot find {} for getBlockLocations", nsId, src);
        throw new FileNotFoundException("File does not exist " + src);
      }
      return mock(LocatedBlocks.class);
    });
    boolean f = mockNn.complete(anyString(), anyString(), any(), anyLong());
    when(f).thenAnswer(invocation -> {
      String src = getSrc(invocation);
      if (!fs.containsKey(src)) {
        LOG.error("{} cannot find {} for complete", nsId, src);
        throw new FileNotFoundException("File does not exist " + src);
      }
      return true;
    });
    LocatedBlock a = mockNn.addBlock(
        anyString(), anyString(), any(), any(), anyLong(), any(), any());
    when(a).thenAnswer(invocation -> {
      String src = getSrc(invocation);
      if (!fs.containsKey(src)) {
        LOG.error("{} cannot find {} for addBlock", nsId, src);
        throw new FileNotFoundException("File does not exist " + src);
      }
      return getMockLocatedBlock(nsId);
    });
    boolean m = mockNn.mkdirs(anyString(), any(), anyBoolean());
    when(m).thenAnswer(invocation -> {
      String src = getSrc(invocation);
      LOG.info("{} mkdirs({})", nsId, src);
      boolean createParent = (boolean)invocation.getArgument(2);
      if (createParent) {
        Path path = new Path(src).getParent();
        while (!path.isRoot()) {
          LOG.info("{} mkdir parent {}", nsId, path);
          fs.put(path.toString(), "DIRECTORY");
          path = path.getParent();
        }
      }
      fs.put(src, "DIRECTORY");
      return true;
    });
    when(mockNn.getServerDefaults()).thenAnswer(invocation -> {
      LOG.info("{} getServerDefaults", nsId);
      FsServerDefaults defaults = mock(FsServerDefaults.class);
      when(defaults.getChecksumType()).thenReturn(
          Type.valueOf(DataChecksum.CHECKSUM_CRC32));
      when(defaults.getKeyProviderUri()).thenReturn(nsId);
      return defaults;
    });
    when(mockNn.getContentSummary(anyString())).thenAnswer(invocation -> {
      String src = getSrc(invocation);
      LOG.info("{} getContentSummary({})", nsId, src);
      if (fs.get(src) == null) {
        throw new FileNotFoundException("File does not exist " + src);
      }
      if (!src.endsWith("/")) {
        src += "/";
      }
      Map<String, String> files =
          fs.subMap(src, src + Character.MAX_VALUE);
      int numFiles = 0;
      int numDirs = 0;
      int length = 0;
      for (Entry<String, String> entry : files.entrySet()) {
        String file = entry.getKey();
        if (file.substring(src.length()).indexOf('/') < 0) {
          String type = entry.getValue();
          if ("DIRECTORY".equals(type)) {
            numDirs++;
          } else if ("FILE".equals(type)) {
            numFiles++;
            length += 100;
          }
        }
      }
      return new ContentSummary.Builder()
          .fileCount(numFiles)
          .directoryCount(numDirs)
          .length(length)
          .erasureCodingPolicy("")
          .build();
    });
  }

  /**
   * Add datanode related operations.
   * @throws IOException If it cannot be setup.
   */
  public void addDatanodeMock() throws IOException {
    when(mockNn.getDatanodeReport(any(DatanodeReportType.class))).thenAnswer(
        invocation -> {
          LOG.info("{} getDatanodeReport()", nsId, invocation.getArgument(0));
          return dns.toArray();
        });
    when(mockNn.getDatanodeStorageReport(any(DatanodeReportType.class)))
        .thenAnswer(invocation -> {
          LOG.info("{} getDatanodeStorageReport()",
              nsId, invocation.getArgument(0));
          DatanodeStorageReport[] ret = new DatanodeStorageReport[dns.size()];
          for (int i = 0; i < dns.size(); i++) {
            DatanodeInfo dn = dns.get(i);
            DatanodeStorage storage = new DatanodeStorage(dn.getName());
            StorageReport[] storageReports = new StorageReport[] {
                new StorageReport(storage, false, 0L, 0L, 0L, 0L, 0L)
            };
            ret[i] = new DatanodeStorageReport(dn, storageReports);
          }
          return ret;
        });
  }

  private static String getSrc(InvocationOnMock invocation) {
    return (String) invocation.getArguments()[0];
  }

  /**
   * Get a mock HDFS file status.
   * @param filename Name of the file.
   * @param type Type of the file (FILE, DIRECTORY, or null).
   * @return HDFS file status
   */
  private static HdfsFileStatus getMockHdfsFileStatus(
      final String filename, final String type) {
    if (type == null) {
      return null;
    }
    HdfsFileStatus fileStatus = mock(HdfsFileStatus.class);
    when(fileStatus.getLocalNameInBytes()).thenReturn(filename.getBytes());
    when(fileStatus.getPermission()).thenReturn(mock(FsPermission.class));
    when(fileStatus.getOwner()).thenReturn("owner");
    when(fileStatus.getGroup()).thenReturn("group");
    if (type.equals("FILE")) {
      when(fileStatus.getLen()).thenReturn(100L);
      when(fileStatus.getReplication()).thenReturn((short) 1);
      when(fileStatus.getBlockSize()).thenReturn(
          HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    } else if (type.equals("DIRECTORY")) {
      when(fileStatus.isDir()).thenReturn(true);
      when(fileStatus.isDirectory()).thenReturn(true);
    }
    return fileStatus;
  }

  /**
   * Get a mock located block pointing to one of the subclusters. It is
   * allocated in a fake Datanode.
   * @param nsId Name service identifier (subcluster).
   * @return Mock located block.
   */
  private static LocatedBlock getMockLocatedBlock(final String nsId) {
    LocatedBlock lb = mock(LocatedBlock.class);
    when(lb.getCachedLocations()).thenReturn(new DatanodeInfo[0]);
    DatanodeID nodeId = new DatanodeID("localhost", "localhost", "dn0",
        1111, 1112, 1113, 1114);
    DatanodeInfo dnInfo = new DatanodeDescriptor(nodeId);
    DatanodeInfoWithStorage datanodeInfoWithStorage =
        new DatanodeInfoWithStorage(dnInfo, "storageID", StorageType.DEFAULT);
    when(lb.getLocations())
        .thenReturn(new DatanodeInfoWithStorage[] {datanodeInfoWithStorage});
    ExtendedBlock eb = mock(ExtendedBlock.class);
    when(eb.getBlockPoolId()).thenReturn(nsId);
    when(lb.getBlock()).thenReturn(eb);
    @SuppressWarnings("unchecked")
    Token<BlockTokenIdentifier> tok = mock(Token.class);
    when(tok.getIdentifier()).thenReturn(nsId.getBytes());
    when(tok.getPassword()).thenReturn(nsId.getBytes());
    when(tok.getKind()).thenReturn(new Text(nsId));
    when(tok.getService()).thenReturn(new Text(nsId));
    when(lb.getBlockToken()).thenReturn(tok);
    return lb;
  }

  /**
   * Register a set of NameNodes in a Router.
   * @param router Router to register to.
   * @param namenodes Set of NameNodes.
   * @throws IOException If it cannot register them.
   */
  public static void registerSubclusters(Router router,
      Collection<MockNamenode> namenodes) throws IOException {
    registerSubclusters(singletonList(router), namenodes, emptySet());
  }

  /**
   * Register a set of NameNodes in a set of Routers.
   * @param routers Set of Routers.
   * @param namenodes Set of NameNodes.
   * @param unavailableSubclusters Set of unavailable subclusters.
   * @throws IOException If it cannot register them.
   */
  public static void registerSubclusters(List<Router> routers,
      Collection<MockNamenode> namenodes,
      Set<String> unavailableSubclusters) throws IOException {

    for (final Router router : routers) {
      MembershipNamenodeResolver resolver =
          (MembershipNamenodeResolver) router.getNamenodeResolver();
      for (final MockNamenode nn : namenodes) {
        String nsId = nn.getNameserviceId();
        String rpcAddress = "localhost:" + nn.getRPCPort();
        String httpAddress = "localhost:" + nn.getHTTPPort();
        String scheme = "http";
        NamenodeStatusReport report = new NamenodeStatusReport(
            nsId, null, rpcAddress, rpcAddress,
            rpcAddress, scheme, httpAddress);
        if (unavailableSubclusters.contains(nsId)) {
          LOG.info("Register {} as UNAVAILABLE", nsId);
          report.setRegistrationValid(false);
        } else {
          LOG.info("Register {} as ACTIVE", nsId);
          report.setRegistrationValid(true);
        }
        report.setNamespaceInfo(new NamespaceInfo(0, nsId, nsId, 0));
        resolver.registerNamenode(report);
      }
      resolver.loadCache(true);
    }
  }
}