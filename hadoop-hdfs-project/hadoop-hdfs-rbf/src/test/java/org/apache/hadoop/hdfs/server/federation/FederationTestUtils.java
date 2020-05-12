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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.ConnectionManager;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Helper utilities for testing HDFS Federation.
 */
public final class FederationTestUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationTestUtils.class);

  public final static String[] NAMESERVICES = {"ns0", "ns1"};
  public final static String[] NAMENODES = {"nn0", "nn1", "nn2", "nn3"};
  public final static String[] ROUTERS =
      {"router0", "router1", "router2", "router3"};


  private FederationTestUtils() {
    // Utility class
  }

  public static void verifyException(Object obj, String methodName,
      Class<? extends Exception> exceptionClass, Class<?>[] parameterTypes,
      Object[] arguments) {

    Throwable triggeredException = null;
    try {
      Method m = obj.getClass().getMethod(methodName, parameterTypes);
      m.invoke(obj, arguments);
    } catch (InvocationTargetException ex) {
      triggeredException = ex.getTargetException();
    } catch (Exception e) {
      triggeredException = e;
    }
    if (exceptionClass != null) {
      assertNotNull("No exception was triggered, expected exception"
          + exceptionClass.getName(), triggeredException);
      assertEquals(exceptionClass, triggeredException.getClass());
    } else {
      assertNull("Exception was triggered but no exception was expected",
          triggeredException);
    }
  }

  public static NamenodeStatusReport createNamenodeReport(String ns, String nn,
      HAServiceState state) {
    Random rand = new Random();
    return createNamenodeReport(ns, nn, "localhost:"
        + rand.nextInt(10000), state);
  }

  public static NamenodeStatusReport createNamenodeReport(String ns, String nn,
      String rpcAddress, HAServiceState state) {
    Random rand = new Random();
    NamenodeStatusReport report = new NamenodeStatusReport(ns, nn, rpcAddress,
        "localhost:" + rand.nextInt(10000),
        "localhost:" + rand.nextInt(10000), "http",
        "testwebaddress-" + ns + nn);
    if (state == null) {
      // Unavailable, no additional info
      return report;
    }
    report.setHAServiceState(state);
    NamespaceInfo nsInfo = new NamespaceInfo(
        1, "tesclusterid", ns, 0, "testbuildvesion", "testsoftwareversion");
    report.setNamespaceInfo(nsInfo);
    return report;
  }

  /**
   * Wait for a namenode to be registered with a particular state.
   * @param resolver Active namenode resolver.
   * @param nsId Nameservice identifier.
   * @param nnId Namenode identifier.
   * @param finalState State to check for.
   * @throws Exception Failed to verify State Store registration of namenode
   *                   nsId:nnId for state.
   */
  public static void waitNamenodeRegistered(
      final ActiveNamenodeResolver resolver,
      final String nsId, final String nnId,
      final FederationNamenodeServiceState state) throws Exception {

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          List<? extends FederationNamenodeContext> namenodes =
              resolver.getNamenodesForNameserviceId(nsId);
          if (namenodes != null) {
            for (FederationNamenodeContext namenode : namenodes) {
              // Check if this is the Namenode we are checking
              if (namenode.getNamenodeId() == nnId  ||
                  namenode.getNamenodeId().equals(nnId)) {
                return state == null || namenode.getState().equals(state);
              }
            }
          }
        } catch (IOException e) {
          // Ignore
        }
        return false;
      }
    }, 1000, 60 * 1000);
  }

  /**
   * Wait for a namenode to be registered with a particular state.
   * @param resolver Active namenode resolver.
   * @param nsId Nameservice identifier.
   * @param state State to check for.
   * @throws Exception Failed to verify State Store registration of namenode
   *                   nsId for state.
   */
  public static void waitNamenodeRegistered(
      final ActiveNamenodeResolver resolver, final String nsId,
      final FederationNamenodeServiceState state) throws Exception {

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          List<? extends FederationNamenodeContext> nns =
              resolver.getNamenodesForNameserviceId(nsId);
          for (FederationNamenodeContext nn : nns) {
            if (nn.getState().equals(state)) {
              return true;
            }
          }
        } catch (IOException e) {
          // Ignore
        }
        return false;
      }
    }, 1000, 20 * 1000);
  }

  public static boolean verifyDate(Date d1, Date d2, long precision) {
    return Math.abs(d1.getTime() - d2.getTime()) < precision;
  }

  public static <T> T getBean(String name, Class<T> obj)
      throws MalformedObjectNameException {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName poolName = new ObjectName(name);
    return JMX.newMXBeanProxy(mBeanServer, poolName, obj);
  }

  public static boolean addDirectory(FileSystem context, String path)
      throws IOException {
    context.mkdirs(new Path(path), new FsPermission("777"));
    return verifyFileExists(context, path);
  }

  public static FileStatus getFileStatus(FileSystem context, String path)
      throws IOException {
    return context.getFileStatus(new Path(path));
  }

  public static boolean verifyFileExists(FileSystem context, String path) {
    try {
      FileStatus status = getFileStatus(context, path);
      if (status != null) {
        return true;
      }
    } catch (Exception e) {
      return false;
    }
    return false;
  }

  public static boolean checkForFileInDirectory(
      FileSystem context, String testPath, String targetFile)
          throws IOException, AccessControlException, FileNotFoundException,
          UnsupportedFileSystemException, IllegalArgumentException {

    FileStatus[] fileStatus = context.listStatus(new Path(testPath));
    String file = null;
    String verifyPath = testPath + "/" + targetFile;
    if (testPath.equals("/")) {
      verifyPath = testPath + targetFile;
    }

    Boolean found = false;
    for (int i = 0; i < fileStatus.length; i++) {
      FileStatus f = fileStatus[i];
      file = Path.getPathWithoutSchemeAndAuthority(f.getPath()).toString();
      if (file.equals(verifyPath)) {
        found = true;
      }
    }
    return found;
  }

  public static int countContents(FileSystem context, String testPath)
      throws IOException {
    Path path = new Path(testPath);
    FileStatus[] fileStatus = context.listStatus(path);
    return fileStatus.length;
  }

  public static void createFile(FileSystem fs, String path, long length)
      throws IOException {
    FsPermission permissions = new FsPermission("700");
    FSDataOutputStream writeStream = fs.create(new Path(path), permissions,
        true, 1000, (short) 1, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT, null);
    for (int i = 0; i < length; i++) {
      writeStream.write(i);
    }
    writeStream.close();
  }

  public static String readFile(FileSystem fs, String path) throws IOException {
    // Read the file from the filesystem via the active namenode
    Path fileName = new Path(path);
    InputStreamReader reader = new InputStreamReader(fs.open(fileName));
    BufferedReader bufferedReader = new BufferedReader(reader);
    StringBuilder data = new StringBuilder();
    String line;

    while ((line = bufferedReader.readLine()) != null) {
      data.append(line);
    }

    bufferedReader.close();
    reader.close();
    return data.toString();
  }

  public static boolean deleteFile(FileSystem fs, String path)
      throws IOException {
    return fs.delete(new Path(path), true);
  }

  /**
   * Simulate that a Namenode is slow by adding a sleep to the check operation
   * in the NN.
   * @param nn Namenode to simulate slow.
   * @param seconds Number of seconds to add to the Namenode.
   * @throws Exception If we cannot add the sleep time.
   */
  public static void simulateSlowNamenode(final NameNode nn, final int seconds)
      throws Exception {
    FSNamesystem namesystem = nn.getNamesystem();
    HAContext haContext = namesystem.getHAContext();
    HAContext spyHAContext = spy(haContext);
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        LOG.info("Simulating slow namenode {}", invocation.getMock());
        try {
          Thread.sleep(seconds * 1000);
        } catch(InterruptedException e) {
          LOG.error("Simulating a slow namenode aborted");
        }
        return null;
      }
    }).when(spyHAContext).checkOperation(any(OperationCategory.class));
    Whitebox.setInternalState(namesystem, "haContext", spyHAContext);
  }

  /**
   * Wait for a number of routers to be registered in state store.
   *
   * @param stateManager number of routers to be registered.
   * @param routerCount number of routers to be registered.
   * @param tiemout max wait time in ms
   */
  public static void waitRouterRegistered(RouterStore stateManager,
      long routerCount, int timeout) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          List<RouterState> cachedRecords = stateManager.getCachedRecords();
          if (cachedRecords.size() == routerCount) {
            return true;
          }
        } catch (IOException e) {
          // Ignore
        }
        return false;
      }
    }, 100, timeout);
  }

  /**
   * Simulate that a RouterRpcServer, the ConnectionManager of its
   * RouterRpcClient throws IOException when call getConnection. So the
   * RouterRpcClient will get a null Connection.
   * @param server RouterRpcServer
   * @throws IOException
   */
  public static void simulateThrowExceptionRouterRpcServer(
      final RouterRpcServer server) throws IOException {
    RouterRpcClient rpcClient = server.getRPCClient();
    ConnectionManager connectionManager =
        new ConnectionManager(server.getConfig());
    ConnectionManager spyConnectionManager = spy(connectionManager);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        LOG.info("Simulating connectionManager throw IOException {}",
            invocation.getMock());
        throw new IOException("Simulate connectionManager throw IOException");
      }
    }).when(spyConnectionManager).getConnection(
        any(UserGroupInformation.class), any(String.class), any(Class.class));

    Whitebox.setInternalState(rpcClient, "connectionManager",
        spyConnectionManager);
  }

  /**
   * Switch namenodes of all hdfs name services to standby.
   * @param cluster a federated HDFS cluster
   */
  public static void transitionClusterNSToStandby(
      StateStoreDFSCluster cluster) {
    // Name services of the cluster
    List<String> nameServiceList = cluster.getNameservices();

    // Change namenodes of each name service to standby
    for (String nameService : nameServiceList) {
      List<NamenodeContext>  nnList = cluster.getNamenodes(nameService);
      for(NamenodeContext namenodeContext : nnList) {
        cluster.switchToStandby(nameService, namenodeContext.getNamenodeId());
      }
    }
  }

  /**
   * Switch the index namenode of all hdfs name services to active.
   * @param cluster a federated HDFS cluster
   * @param index the index of namenodes
   */
  public static void transitionClusterNSToActive(
      StateStoreDFSCluster cluster, int index) {
    // Name services of the cluster
    List<String> nameServiceList = cluster.getNameservices();

    // Change the index namenode of each name service to active
    for (String nameService : nameServiceList) {
      List<NamenodeContext> listNamenodeContext =
          cluster.getNamenodes(nameService);
      cluster.switchToActive(nameService,
          listNamenodeContext.get(index).getNamenodeId());
    }
  }

  /**
   * Get the file system for HDFS in an RPC port.
   * @param rpcPort RPC port.
   * @return HDFS file system.
   * @throws IOException If it cannot create the file system.
   */
  public static FileSystem getFileSystem(int rpcPort) throws IOException {
    Configuration conf = new HdfsConfiguration();
    URI uri = URI.create("hdfs://localhost:" + rpcPort);
    return DistributedFileSystem.get(uri, conf);
  }

  /**
   * Get the file system for HDFS for a Router.
   * @param router Router.
   * @return HDFS file system.
   * @throws IOException If it cannot create the file system.
   */
  public static FileSystem getFileSystem(final Router router)
      throws IOException {
    InetSocketAddress rpcAddress = router.getRpcServerAddress();
    int rpcPort = rpcAddress.getPort();
    return getFileSystem(rpcPort);
  }

  /**
   * Get the admin interface for a Router.
   * @param router Router.
   * @return Admin interface.
   * @throws IOException If it cannot create the admin interface.
   */
  public static RouterClient getAdminClient(
      final Router router) throws IOException {
    Configuration conf = new HdfsConfiguration();
    InetSocketAddress routerSocket = router.getAdminServerAddress();
    return new RouterClient(routerSocket, conf);
  }

  /**
   * Add a mount table entry in some name services and wait until it is
   * available. If there are multiple routers,
   * {@link #createMountTableEntry(List, String, DestinationOrder, Collection)}
   * should be used instead because the method does not refresh
   * the mount tables of the other routers.
   * @param router Router to change.
   * @param mountPoint Name of the mount point.
   * @param order Order of the mount table entry.
   * @param nsIds Name service identifiers.
   * @throws Exception If the entry could not be created.
   */
  public static void createMountTableEntry(
      final Router router,
      final String mountPoint, final DestinationOrder order,
      Collection<String> nsIds) throws Exception {
    createMountTableEntry(
        Collections.singletonList(router), mountPoint, order, nsIds);
  }

  /**
   * Add a mount table entry in some name services and wait until it is
   * available.
   * @param routers List of routers.
   * @param mountPoint Name of the mount point.
   * @param order Order of the mount table entry.
   * @param nsIds Name service identifiers.
   * @throws Exception If the entry could not be created.
   */
  public static void createMountTableEntry(
      final List<Router> routers,
      final String mountPoint,
      final DestinationOrder order,
      final Collection<String> nsIds) throws Exception {
    Router router = routers.get(0);
    RouterClient admin = getAdminClient(router);
    MountTableManager mountTable = admin.getMountTableManager();
    Map<String, String> destMap = new HashMap<>();
    for (String nsId : nsIds) {
      destMap.put(nsId, mountPoint);
    }
    MountTable newEntry = MountTable.newInstance(mountPoint, destMap);
    newEntry.setDestOrder(order);
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    boolean created = addResponse.getStatus();
    assertTrue(created);

    refreshRoutersCaches(routers);

    // Check for the path
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(mountPoint);
    GetMountTableEntriesResponse getResponse =
        mountTable.getMountTableEntries(getRequest);
    List<MountTable> entries = getResponse.getEntries();
    assertEquals("Too many entries: " + entries, 1, entries.size());
    assertEquals(mountPoint, entries.get(0).getSourcePath());
  }

  /**
   * Refresh the caches of a set of Routers.
   * @param routers List of Routers.
   */
  public static void refreshRoutersCaches(final List<Router> routers) {
    for (final Router router : routers) {
      StateStoreService stateStore = router.getStateStore();
      stateStore.refreshCaches(true);
    }
  }
}
