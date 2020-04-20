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
package org.apache.hadoop.hdfs.server.federation.router;

import static java.util.Arrays.asList;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createMountTableEntry;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.getAdminClient;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.getFileSystem;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.refreshRoutersCaches;
import static org.apache.hadoop.hdfs.server.federation.MockNamenode.registerSubclusters;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.MockNamenode;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test the handling of fault tolerant mount points in the Router.
 */
public class TestRouterFaultTolerant {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterFaultTolerant.class);

  /** Number of files to create for testing. */
  private static final int NUM_FILES = 10;
  /** Number of Routers for test. */
  private static final int NUM_ROUTERS = 2;


  /** Namenodes for the test per name service id (subcluster). */
  private Map<String, MockNamenode> namenodes = new HashMap<>();
  /** Routers for the test. */
  private List<Router> routers = new ArrayList<>();

  /** Run test tasks in parallel. */
  private ExecutorService service;


  @Before
  public void setup() throws Exception {
    LOG.info("Start the Namenodes");
    Configuration nnConf = new HdfsConfiguration();
    nnConf.setInt(DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY, 10);
    for (final String nsId : asList("ns0", "ns1")) {
      MockNamenode nn = new MockNamenode(nsId, nnConf);
      nn.transitionToActive();
      nn.addFileSystemMock();
      namenodes.put(nsId, nn);
    }

    LOG.info("Start the Routers");
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    routerConf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "0.0.0.0:0");
    routerConf.set(RBFConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    routerConf.set(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY, "0.0.0.0:0");
    // Speedup time outs
    routerConf.setTimeDuration(
        RBFConfigKeys.DFS_ROUTER_CLIENT_CONNECT_TIMEOUT,
        500, TimeUnit.MILLISECONDS);

    Configuration stateStoreConf = getStateStoreConfiguration();
    stateStoreConf.setClass(
        RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MembershipNamenodeResolver.class, ActiveNamenodeResolver.class);
    stateStoreConf.setClass(
        RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MultipleDestinationMountTableResolver.class,
        FileSubclusterResolver.class);
    routerConf.addResource(stateStoreConf);

    for (int i = 0; i < NUM_ROUTERS; i++) {
      // router0 doesn't allow partial listing
      routerConf.setBoolean(
          RBFConfigKeys.DFS_ROUTER_ALLOW_PARTIAL_LIST, i != 0);

      final Router router = new Router();
      router.init(routerConf);
      router.start();
      routers.add(router);
    }

    LOG.info("Registering the subclusters in the Routers");
    registerSubclusters(
        routers, namenodes.values(), Collections.singleton("ns1"));

    service = Executors.newFixedThreadPool(10);
  }

  @After
  public void cleanup() throws Exception {
    LOG.info("Stopping the cluster");
    for (final MockNamenode nn : namenodes.values()) {
      nn.stop();
    }
    namenodes.clear();

    routers.forEach(router ->  router.stop());
    routers.clear();

    if (service != null) {
      service.shutdown();
      service = null;
    }
  }

  /**
   * Update a mount table entry to be fault tolerant.
   * @param mountPoint Mount point to update.
   * @throws IOException If it cannot update the mount point.
   */
  private void updateMountPointFaultTolerant(final String mountPoint)
      throws IOException {
    Router router = getRandomRouter();
    RouterClient admin = getAdminClient(router);
    MountTableManager mountTable = admin.getMountTableManager();
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(mountPoint);
    GetMountTableEntriesResponse entries =
        mountTable.getMountTableEntries(getRequest);
    MountTable updateEntry = entries.getEntries().get(0);
    updateEntry.setFaultTolerant(true);
    UpdateMountTableEntryRequest updateRequest =
        UpdateMountTableEntryRequest.newInstance(updateEntry);
    UpdateMountTableEntryResponse updateResponse =
        mountTable.updateMountTableEntry(updateRequest);
    assertTrue(updateResponse.getStatus());

    refreshRoutersCaches(routers);
  }

  /**
   * Test the behavior of the Router when one of the subclusters in a mount
   * point fails. In particular, it checks if it can write files or not.
   * Related to {@link TestRouterRpcMultiDestination#testSubclusterDown()}.
   */
  @Test
  public void testWriteWithFailedSubcluster() throws Exception {

    LOG.info("Stop ns1 to simulate an unavailable subcluster");
    namenodes.get("ns1").stop();

    // Run the actual tests with each approach
    final List<Callable<Boolean>> tasks = new ArrayList<>();
    final List<DestinationOrder> orders = asList(
        DestinationOrder.HASH_ALL,
        DestinationOrder.SPACE,
        DestinationOrder.RANDOM,
        DestinationOrder.HASH);
    for (DestinationOrder order : orders) {
      tasks.add(() -> {
        testWriteWithFailedSubcluster(order);
        return true;
      });
    }
    TaskResults results = collectResults("Full tests", tasks);
    assertEquals(orders.size(), results.getSuccess());
  }

  /**
   * Test the behavior of the Router when one of the subclusters in a mount
   * point fails. It assumes that ns1 is already down.
   * @param order Destination order of the mount point.
   * @throws Exception If we cannot run the test.
   */
  private void testWriteWithFailedSubcluster(final DestinationOrder order)
      throws Exception {

    final FileSystem router0Fs = getFileSystem(routers.get(0));
    final FileSystem router1Fs = getFileSystem(routers.get(1));
    final FileSystem ns0Fs = getFileSystem(namenodes.get("ns0").getRPCPort());

    final String mountPoint = "/" + order + "-failsubcluster";
    final Path mountPath = new Path(mountPoint);
    LOG.info("Setup {} with order {}", mountPoint, order);
    createMountTableEntry(
        getRandomRouter(), mountPoint, order, namenodes.keySet());
    refreshRoutersCaches(routers);

    LOG.info("Write in {} should succeed writing in ns0 and fail for ns1",
        mountPath);
    checkDirectoriesFaultTolerant(
        mountPath, order, router0Fs, router1Fs, ns0Fs, false);
    checkFilesFaultTolerant(
        mountPath, order, router0Fs, router1Fs, ns0Fs, false);

    LOG.info("Make {} fault tolerant and everything succeeds", mountPath);
    IOException ioe = null;
    try {
      updateMountPointFaultTolerant(mountPoint);
    } catch (IOException e) {
      ioe = e;
    }
    if (DestinationOrder.FOLDER_ALL.contains(order)) {
      assertNull(ioe);
      checkDirectoriesFaultTolerant(
          mountPath, order, router0Fs, router1Fs, ns0Fs, true);
      checkFilesFaultTolerant(
          mountPath, order, router0Fs, router1Fs, ns0Fs, true);
    } else {
      assertTrue(ioe.getMessage().startsWith(
          "Invalid entry, fault tolerance only supported for ALL order"));
    }
  }

  /**
   * Check directory creation on a mount point.
   * If it is fault tolerant, it should be able to write everything.
   * If it is not fault tolerant, it should fail to write some.
   */
  private void checkDirectoriesFaultTolerant(
      Path mountPoint, DestinationOrder order,
      FileSystem router0Fs, FileSystem router1Fs, FileSystem ns0Fs,
      boolean faultTolerant) throws Exception {

    final FileStatus[] dirs0 = listStatus(router1Fs, mountPoint);

    LOG.info("Create directories in {}", mountPoint);
    final List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_FILES; i++) {
      final Path dir = new Path(mountPoint,
          String.format("dir-%s-%03d", faultTolerant, i));
      FileSystem fs = getRandomRouterFileSystem();
      tasks.add(getDirCreateTask(fs, dir));
    }
    TaskResults results = collectResults("Create dir " + mountPoint, tasks);

    LOG.info("Check directories results for {}: {}", mountPoint, results);
    if (faultTolerant || DestinationOrder.FOLDER_ALL.contains(order)) {
      assertEquals(NUM_FILES, results.getSuccess());
      assertEquals(0, results.getFailure());
    } else {
      assertBothResults("check dir " + mountPoint, NUM_FILES, results);
    }

    LOG.info("Check directories listing for {}", mountPoint);
    tasks.add(getListFailTask(router0Fs, mountPoint));
    int filesExpected = dirs0.length + results.getSuccess();
    tasks.add(getListSuccessTask(router1Fs, mountPoint, filesExpected));
    results = collectResults("List " + mountPoint, tasks);
    assertEquals("Failed listing", 2, results.getSuccess());

    tasks.add(getContentSummaryFailTask(router0Fs, mountPoint));
    tasks.add(getContentSummarySuccessTask(
        router1Fs, mountPoint, filesExpected));
    results = collectResults("Content summary "  + mountPoint, tasks);
    assertEquals("Failed content summary", 2, results.getSuccess());
  }

  /**
   * Check file creation on a mount point.
   * If it is fault tolerant, it should be able to write everything.
   * If it is not fault tolerant, it should fail to write some of the files.
   */
  private void checkFilesFaultTolerant(
      Path mountPoint, DestinationOrder order,
      FileSystem router0Fs, FileSystem router1Fs, FileSystem ns0Fs,
      boolean faultTolerant) throws Exception {

    // Get one of the existing sub directories
    final FileStatus[] dirs0 = listStatus(router1Fs, mountPoint);
    final Path dir0 = Path.getPathWithoutSchemeAndAuthority(
        dirs0[0].getPath());

    LOG.info("Create files in {}",  dir0);
    final List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_FILES; i++) {
      final String newFile = String.format("%s/file-%03d.txt", dir0, i);
      FileSystem fs = getRandomRouterFileSystem();
      tasks.add(getFileCreateTask(fs, newFile, ns0Fs));
    }
    TaskResults results = collectResults("Create file " + dir0, tasks);

    LOG.info("Check files results for {}: {}", dir0, results);
    if (faultTolerant) {
      assertEquals("Not enough success in " + mountPoint,
          NUM_FILES, results.getSuccess());
      assertEquals("Nothing should fail in " + mountPoint, 0,
          results.getFailure());
    } else {
      assertEquals("Nothing should succeed in " + mountPoint,
          0, results.getSuccess());
      assertEquals("Everything should fail in " + mountPoint,
          NUM_FILES, results.getFailure());
    }

    LOG.info("Check files listing for {}", dir0);
    tasks.add(getListFailTask(router0Fs, dir0));
    tasks.add(getListSuccessTask(router1Fs, dir0, results.getSuccess()));
    assertEquals(2, collectResults("List " + dir0, tasks).getSuccess());

    tasks.add(getContentSummaryFailTask(router0Fs, dir0));
    tasks.add(getContentSummarySuccessTask(
        router1Fs, dir0, results.getSuccess()));
    results = collectResults("Content summary "  + dir0, tasks);
    assertEquals(2, results.getSuccess());
  }

  /**
   * Get the string representation for the files.
   * @param files Files to check.
   * @return String representation.
   */
  private static String toString(final FileStatus[] files) {
    final StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (final FileStatus file : files) {
      if (sb.length() > 1) {
        sb.append(", ");
      }
      sb.append(Path.getPathWithoutSchemeAndAuthority(file.getPath()));
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * List the files in a path.
   * @param fs File system to check.
   * @param path Path to list.
   * @return List of files.
   * @throws IOException If we cannot list.
   */
  private FileStatus[] listStatus(final FileSystem fs, final Path path)
      throws IOException {
    FileStatus[] files = new FileStatus[] {};
    try {
      files = fs.listStatus(path);
    } catch (FileNotFoundException fnfe) {
      LOG.debug("File not found: {}", fnfe.getMessage());
    }
    return files;
  }

  /**
   * Task that creates a file and checks if it is available.
   * @param file File to create.
   * @param checkFs File system for checking if the file is properly created.
   * @return Result of creating the file.
   */
  private static Callable<Boolean> getFileCreateTask(
      final FileSystem fs, final String file, FileSystem checkFs) {
    return () -> {
      try {
        Path path = new Path(file);
        FSDataOutputStream os = fs.create(path);
        // We don't write because we have no mock Datanodes
        os.close();
        FileStatus fileStatus = checkFs.getFileStatus(path);
        assertTrue("File not created properly: " + fileStatus,
            fileStatus.getLen() > 0);
        return true;
      } catch (RemoteException re) {
        return false;
      }
    };
  }

  /**
   * Task that creates a directory.
   * @param dir Directory to create.
   * @return Result of creating the directory..
   */
  private static Callable<Boolean> getDirCreateTask(
      final FileSystem fs, final Path dir) {
    return () -> {
      try {
        fs.mkdirs(dir);
        return true;
      } catch (RemoteException re) {
        return false;
      }
    };
  }

  /**
   * Task that lists a directory and expects to fail.
   * @param fs File system to check.
   * @param path Path to try to list.
   * @return If the listing failed as expected.
   */
  private static Callable<Boolean> getListFailTask(FileSystem fs, Path path) {
    return () -> {
      try {
        fs.listStatus(path);
        return false;
      } catch (RemoteException re) {
        return true;
      }
    };
  }

  /**
   * Task that lists a directory and succeeds.
   * @param fs File system to check.
   * @param path Path to list.
   * @param expected Number of files to expect to find.
   * @return If the listing succeeds.
   */
  private static Callable<Boolean> getListSuccessTask(
      FileSystem fs, Path path, int expected) {
    return () -> {
      final FileStatus[] dirs = fs.listStatus(path);
      assertEquals(toString(dirs), expected, dirs.length);
      return true;
    };
  }


  /**
   * Task that lists a directory and expects to fail.
   * @param fs File system to check.
   * @param path Path to try to list.
   * @return If the listing failed as expected.
   */
  private static Callable<Boolean> getContentSummaryFailTask(
      FileSystem fs, Path path) {
    return () -> {
      try {
        fs.getContentSummary(path);
        return false;
      } catch (RemoteException re) {
        return true;
      }
    };
  }

  /**
   * Task that lists a directory and succeeds.
   * @param fs File system to check.
   * @param path Path to list.
   * @param expected Number of files to expect to find.
   * @return If the listing succeeds.
   */
  private static Callable<Boolean> getContentSummarySuccessTask(
      FileSystem fs, Path path, int expected) {
    return () -> {
      ContentSummary summary = fs.getContentSummary(path);
      assertEquals("Wrong summary for " + path,
          expected, summary.getFileAndDirectoryCount());
      return true;
    };
  }

  /**
   * Invoke a set of tasks and collect their outputs.
   * The tasks should do assertions.
   *
   * @param service Execution Service to run the tasks.
   * @param tasks Tasks to run.
   * @throws Exception If it cannot collect the results.
   */
  private TaskResults collectResults(final String tag,
      final Collection<Callable<Boolean>> tasks) throws Exception {
    final TaskResults results = new TaskResults();
    service.invokeAll(tasks).forEach(task -> {
      try {
        boolean succeeded = task.get();
        if (succeeded) {
          LOG.info("Got success for {}", tag);
          results.incrSuccess();
        } else {
          LOG.info("Got failure for {}", tag);
          results.incrFailure();
        }
      } catch (Exception e) {
        StringWriter stackTrace = new StringWriter();
        PrintWriter writer = new PrintWriter(stackTrace);
        if (e instanceof ExecutionException) {
          e.getCause().printStackTrace(writer);
        } else {
          e.printStackTrace(writer);
        }
        fail("Failed to run \"" + tag + "\": " + stackTrace);
      }
    });
    tasks.clear();
    return results;
  }

  /**
   * Class to summarize the results of running a task.
   */
  static class TaskResults {
    private final AtomicInteger success = new AtomicInteger(0);
    private final AtomicInteger failure = new AtomicInteger(0);
    public void incrSuccess() {
      success.incrementAndGet();
    }
    public void incrFailure() {
      failure.incrementAndGet();
    }
    public int getSuccess() {
      return success.get();
    }
    public int getFailure() {
      return failure.get();
    }
    public int getTotal() {
      return success.get() + failure.get();
    }
    @Override
    public String toString() {
      return new StringBuilder()
          .append("Success=").append(getSuccess())
          .append(" Failure=").append(getFailure())
          .toString();
    }
  }

  /**
   * Asserts that the results are the expected amount and it has both success
   * and failure.
   * @param msg Message to show when the assertion fails.
   * @param expected Expected number of results.
   * @param actual Actual results.
   */
  private static void assertBothResults(String msg,
      int expected, TaskResults actual) {
    assertEquals(msg, expected, actual.getTotal());
    assertTrue("Expected some success for " + msg, actual.getSuccess() > 0);
    assertTrue("Expected some failure for " + msg, actual.getFailure() > 0);
  }

  /**
   * Get a random Router from the cluster.
   * @return Random Router.
   */
  private Router getRandomRouter() {
    Random rnd = new Random();
    int index = rnd.nextInt(routers.size());
    return routers.get(index);
  }

  /**
   * Get a file system from one of the Routers as a random user to allow better
   * concurrency in the Router.
   * @return File system from a random user.
   * @throws Exception If we cannot create the file system.
   */
  private FileSystem getRandomRouterFileSystem() throws Exception {
    final UserGroupInformation userUgi =
        UserGroupInformation.createUserForTesting(
            "user-" + UUID.randomUUID(), new String[]{"group"});
    Router router = getRandomRouter();
    return userUgi.doAs(
        (PrivilegedExceptionAction<FileSystem>) () -> getFileSystem(router));
  }

  @Test
  public void testReadWithFailedSubcluster() throws Exception {

    DestinationOrder order = DestinationOrder.HASH_ALL;
    final String mountPoint = "/" + order + "-testread";
    final Path mountPath = new Path(mountPoint);
    LOG.info("Setup {} with order {}", mountPoint, order);
    createMountTableEntry(
        routers, mountPoint, order, namenodes.keySet());

    FileSystem fs = getRandomRouterFileSystem();

    // Create a file (we don't write because we have no mock Datanodes)
    final Path fileexisting = new Path(mountPath, "fileexisting");
    final Path filenotexisting = new Path(mountPath, "filenotexisting");
    FSDataOutputStream os = fs.create(fileexisting);
    assertNotNull(os);
    os.close();

    // We should be able to read existing files
    FSDataInputStream fsdis = fs.open(fileexisting);
    assertNotNull("We should be able to read the file", fsdis);
    // We shouldn't be able to read non-existing files
    LambdaTestUtils.intercept(FileNotFoundException.class,
        () -> fs.open(filenotexisting));

    // Check the subcluster where the file got created
    String nsIdWithFile = null;
    for (Entry<String, MockNamenode> entry : namenodes.entrySet()) {
      String nsId = entry.getKey();
      MockNamenode nn = entry.getValue();
      int rpc = nn.getRPCPort();
      FileSystem nnfs = getFileSystem(rpc);

      try {
        FileStatus fileStatus = nnfs.getFileStatus(fileexisting);
        assertNotNull(fileStatus);
        assertNull("The file cannot be in two subclusters", nsIdWithFile);
        nsIdWithFile = nsId;
      } catch (FileNotFoundException fnfe) {
        LOG.debug("File not found in {}", nsId);
      }
    }
    assertNotNull("The file has to be in one subcluster", nsIdWithFile);

    LOG.info("Stop {} to simulate an unavailable subcluster", nsIdWithFile);
    namenodes.get(nsIdWithFile).stop();

    // We should not get FileNotFoundException anymore
    try {
      fs.open(fileexisting);
      fail("It should throw an unavailable cluster exception");
    } catch(RemoteException re) {
      IOException ioe = re.unwrapRemoteException();
      assertTrue("Expected an unavailable exception for:" + ioe.getClass(),
          RouterRpcClient.isUnavailableException(ioe));
    }
  }
}
