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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import static org.apache.hadoop.service.Service.STATE.INITED;
import static org.apache.hadoop.service.Service.STATE.STARTED;
import static org.apache.hadoop.service.Service.STATE.STOPPED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryLocalPathHandler;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.junit.Assert;
import org.junit.Test;

public class TestAuxServices {
  private static final Logger LOG =
       LoggerFactory.getLogger(TestAuxServices.class);
  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestAuxServices.class.getName());
  private final static AuxiliaryLocalPathHandler MOCK_AUX_PATH_HANDLER =
      mock(AuxiliaryLocalPathHandler.class);
  private final static Context MOCK_CONTEXT = mock(Context.class);
  private final static DeletionService MOCK_DEL_SERVICE = mock(
      DeletionService.class);

  static class LightService extends AuxiliaryService implements Service
       {
    private final char idef;
    private final int expected_appId;
    private int remaining_init;
    private int remaining_stop;
    private ByteBuffer meta = null;
    private ArrayList<Integer> stoppedApps;
    private ContainerId containerId;
    private Resource resource;

         LightService(String name, char idef, int expected_appId) {
      this(name, idef, expected_appId, null);
    } 
    LightService(String name, char idef, int expected_appId, ByteBuffer meta) {
      super(name);
      this.idef = idef;
      this.expected_appId = expected_appId;
      this.meta = meta;
      this.stoppedApps = new ArrayList<Integer>();
    }

    @SuppressWarnings("unchecked")
    public ArrayList<Integer> getAppIdsStopped() {
      return (ArrayList<Integer>)this.stoppedApps.clone();
    }

    @Override 
    protected void serviceInit(Configuration conf) throws Exception {
      remaining_init = conf.getInt(idef + ".expected.init", 0);
      remaining_stop = conf.getInt(idef + ".expected.stop", 0);
      super.serviceInit(conf);
    }
    @Override
    protected void serviceStop() throws Exception {
      assertEquals(0, remaining_init);
      assertEquals(0, remaining_stop);
      super.serviceStop();
    }
    @Override
    public void initializeApplication(ApplicationInitializationContext context) {
      ByteBuffer data = context.getApplicationDataForService();
      assertEquals(idef, data.getChar());
      assertEquals(expected_appId, data.getInt());
      assertEquals(expected_appId, context.getApplicationId().getId());
    }
    @Override
    public void stopApplication(ApplicationTerminationContext context) {
      stoppedApps.add(context.getApplicationId().getId());
    }
    @Override
    public ByteBuffer getMetaData() {
      return meta;
    }

    @Override
    public void initializeContainer(
        ContainerInitializationContext initContainerContext) {
      containerId = initContainerContext.getContainerId();
      resource = initContainerContext.getResource();
    }

    @Override
    public void stopContainer(
        ContainerTerminationContext stopContainerContext) {
      containerId = stopContainerContext.getContainerId();
      resource = stopContainerContext.getResource();
    }

 }

  static class ServiceA extends LightService {
    public ServiceA() { 
      super("A", 'A', 65, ByteBuffer.wrap("A".getBytes()));
    }
  }

  static class ServiceB extends LightService {
    public ServiceB() { 
      super("B", 'B', 66, ByteBuffer.wrap("B".getBytes()));
    }
  }

  // Override getMetaData() method to return current
  // class path. This class would be used for
  // testCustomizedAuxServiceClassPath.
  static class ServiceC extends LightService {
    public ServiceC() {
      super("C", 'C', 66, ByteBuffer.wrap("C".getBytes()));
    }

    @Override
    public ByteBuffer getMetaData() {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      URL[] urls = ((URLClassLoader)loader).getURLs();
      List<String> urlString = new ArrayList<String>();
      for (URL url : urls) {
        urlString.add(url.toString());
      }
      String joinedString = StringUtils.join(",", urlString);
      return ByteBuffer.wrap(joinedString.getBytes());
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testRemoteAuxServiceClassPath() throws Exception {
    Configuration conf = new YarnConfiguration();
    FileSystem fs = FileSystem.get(conf);
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES,
        new String[] {"ServiceC"});
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT,
        "ServiceC"), ServiceC.class, Service.class);

    Context mockContext2 = mock(Context.class);
    LocalDirsHandlerService mockDirsHandler = mock(
        LocalDirsHandlerService.class);
    String root = "target/LocalDir";
    Path rootAuxServiceDirPath = new Path(root, "nmAuxService");
    when(mockDirsHandler.getLocalPathForWrite(anyString())).thenReturn(
        rootAuxServiceDirPath);
    when(mockContext2.getLocalDirsHandler()).thenReturn(mockDirsHandler);

    File rootDir = GenericTestUtils.getTestDir(getClass()
        .getSimpleName());
    if (!rootDir.exists()) {
      rootDir.mkdirs();
    }
    AuxServices aux = null;
    File testJar = null;
    try {
      // the remote jar file should not be be writable by group or others.
      try {
        testJar = JarFinder.makeClassLoaderTestJar(this.getClass(), rootDir,
            "test-runjar.jar", 2048, ServiceC.class.getName());
        // Give group a write permission.
        // We should not load the auxservice from remote jar file.
        Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.GROUP_WRITE);
        Files.setPosixFilePermissions(Paths.get(testJar.getAbsolutePath()),
            perms);
        conf.set(String.format(
            YarnConfiguration.NM_AUX_SERVICE_REMOTE_CLASSPATH, "ServiceC"),
            testJar.getAbsolutePath());
        aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
            mockContext2, MOCK_DEL_SERVICE);
        aux.init(conf);
        Assert.fail("The permission of the jar is wrong."
            + "Should throw out exception.");
      } catch (YarnRuntimeException ex) {
        Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(
            "The remote jarfile should not be writable by group or others"));
      }

      Files.delete(Paths.get(testJar.getAbsolutePath()));

      testJar = JarFinder.makeClassLoaderTestJar(this.getClass(), rootDir,
          "test-runjar.jar", 2048, ServiceC.class.getName());
      conf.set(String.format(
          YarnConfiguration.NM_AUX_SERVICE_REMOTE_CLASSPATH, "ServiceC"),
          testJar.getAbsolutePath());
      aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
          mockContext2, MOCK_DEL_SERVICE);
      aux.init(conf);
      aux.start();
      Map<String, ByteBuffer> meta = aux.getMetaData();
      String auxName = "";
      Assert.assertTrue(meta.size() == 1);
      for(Entry<String, ByteBuffer> i : meta.entrySet()) {
        auxName = i.getKey();
      }
      Assert.assertEquals("ServiceC", auxName);
      aux.serviceStop();
      FileStatus[] status = fs.listStatus(rootAuxServiceDirPath);
      Assert.assertTrue(status.length == 1);

      // initialize the same auxservice again, and make sure that we did not
      // re-download the jar from remote directory.
      aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
          mockContext2, MOCK_DEL_SERVICE);
      aux.init(conf);
      aux.start();
      meta = aux.getMetaData();
      Assert.assertTrue(meta.size() == 1);
      for(Entry<String, ByteBuffer> i : meta.entrySet()) {
        auxName = i.getKey();
      }
      Assert.assertEquals("ServiceC", auxName);
      verify(MOCK_DEL_SERVICE, times(0)).delete(any(FileDeletionTask.class));
      status = fs.listStatus(rootAuxServiceDirPath);
      Assert.assertTrue(status.length == 1);
      aux.serviceStop();

      // change the last modification time for remote jar,
      // we will re-download the jar and clean the old jar
      long time = System.currentTimeMillis() + 3600*1000;
      FileTime fileTime = FileTime.fromMillis(time);
      Files.setLastModifiedTime(Paths.get(testJar.getAbsolutePath()),
          fileTime);
      conf.set(
          String.format(YarnConfiguration.NM_AUX_SERVICE_REMOTE_CLASSPATH,
              "ServiceC"),
          testJar.getAbsolutePath());
      aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
          mockContext2, MOCK_DEL_SERVICE);
      aux.init(conf);
      aux.start();
      verify(MOCK_DEL_SERVICE, times(1)).delete(any(FileDeletionTask.class));
      status = fs.listStatus(rootAuxServiceDirPath);
      Assert.assertTrue(status.length == 2);
      aux.serviceStop();
    } finally {
      if (testJar != null) {
        testJar.delete();
        rootDir.delete();
      }
      if (fs.exists(new Path(root))) {
        fs.delete(new Path(root), true);
      }
    }
  }

  // To verify whether we could load class from customized class path.
  // We would use ServiceC in this test. Also create a separate jar file
  // including ServiceC class, and add this jar to customized directory.
  // By setting some proper configurations, we should load ServiceC class
  // from customized class path.
  @Test (timeout = 15000)
  public void testCustomizedAuxServiceClassPath() throws Exception {
    // verify that we can load AuxService Class from default Class path
    Configuration conf = new YarnConfiguration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES,
        new String[] {"ServiceC"});
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT,
        "ServiceC"), ServiceC.class, Service.class);
    @SuppressWarnings("resource")
    AuxServices aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
        MOCK_CONTEXT, MOCK_DEL_SERVICE);
    aux.init(conf);
    aux.start();
    Map<String, ByteBuffer> meta = aux.getMetaData();
    String auxName = "";
    Set<String> defaultAuxClassPath = null;
    Assert.assertTrue(meta.size() == 1);
    for(Entry<String, ByteBuffer> i : meta.entrySet()) {
      auxName = i.getKey();
      String auxClassPath = Charsets.UTF_8.decode(i.getValue()).toString();
      defaultAuxClassPath = new HashSet<String>(Arrays.asList(StringUtils
          .getTrimmedStrings(auxClassPath)));
    }
    Assert.assertEquals("ServiceC", auxName);
    aux.serviceStop();

    // create a new jar file, and configure it as customized class path
    // for this AuxService, and make sure that we could load the class
    // from this configured customized class path
    File rootDir = GenericTestUtils.getTestDir(getClass()
        .getSimpleName());
    if (!rootDir.exists()) {
      rootDir.mkdirs();
    }
    File testJar = null;
    try {
      testJar = JarFinder.makeClassLoaderTestJar(this.getClass(), rootDir,
          "test-runjar.jar", 2048, ServiceC.class.getName());
      conf = new YarnConfiguration();
      conf.setStrings(YarnConfiguration.NM_AUX_SERVICES,
          new String[] {"ServiceC"});
      conf.set(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "ServiceC"),
          ServiceC.class.getName());
      conf.set(String.format(
          YarnConfiguration.NM_AUX_SERVICES_CLASSPATH, "ServiceC"),
          testJar.getAbsolutePath());
      // remove "-org.apache.hadoop." from system classes
      String systemClasses = "-org.apache.hadoop." + "," +
              ApplicationClassLoader.SYSTEM_CLASSES_DEFAULT;
      conf.set(String.format(
          YarnConfiguration.NM_AUX_SERVICES_SYSTEM_CLASSES,
          "ServiceC"), systemClasses);
      aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
          MOCK_CONTEXT, MOCK_DEL_SERVICE);
      aux.init(conf);
      aux.start();
      meta = aux.getMetaData();
      Assert.assertTrue(meta.size() == 1);
      Set<String> customizedAuxClassPath = null;
      for(Entry<String, ByteBuffer> i : meta.entrySet()) {
        Assert.assertTrue(auxName.equals(i.getKey()));
        String classPath = Charsets.UTF_8.decode(i.getValue()).toString();
        customizedAuxClassPath = new HashSet<String>(Arrays.asList(StringUtils
            .getTrimmedStrings(classPath)));
        Assert.assertTrue(classPath.contains(testJar.getName()));
      }
      aux.stop();

      // Verify that we do not have any overlap between customized class path
      // and the default class path.
      Set<String> mutalClassPath = Sets.intersection(defaultAuxClassPath,
          customizedAuxClassPath);
      Assert.assertTrue(mutalClassPath.isEmpty());
    } finally {
      if (testJar != null) {
        testJar.delete();
        rootDir.delete();
      }
    }
  }

  @Test
  public void testAuxEventDispatch() {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] { "Asrv", "Bsrv" });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv"),
        ServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv"),
        ServiceB.class, Service.class);
    conf.setInt("A.expected.init", 1);
    conf.setInt("B.expected.stop", 1);
    final AuxServices aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
        MOCK_CONTEXT, MOCK_DEL_SERVICE);
    aux.init(conf);
    aux.start();

    ApplicationId appId1 = ApplicationId.newInstance(0, 65);
    ByteBuffer buf = ByteBuffer.allocate(6);
    buf.putChar('A');
    buf.putInt(65);
    buf.flip();
    AuxServicesEvent event = new AuxServicesEvent(
        AuxServicesEventType.APPLICATION_INIT, "user0", appId1, "Asrv", buf);
    aux.handle(event);
    ApplicationId appId2 = ApplicationId.newInstance(0, 66);
    event = new AuxServicesEvent(
        AuxServicesEventType.APPLICATION_STOP, "user0", appId2, "Bsrv", null);
    // verify all services got the stop event 
    aux.handle(event);
    Collection<AuxiliaryService> servs = aux.getServices();
    for (AuxiliaryService serv: servs) {
      ArrayList<Integer> appIds = ((LightService)serv).getAppIdsStopped();
      assertEquals("app not properly stopped", 1, appIds.size());
      assertTrue("wrong app stopped", appIds.contains((Integer)66));
    }

    for (AuxiliaryService serv : servs) {
      assertNull(((LightService) serv).containerId);
      assertNull(((LightService) serv).resource);
    }


    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId1, 1);
    ContainerTokenIdentifier cti = new ContainerTokenIdentifier(
        ContainerId.newContainerId(attemptId, 1), "", "",
        Resource.newInstance(1, 1), 0,0,0, Priority.newInstance(0), 0);
    Context context = mock(Context.class);
    Container container = new ContainerImpl(new YarnConfiguration(), null, null, null,
        null, cti, context);
    ContainerId containerId = container.getContainerId();
    Resource resource = container.getResource();
    event = new AuxServicesEvent(AuxServicesEventType.CONTAINER_INIT,container);
    aux.handle(event);
    for (AuxiliaryService serv : servs) {
      assertEquals(containerId, ((LightService) serv).containerId);
      assertEquals(resource, ((LightService) serv).resource);
      ((LightService) serv).containerId = null;
      ((LightService) serv).resource = null;
    }

    event = new AuxServicesEvent(AuxServicesEventType.CONTAINER_STOP, container);
    aux.handle(event);
    for (AuxiliaryService serv : servs) {
      assertEquals(containerId, ((LightService) serv).containerId);
      assertEquals(resource, ((LightService) serv).resource);
    }
  }

  @Test
  public void testAuxServices() {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] { "Asrv", "Bsrv" });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv"),
        ServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv"),
        ServiceB.class, Service.class);
    final AuxServices aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
        MOCK_CONTEXT, MOCK_DEL_SERVICE);
    aux.init(conf);

    int latch = 1;
    for (Service s : aux.getServices()) {
      assertEquals(INITED, s.getServiceState());
      if (s instanceof ServiceA) { latch *= 2; }
      else if (s instanceof ServiceB) { latch *= 3; }
      else fail("Unexpected service type " + s.getClass());
    }
    assertEquals("Invalid mix of services", 6, latch);
    aux.start();
    for (AuxiliaryService s : aux.getServices()) {
      assertEquals(STARTED, s.getServiceState());
      assertEquals(s.getAuxiliaryLocalPathHandler(),
          MOCK_AUX_PATH_HANDLER);
    }

    aux.stop();
    for (Service s : aux.getServices()) {
      assertEquals(STOPPED, s.getServiceState());
    }
  }


  @Test
  public void testAuxServicesMeta() {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] { "Asrv", "Bsrv" });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv"),
        ServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv"),
        ServiceB.class, Service.class);
    final AuxServices aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
        MOCK_CONTEXT, MOCK_DEL_SERVICE);
    aux.init(conf);

    int latch = 1;
    for (Service s : aux.getServices()) {
      assertEquals(INITED, s.getServiceState());
      if (s instanceof ServiceA) { latch *= 2; }
      else if (s instanceof ServiceB) { latch *= 3; }
      else fail("Unexpected service type " + s.getClass());
    }
    assertEquals("Invalid mix of services", 6, latch);
    aux.start();
    for (Service s : aux.getServices()) {
      assertEquals(STARTED, s.getServiceState());
    }

    Map<String, ByteBuffer> meta = aux.getMetaData();
    assertEquals(2, meta.size());
    assertEquals("A", new String(meta.get("Asrv").array()));
    assertEquals("B", new String(meta.get("Bsrv").array()));

    aux.stop();
    for (Service s : aux.getServices()) {
      assertEquals(STOPPED, s.getServiceState());
    }
  }



  @Test
  public void testAuxUnexpectedStop() {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] { "Asrv", "Bsrv" });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv"),
        ServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv"),
        ServiceB.class, Service.class);
    final AuxServices aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
        MOCK_CONTEXT, MOCK_DEL_SERVICE);
    aux.init(conf);
    aux.start();

    Service s = aux.getServices().iterator().next();
    s.stop();
    assertEquals("Auxiliary service stopped, but AuxService unaffected.",
        STOPPED, aux.getServiceState());
    assertTrue(aux.getServices().isEmpty());
  }

  @Test
  public void testValidAuxServiceName() {
    final AuxServices aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
        MOCK_CONTEXT, MOCK_DEL_SERVICE);
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] {"Asrv1", "Bsrv_2"});
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv1"),
        ServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv_2"),
        ServiceB.class, Service.class);
    try {
      aux.init(conf);
    } catch (Exception ex) {
      Assert.fail("Should not receive the exception.");
    }

    //Test bad auxService Name
    final AuxServices aux1 = new AuxServices(MOCK_AUX_PATH_HANDLER,
        MOCK_CONTEXT, MOCK_DEL_SERVICE);
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] {"1Asrv1"});
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "1Asrv1"),
        ServiceA.class, Service.class);
    try {
      aux1.init(conf);
      Assert.fail("Should receive the exception.");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("The ServiceName: 1Asrv1 set in " +
          "yarn.nodemanager.aux-services is invalid.The valid service name " +
          "should only contain a-zA-Z0-9_ and can not start with numbers"));
    }
  }

  @Test
  public void testAuxServiceRecoverySetup() throws IOException {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.NM_RECOVERY_DIR, TEST_DIR.toString());
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES,
        new String[] { "Asrv", "Bsrv" });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv"),
        RecoverableServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv"),
        RecoverableServiceB.class, Service.class);
    try {
      final AuxServices aux = new AuxServices(MOCK_AUX_PATH_HANDLER,
          MOCK_CONTEXT, MOCK_DEL_SERVICE);
      aux.init(conf);
      Assert.assertEquals(2, aux.getServices().size());
      File auxStorageDir = new File(TEST_DIR,
          AuxServices.STATE_STORE_ROOT_NAME);
      Assert.assertEquals(2, auxStorageDir.listFiles().length);
      aux.close();
    } finally {
      FileUtil.fullyDelete(TEST_DIR);
    }
  }

  static class RecoverableAuxService extends AuxiliaryService {
    static final FsPermission RECOVERY_PATH_PERMS =
        new FsPermission((short)0700);

    String auxName;

    RecoverableAuxService(String name, String auxName) {
      super(name);
      this.auxName = auxName;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      super.serviceInit(conf);
      Path storagePath = getRecoveryPath();
      Assert.assertNotNull("Recovery path not present when aux service inits",
          storagePath);
      Assert.assertTrue(storagePath.toString().contains(auxName));
      FileSystem fs = FileSystem.getLocal(conf);
      Assert.assertTrue("Recovery path does not exist",
          fs.exists(storagePath));
      Assert.assertEquals("Recovery path has wrong permissions",
          new FsPermission((short)0700),
          fs.getFileStatus(storagePath).getPermission());
    }

    @Override
    public void initializeApplication(
        ApplicationInitializationContext initAppContext) {
    }

    @Override
    public void stopApplication(ApplicationTerminationContext stopAppContext) {
    }

    @Override
    public ByteBuffer getMetaData() {
      return null;
    }
  }

  static class RecoverableServiceA extends RecoverableAuxService {
    RecoverableServiceA() {
      super("RecoverableServiceA", "Asrv");
    }
  }

  static class RecoverableServiceB extends RecoverableAuxService {
    RecoverableServiceB() {
      super("RecoverableServiceB", "Bsrv");
    }
  }
}
