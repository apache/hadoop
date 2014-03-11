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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.ResourceStatusType;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.LocalResourceStatusPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.LocalizerStatusPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService.LocalizerRunner;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService.LocalizerTracker;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService.PublicLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceFailedLocalizationEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestResourceLocalizationService {

  static final Path basedir =
      new Path("target", TestResourceLocalizationService.class.getName());
  static Server mockServer;

  private Configuration conf;
  private AbstractFileSystem spylfs;
  private FileContext lfs;
  
  @BeforeClass
  public static void setupClass() {
    mockServer = mock(Server.class);
    doReturn(new InetSocketAddress(123)).when(mockServer).getListenerAddress();
  }

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    spylfs = spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    lfs = FileContext.getFileContext(spylfs, conf);
    doNothing().when(spylfs).mkdir(
        isA(Path.class), isA(FsPermission.class), anyBoolean());
    String logDir = lfs.makeQualified(new Path(basedir, "logdir ")).toString();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir);
  }

  @After
  public void cleanup() {
    conf = null;
  }
  
  @Test
  public void testLocalizationInit() throws Exception {
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(new Configuration());

    ContainerExecutor exec = mock(ContainerExecutor.class);
    DeletionService delService = spy(new DeletionService(exec));
    delService.init(conf);
    delService.start();

    List<Path> localDirs = new ArrayList<Path>();
    String[] sDirs = new String[4];
    for (int i = 0; i < 4; ++i) {
      localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
      sDirs[i] = localDirs.get(i).toString();
    }
    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);

    LocalDirsHandlerService diskhandler = new LocalDirsHandlerService();
    diskhandler.init(conf);

    ResourceLocalizationService locService =
      spy(new ResourceLocalizationService(dispatcher, exec, delService,
                                          diskhandler));
    doReturn(lfs)
      .when(locService).getLocalFileContext(isA(Configuration.class));
    try {
      dispatcher.start();

      // initialize ResourceLocalizationService
      locService.init(conf);

      final FsPermission defaultPerm = new FsPermission((short)0755);

      // verify directory creation
      for (Path p : localDirs) {
        p = new Path((new URI(p.toString())).getPath());
        Path usercache = new Path(p, ContainerLocalizer.USERCACHE);
        verify(spylfs)
          .mkdir(eq(usercache),
              eq(defaultPerm), eq(true));
        Path publicCache = new Path(p, ContainerLocalizer.FILECACHE);
        verify(spylfs)
          .mkdir(eq(publicCache),
              eq(defaultPerm), eq(true));
        Path nmPriv = new Path(p, ResourceLocalizationService.NM_PRIVATE_DIR);
        verify(spylfs).mkdir(eq(nmPriv),
            eq(ResourceLocalizationService.NM_PRIVATE_PERM), eq(true));
      }
    } finally {
      dispatcher.stop();
      delService.stop();
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generics
  public void testResourceRelease() throws Exception {
    List<Path> localDirs = new ArrayList<Path>();
    String[] sDirs = new String[4];
    for (int i = 0; i < 4; ++i) {
      localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
      sDirs[i] = localDirs.get(i).toString();
    }
    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);

    LocalizerTracker mockLocallilzerTracker = mock(LocalizerTracker.class);
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    EventHandler<ApplicationEvent> applicationBus = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, applicationBus);
    EventHandler<ContainerEvent> containerBus = mock(EventHandler.class);
    dispatcher.register(ContainerEventType.class, containerBus);
    //Ignore actual localization
    EventHandler<LocalizerEvent> localizerBus = mock(EventHandler.class);
    dispatcher.register(LocalizerEventType.class, localizerBus);

    ContainerExecutor exec = mock(ContainerExecutor.class);
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);

    DeletionService delService = new DeletionService(exec);
    delService.init(new Configuration());
    delService.start();

    ResourceLocalizationService rawService =
      new ResourceLocalizationService(dispatcher, exec, delService,
                                      dirsHandler);
    ResourceLocalizationService spyService = spy(rawService);
    doReturn(mockServer).when(spyService).createServer();
    doReturn(mockLocallilzerTracker).when(spyService).createLocalizerTracker(
        isA(Configuration.class));
    doReturn(lfs).when(spyService)
        .getLocalFileContext(isA(Configuration.class));
    try {
      spyService.init(conf);
      spyService.start();

      final String user = "user0";
      // init application
      final Application app = mock(Application.class);
      final ApplicationId appId =
          BuilderUtils.newApplicationId(314159265358979L, 3);
      when(app.getUser()).thenReturn(user);
      when(app.getAppId()).thenReturn(appId);
      spyService.handle(new ApplicationLocalizationEvent(
          LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
      dispatcher.await();
            
      //Get a handle on the trackers after they're setup with INIT_APP_RESOURCES
      LocalResourcesTracker appTracker =
          spyService.getLocalResourcesTracker(
              LocalResourceVisibility.APPLICATION, user, appId);
      LocalResourcesTracker privTracker =
          spyService.getLocalResourcesTracker(LocalResourceVisibility.PRIVATE,
              user, appId);
      LocalResourcesTracker pubTracker =
          spyService.getLocalResourcesTracker(LocalResourceVisibility.PUBLIC,
              user, appId);

      // init container.
      final Container c = getMockContainer(appId, 42);
      
      // init resources
      Random r = new Random();
      long seed = r.nextLong();
      System.out.println("SEED: " + seed);
      r.setSeed(seed);
      
      // Send localization requests for one resource of each type.
      final LocalResource privResource = getPrivateMockedResource(r);
      final LocalResourceRequest privReq =
          new LocalResourceRequest(privResource);
      
      final LocalResource pubResource = getPublicMockedResource(r);
      final LocalResourceRequest pubReq = new LocalResourceRequest(pubResource);
      final LocalResource pubResource2 = getPublicMockedResource(r);
      final LocalResourceRequest pubReq2 =
          new LocalResourceRequest(pubResource2);
      
      final LocalResource appResource = getAppMockedResource(r);
      final LocalResourceRequest appReq = new LocalResourceRequest(appResource);
      
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
          new HashMap<LocalResourceVisibility, 
                      Collection<LocalResourceRequest>>();
      req.put(LocalResourceVisibility.PRIVATE,
          Collections.singletonList(privReq));
      req.put(LocalResourceVisibility.PUBLIC,
          Collections.singletonList(pubReq));
      req.put(LocalResourceVisibility.APPLICATION,
          Collections.singletonList(appReq));
      
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req2 =
        new HashMap<LocalResourceVisibility, 
                    Collection<LocalResourceRequest>>();
      req2.put(LocalResourceVisibility.PRIVATE,
          Collections.singletonList(privReq));
      req2.put(LocalResourceVisibility.PUBLIC,
          Collections.singletonList(pubReq2));
      
      Set<LocalResourceRequest> pubRsrcs = new HashSet<LocalResourceRequest>();
      pubRsrcs.add(pubReq);
      pubRsrcs.add(pubReq2);
      
      // Send Request event
      spyService.handle(new ContainerLocalizationRequestEvent(c, req));
      spyService.handle(new ContainerLocalizationRequestEvent(c, req2));
      dispatcher.await();

      int privRsrcCount = 0;
      for (LocalizedResource lr : privTracker) {
        privRsrcCount++;
        Assert.assertEquals("Incorrect reference count", 2, lr.getRefCount());
        Assert.assertEquals(privReq, lr.getRequest());
      }
      Assert.assertEquals(1, privRsrcCount);

      int pubRsrcCount = 0;
      for (LocalizedResource lr : pubTracker) {
        pubRsrcCount++;
        Assert.assertEquals("Incorrect reference count", 1, lr.getRefCount());
        pubRsrcs.remove(lr.getRequest());
      }
      Assert.assertEquals(0, pubRsrcs.size());
      Assert.assertEquals(2, pubRsrcCount);

      int appRsrcCount = 0;
      for (LocalizedResource lr : appTracker) {
        appRsrcCount++;
        Assert.assertEquals("Incorrect reference count", 1, lr.getRefCount());
        Assert.assertEquals(appReq, lr.getRequest());
      }
      Assert.assertEquals(1, appRsrcCount);
      
      //Send Cleanup Event
      spyService.handle(new ContainerLocalizationCleanupEvent(c, req));
      verify(mockLocallilzerTracker)
        .cleanupPrivLocalizers("container_314159265358979_0003_01_000042");
      req2.remove(LocalResourceVisibility.PRIVATE);
      spyService.handle(new ContainerLocalizationCleanupEvent(c, req2));
      dispatcher.await();
      
      pubRsrcs.add(pubReq);
      pubRsrcs.add(pubReq2);

      privRsrcCount = 0;
      for (LocalizedResource lr : privTracker) {
        privRsrcCount++;
        Assert.assertEquals("Incorrect reference count", 1, lr.getRefCount());
        Assert.assertEquals(privReq, lr.getRequest());
      }
      Assert.assertEquals(1, privRsrcCount);

      pubRsrcCount = 0;
      for (LocalizedResource lr : pubTracker) {
        pubRsrcCount++;
        Assert.assertEquals("Incorrect reference count", 0, lr.getRefCount());
        pubRsrcs.remove(lr.getRequest());
      }
      Assert.assertEquals(0, pubRsrcs.size());
      Assert.assertEquals(2, pubRsrcCount);

      appRsrcCount = 0;
      for (LocalizedResource lr : appTracker) {
        appRsrcCount++;
        Assert.assertEquals("Incorrect reference count", 0, lr.getRefCount());
        Assert.assertEquals(appReq, lr.getRequest());
      }
      Assert.assertEquals(1, appRsrcCount);
    } finally {
      dispatcher.stop();
      delService.stop();
    }
  }
  
  @Test( timeout = 10000)
  @SuppressWarnings("unchecked") // mocked generics
  public void testLocalizationHeartbeat() throws Exception {
    List<Path> localDirs = new ArrayList<Path>();
    String[] sDirs = new String[1];
    // Making sure that we have only one local disk so that it will only be
    // selected for consecutive resource localization calls.  This is required
    // to test LocalCacheDirectoryManager.
    localDirs.add(lfs.makeQualified(new Path(basedir, 0 + "")));
    sDirs[0] = localDirs.get(0).toString();

    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);
    // Adding configuration to make sure there is only one file per
    // directory
    conf.set(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY, "37");
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    EventHandler<ApplicationEvent> applicationBus = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, applicationBus);
    EventHandler<ContainerEvent> containerBus = mock(EventHandler.class);
    dispatcher.register(ContainerEventType.class, containerBus);

    ContainerExecutor exec = mock(ContainerExecutor.class);
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);

    DeletionService delServiceReal = new DeletionService(exec);
    DeletionService delService = spy(delServiceReal);
    delService.init(new Configuration());
    delService.start();

    ResourceLocalizationService rawService =
      new ResourceLocalizationService(dispatcher, exec, delService,
                                      dirsHandler);
    ResourceLocalizationService spyService = spy(rawService);
    doReturn(mockServer).when(spyService).createServer();
    doReturn(lfs).when(spyService).getLocalFileContext(isA(Configuration.class));
    try {
      spyService.init(conf);
      spyService.start();

      // init application
      final Application app = mock(Application.class);
      final ApplicationId appId =
          BuilderUtils.newApplicationId(314159265358979L, 3);
      when(app.getUser()).thenReturn("user0");
      when(app.getAppId()).thenReturn(appId);
      spyService.handle(new ApplicationLocalizationEvent(
          LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
      ArgumentMatcher<ApplicationEvent> matchesAppInit =
        new ArgumentMatcher<ApplicationEvent>() {
          @Override
          public boolean matches(Object o) {
            ApplicationEvent evt = (ApplicationEvent) o;
            return evt.getType() == ApplicationEventType.APPLICATION_INITED
              && appId == evt.getApplicationID();
          }
        };
      dispatcher.await();
      verify(applicationBus).handle(argThat(matchesAppInit));

      // init container rsrc, localizer
      Random r = new Random();
      long seed = r.nextLong();
      System.out.println("SEED: " + seed);
      r.setSeed(seed);
      final Container c = getMockContainer(appId, 42);
      FSDataOutputStream out =
        new FSDataOutputStream(new DataOutputBuffer(), null);
      doReturn(out).when(spylfs).createInternal(isA(Path.class),
          isA(EnumSet.class), isA(FsPermission.class), anyInt(), anyShort(),
          anyLong(), isA(Progressable.class), isA(ChecksumOpt.class), anyBoolean());
      final LocalResource resource1 = getPrivateMockedResource(r);
      LocalResource resource2 = null;
      do {
        resource2 = getPrivateMockedResource(r);
      } while (resource2 == null || resource2.equals(resource1));
      // above call to make sure we don't get identical resources.
      
      final LocalResourceRequest req1 = new LocalResourceRequest(resource1);
      final LocalResourceRequest req2 = new LocalResourceRequest(resource2);
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs =
        new HashMap<LocalResourceVisibility, 
                    Collection<LocalResourceRequest>>();
      List<LocalResourceRequest> privateResourceList =
          new ArrayList<LocalResourceRequest>();
      privateResourceList.add(req1);
      privateResourceList.add(req2);
      rsrcs.put(LocalResourceVisibility.PRIVATE, privateResourceList);
      spyService.handle(new ContainerLocalizationRequestEvent(c, rsrcs));
      // Sigh. Thread init of private localizer not accessible
      Thread.sleep(1000);
      dispatcher.await();
      String appStr = ConverterUtils.toString(appId);
      String ctnrStr = c.getContainerId().toString();
      ArgumentCaptor<Path> tokenPathCaptor = ArgumentCaptor.forClass(Path.class);
      verify(exec).startLocalizer(tokenPathCaptor.capture(),
          isA(InetSocketAddress.class), eq("user0"), eq(appStr), eq(ctnrStr),
          isA(List.class), isA(List.class));
      Path localizationTokenPath = tokenPathCaptor.getValue();

      // heartbeat from localizer
      LocalResourceStatus rsrcStat1 = mock(LocalResourceStatus.class);
      LocalResourceStatus rsrcStat2 = mock(LocalResourceStatus.class);
      LocalizerStatus stat = mock(LocalizerStatus.class);
      when(stat.getLocalizerId()).thenReturn(ctnrStr);
      when(rsrcStat1.getResource()).thenReturn(resource1);
      when(rsrcStat2.getResource()).thenReturn(resource2);
      when(rsrcStat1.getLocalSize()).thenReturn(4344L);
      when(rsrcStat2.getLocalSize()).thenReturn(2342L);
      URL locPath = getPath("/cache/private/blah");
      when(rsrcStat1.getLocalPath()).thenReturn(locPath);
      when(rsrcStat2.getLocalPath()).thenReturn(locPath);
      when(rsrcStat1.getStatus()).thenReturn(ResourceStatusType.FETCH_SUCCESS);
      when(rsrcStat2.getStatus()).thenReturn(ResourceStatusType.FETCH_SUCCESS);
      when(stat.getResources())
        .thenReturn(Collections.<LocalResourceStatus>emptyList())
        .thenReturn(Collections.singletonList(rsrcStat1))
        .thenReturn(Collections.singletonList(rsrcStat2))
        .thenReturn(Collections.<LocalResourceStatus>emptyList());

      String localPath = Path.SEPARATOR + ContainerLocalizer.USERCACHE +
          Path.SEPARATOR + "user0" + Path.SEPARATOR +
          ContainerLocalizer.FILECACHE;
      
      // get first resource
      LocalizerHeartbeatResponse response = spyService.heartbeat(stat);
      assertEquals(LocalizerAction.LIVE, response.getLocalizerAction());
      assertEquals(1, response.getResourceSpecs().size());
      assertEquals(req1,
        new LocalResourceRequest(response.getResourceSpecs().get(0).getResource()));
      URL localizedPath =
          response.getResourceSpecs().get(0).getDestinationDirectory();
      // Appending to local path unique number(10) generated as a part of
      // LocalResourcesTracker
      assertTrue(localizedPath.getFile().endsWith(
        localPath + Path.SEPARATOR + "10"));

      // get second resource
      response = spyService.heartbeat(stat);
      assertEquals(LocalizerAction.LIVE, response.getLocalizerAction());
      assertEquals(1, response.getResourceSpecs().size());
      assertEquals(req2, new LocalResourceRequest(response.getResourceSpecs()
        .get(0).getResource()));
      localizedPath =
          response.getResourceSpecs().get(0).getDestinationDirectory();
      // Resource's destination path should be now inside sub directory 0 as
      // LocalCacheDirectoryManager will be used and we have restricted number
      // of files per directory to 1.
      assertTrue(localizedPath.getFile().endsWith(
        localPath + Path.SEPARATOR + "0" + Path.SEPARATOR + "11"));

      // empty rsrc
      response = spyService.heartbeat(stat);
      assertEquals(LocalizerAction.LIVE, response.getLocalizerAction());
      assertEquals(0, response.getResourceSpecs().size());

      // get shutdown
      response = spyService.heartbeat(stat);
      assertEquals(LocalizerAction.DIE, response.getLocalizerAction());


      dispatcher.await();
      // verify container notification
      ArgumentMatcher<ContainerEvent> matchesContainerLoc =
        new ArgumentMatcher<ContainerEvent>() {
          @Override
          public boolean matches(Object o) {
            ContainerEvent evt = (ContainerEvent) o;
            return evt.getType() == ContainerEventType.RESOURCE_LOCALIZED
              && c.getContainerId() == evt.getContainerID();
          }
        };
      // total 2 resource localzation calls. one for each resource.
      verify(containerBus, times(2)).handle(argThat(matchesContainerLoc));
        
      // Verify deletion of localization token.
      verify(delService).delete((String)isNull(), eq(localizationTokenPath));
    } finally {
      spyService.stop();
      dispatcher.stop();
      delService.stop();
    }
  }

  @Test(timeout=20000)
  @SuppressWarnings("unchecked") // mocked generics
  public void testFailedPublicResource() throws Exception {
    List<Path> localDirs = new ArrayList<Path>();
    String[] sDirs = new String[4];
    for (int i = 0; i < 4; ++i) {
      localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
      sDirs[i] = localDirs.get(i).toString();
    }
    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);

    DrainDispatcher dispatcher = new DrainDispatcher();
    EventHandler<ApplicationEvent> applicationBus = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, applicationBus);
    EventHandler<ContainerEvent> containerBus = mock(EventHandler.class);
    dispatcher.register(ContainerEventType.class, containerBus);

    ContainerExecutor exec = mock(ContainerExecutor.class);
    DeletionService delService = mock(DeletionService.class);
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);

    dispatcher.init(conf);
    dispatcher.start();

    try {
      ResourceLocalizationService rawService =
          new ResourceLocalizationService(dispatcher, exec, delService,
                                        dirsHandler);
      ResourceLocalizationService spyService = spy(rawService);
      doReturn(mockServer).when(spyService).createServer();
      doReturn(lfs).when(spyService).getLocalFileContext(
          isA(Configuration.class));

      spyService.init(conf);
      spyService.start();

      final String user = "user0";
      // init application
      final Application app = mock(Application.class);
      final ApplicationId appId =
          BuilderUtils.newApplicationId(314159265358979L, 3);
      when(app.getUser()).thenReturn(user);
      when(app.getAppId()).thenReturn(appId);
      spyService.handle(new ApplicationLocalizationEvent(
          LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
      dispatcher.await();

      // init container.
      final Container c = getMockContainer(appId, 42);

      // init resources
      Random r = new Random();
      long seed = r.nextLong();
      System.out.println("SEED: " + seed);
      r.setSeed(seed);

      // cause chmod to fail after a delay
      final CyclicBarrier barrier = new CyclicBarrier(2);
      doAnswer(new Answer<Void>() {
          public Void answer(InvocationOnMock invocation) throws IOException {
            try {
              barrier.await();
            } catch (InterruptedException e) {
            } catch (BrokenBarrierException e) {
            }
            throw new IOException("forced failure");
          }
        }).when(spylfs)
            .setPermission(isA(Path.class), isA(FsPermission.class));

      // Queue up two localization requests for the same public resource
      final LocalResource pubResource = getPublicMockedResource(r);
      final LocalResourceRequest pubReq = new LocalResourceRequest(pubResource);

      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
          new HashMap<LocalResourceVisibility,
                      Collection<LocalResourceRequest>>();
      req.put(LocalResourceVisibility.PUBLIC,
          Collections.singletonList(pubReq));

      Set<LocalResourceRequest> pubRsrcs = new HashSet<LocalResourceRequest>();
      pubRsrcs.add(pubReq);

      spyService.handle(new ContainerLocalizationRequestEvent(c, req));
      spyService.handle(new ContainerLocalizationRequestEvent(c, req));
      dispatcher.await();

      // allow the chmod to fail now that both requests have been queued
      barrier.await();
      verify(containerBus, timeout(5000).times(2))
          .handle(isA(ContainerResourceFailedEvent.class));
    } finally {
      dispatcher.stop();
    }
  }
  
  /*
   * Test case for handling RejectedExecutionException and IOException which can
   * be thrown when adding public resources to the pending queue.
   * RejectedExecutionException can be thrown either due to the incoming queue
   * being full or if the ExecutorCompletionService threadpool is shutdown.
   * Since it's hard to simulate the queue being full, this test just shuts down
   * the threadpool and makes sure the exception is handled. If anything is
   * messed up the async dispatcher thread will cause a system exit causing the
   * test to fail.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testPublicResourceAddResourceExceptions() throws Exception {
    List<Path> localDirs = new ArrayList<Path>();
    String[] sDirs = new String[4];
    for (int i = 0; i < 4; ++i) {
      localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
      sDirs[i] = localDirs.get(i).toString();
    }
    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    DrainDispatcher dispatcher = new DrainDispatcher();
    EventHandler<ApplicationEvent> applicationBus = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, applicationBus);
    EventHandler<ContainerEvent> containerBus = mock(EventHandler.class);
    dispatcher.register(ContainerEventType.class, containerBus);

    ContainerExecutor exec = mock(ContainerExecutor.class);
    DeletionService delService = mock(DeletionService.class);
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    LocalDirsHandlerService dirsHandlerSpy = spy(dirsHandler);
    dirsHandlerSpy.init(conf);

    dispatcher.init(conf);
    dispatcher.start();

    try {
      ResourceLocalizationService rawService =
          new ResourceLocalizationService(dispatcher, exec, delService,
            dirsHandlerSpy);
      ResourceLocalizationService spyService = spy(rawService);
      doReturn(mockServer).when(spyService).createServer();
      doReturn(lfs).when(spyService).getLocalFileContext(
        isA(Configuration.class));

      spyService.init(conf);
      spyService.start();

      final String user = "user0";
      // init application
      final Application app = mock(Application.class);
      final ApplicationId appId =
          BuilderUtils.newApplicationId(314159265358979L, 3);
      when(app.getUser()).thenReturn(user);
      when(app.getAppId()).thenReturn(appId);
      spyService.handle(new ApplicationLocalizationEvent(
        LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
      dispatcher.await();

      // init resources
      Random r = new Random();
      r.setSeed(r.nextLong());

      // Queue localization request for the public resource
      final LocalResource pubResource = getPublicMockedResource(r);
      final LocalResourceRequest pubReq = new LocalResourceRequest(pubResource);
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
          new HashMap<LocalResourceVisibility, Collection<LocalResourceRequest>>();
      req
        .put(LocalResourceVisibility.PUBLIC, Collections.singletonList(pubReq));

      // init container.
      final Container c = getMockContainer(appId, 42);

      // first test ioexception
      Mockito
        .doThrow(new IOException())
        .when(dirsHandlerSpy)
        .getLocalPathForWrite(isA(String.class), Mockito.anyLong(),
          Mockito.anyBoolean());
      // send request
      spyService.handle(new ContainerLocalizationRequestEvent(c, req));
      dispatcher.await();
      LocalResourcesTracker tracker =
          spyService.getLocalResourcesTracker(LocalResourceVisibility.PUBLIC,
            user, appId);
      Assert.assertNull(tracker.getLocalizedResource(pubReq));

      // test RejectedExecutionException
      Mockito
        .doCallRealMethod()
        .when(dirsHandlerSpy)
        .getLocalPathForWrite(isA(String.class), Mockito.anyLong(),
          Mockito.anyBoolean());

      // shutdown the thread pool
      PublicLocalizer publicLocalizer = spyService.getPublicLocalizer();
      publicLocalizer.threadPool.shutdown();

      spyService.handle(new ContainerLocalizationRequestEvent(c, req));
      dispatcher.await();
      tracker =
          spyService.getLocalResourcesTracker(LocalResourceVisibility.PUBLIC,
            user, appId);
      Assert.assertNull(tracker.getLocalizedResource(pubReq));

    } finally {
      // if we call stop with events in the queue, an InterruptedException gets
      // thrown resulting in the dispatcher thread causing a system exit
      dispatcher.await();
      dispatcher.stop();
    }
  }

  @Test(timeout = 100000)
  @SuppressWarnings("unchecked")
  public void testParallelDownloadAttemptsForPrivateResource() throws Exception {

    DrainDispatcher dispatcher1 = null;
    try {
      dispatcher1 = new DrainDispatcher();
      String user = "testuser";
      ApplicationId appId = BuilderUtils.newApplicationId(1, 1);

      // creating one local directory
      List<Path> localDirs = new ArrayList<Path>();
      String[] sDirs = new String[1];
      for (int i = 0; i < 1; ++i) {
        localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
        sDirs[i] = localDirs.get(i).toString();
      }
      conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);

      LocalDirsHandlerService localDirHandler = new LocalDirsHandlerService();
      localDirHandler.init(conf);
      // Registering event handlers
      EventHandler<ApplicationEvent> applicationBus = mock(EventHandler.class);
      dispatcher1.register(ApplicationEventType.class, applicationBus);
      EventHandler<ContainerEvent> containerBus = mock(EventHandler.class);
      dispatcher1.register(ContainerEventType.class, containerBus);

      ContainerExecutor exec = mock(ContainerExecutor.class);
      DeletionService delService = mock(DeletionService.class);
      LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
      // initializing directory handler.
      dirsHandler.init(conf);

      dispatcher1.init(conf);
      dispatcher1.start();

      ResourceLocalizationService rls =
          new ResourceLocalizationService(dispatcher1, exec, delService,
            localDirHandler);
      dispatcher1.register(LocalizationEventType.class, rls);
      rls.init(conf);

      rls.handle(createApplicationLocalizationEvent(user, appId));

      LocalResourceRequest req =
          new LocalResourceRequest(new Path("file:///tmp"), 123L,
            LocalResourceType.FILE, LocalResourceVisibility.PRIVATE, "");

      // We need to pre-populate the LocalizerRunner as the
      // Resource Localization Service code internally starts them which
      // definitely we don't want.

      // creating new containers and populating corresponding localizer runners

      // Container - 1
      ContainerImpl container1 = createMockContainer(user, 1);
      String localizerId1 = container1.getContainerId().toString();
      rls.getPrivateLocalizers().put(
        localizerId1,
        rls.new LocalizerRunner(new LocalizerContext(user, container1
          .getContainerId(), null), localizerId1));
      LocalizerRunner localizerRunner1 = rls.getLocalizerRunner(localizerId1);

      dispatcher1.getEventHandler().handle(
        createContainerLocalizationEvent(container1,
          LocalResourceVisibility.PRIVATE, req));
      Assert
        .assertTrue(waitForPrivateDownloadToStart(rls, localizerId1, 1, 200));

      // Container - 2 now makes the request.
      ContainerImpl container2 = createMockContainer(user, 2);
      String localizerId2 = container2.getContainerId().toString();
      rls.getPrivateLocalizers().put(
        localizerId2,
        rls.new LocalizerRunner(new LocalizerContext(user, container2
          .getContainerId(), null), localizerId2));
      LocalizerRunner localizerRunner2 = rls.getLocalizerRunner(localizerId2);
      dispatcher1.getEventHandler().handle(
        createContainerLocalizationEvent(container2,
          LocalResourceVisibility.PRIVATE, req));
      Assert
        .assertTrue(waitForPrivateDownloadToStart(rls, localizerId2, 1, 200));

      // Retrieving localized resource.
      LocalResourcesTracker tracker =
          rls.getLocalResourcesTracker(LocalResourceVisibility.PRIVATE, user,
            appId);
      LocalizedResource lr = tracker.getLocalizedResource(req);
      // Resource would now have moved into DOWNLOADING state
      Assert.assertEquals(ResourceState.DOWNLOADING, lr.getState());
      // Resource should have one permit
      Assert.assertEquals(1, lr.sem.availablePermits());

      // Resource Localization Service receives first heart beat from
      // ContainerLocalizer for container1
      LocalizerHeartbeatResponse response1 =
          rls.heartbeat(createLocalizerStatus(localizerId1));

      // Resource must have been added to scheduled map
      Assert.assertEquals(1, localizerRunner1.scheduled.size());
      // Checking resource in the response and also available permits for it.
      Assert.assertEquals(req.getResource(), response1.getResourceSpecs()
        .get(0).getResource().getResource());
      Assert.assertEquals(0, lr.sem.availablePermits());

      // Resource Localization Service now receives first heart beat from
      // ContainerLocalizer for container2
      LocalizerHeartbeatResponse response2 =
          rls.heartbeat(createLocalizerStatus(localizerId2));

      // Resource must not have been added to scheduled map
      Assert.assertEquals(0, localizerRunner2.scheduled.size());
      // No resource is returned in response
      Assert.assertEquals(0, response2.getResourceSpecs().size());

      // ContainerLocalizer - 1 now sends failed resource heartbeat.
      rls.heartbeat(createLocalizerStatusForFailedResource(localizerId1, req));

      // Resource Localization should fail and state is modified accordingly.
      // Also Local should be release on the LocalizedResource.
      Assert
        .assertTrue(waitForResourceState(lr, rls, req,
          LocalResourceVisibility.PRIVATE, user, appId, ResourceState.FAILED,
          200));
      Assert.assertTrue(lr.getState().equals(ResourceState.FAILED));
      Assert.assertEquals(0, localizerRunner1.scheduled.size());

      // Now Container-2 once again sends heart beat to resource localization
      // service

      // Now container-2 again try to download the resource it should still
      // not get the resource as the resource is now not in DOWNLOADING state.
      response2 = rls.heartbeat(createLocalizerStatus(localizerId2));

      // Resource must not have been added to scheduled map.
      // Also as the resource has failed download it will be removed from
      // pending list.
      Assert.assertEquals(0, localizerRunner2.scheduled.size());
      Assert.assertEquals(0, localizerRunner2.pending.size());
      Assert.assertEquals(0, response2.getResourceSpecs().size());

    } finally {
      if (dispatcher1 != null) {
        dispatcher1.stop();
      }
    }
  }
  
  

  @Test(timeout = 10000)
  @SuppressWarnings("unchecked")
  public void testLocalResourcePath() throws Exception {

    // test the local path where application and user cache files will be
    // localized.

    DrainDispatcher dispatcher1 = null;
    try {
      dispatcher1 = new DrainDispatcher();
      String user = "testuser";
      ApplicationId appId = BuilderUtils.newApplicationId(1, 1);

      // creating one local directory
      List<Path> localDirs = new ArrayList<Path>();
      String[] sDirs = new String[1];
      for (int i = 0; i < 1; ++i) {
        localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
        sDirs[i] = localDirs.get(i).toString();
      }
      conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);

      LocalDirsHandlerService localDirHandler = new LocalDirsHandlerService();
      localDirHandler.init(conf);
      // Registering event handlers
      EventHandler<ApplicationEvent> applicationBus = mock(EventHandler.class);
      dispatcher1.register(ApplicationEventType.class, applicationBus);
      EventHandler<ContainerEvent> containerBus = mock(EventHandler.class);
      dispatcher1.register(ContainerEventType.class, containerBus);

      ContainerExecutor exec = mock(ContainerExecutor.class);
      DeletionService delService = mock(DeletionService.class);
      LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
      // initializing directory handler.
      dirsHandler.init(conf);

      dispatcher1.init(conf);
      dispatcher1.start();

      ResourceLocalizationService rls =
          new ResourceLocalizationService(dispatcher1, exec, delService,
            localDirHandler);
      dispatcher1.register(LocalizationEventType.class, rls);
      rls.init(conf);

      rls.handle(createApplicationLocalizationEvent(user, appId));

      // We need to pre-populate the LocalizerRunner as the
      // Resource Localization Service code internally starts them which
      // definitely we don't want.

      // creating new container and populating corresponding localizer runner

      // Container - 1
      Container container1 = createMockContainer(user, 1);
      String localizerId1 = container1.getContainerId().toString();
      rls.getPrivateLocalizers().put(
        localizerId1,
        rls.new LocalizerRunner(new LocalizerContext(user, container1
          .getContainerId(), null), localizerId1));

      // Creating two requests for container
      // 1) Private resource
      // 2) Application resource
      LocalResourceRequest reqPriv =
          new LocalResourceRequest(new Path("file:///tmp1"), 123L,
            LocalResourceType.FILE, LocalResourceVisibility.PRIVATE, "");
      List<LocalResourceRequest> privList =
          new ArrayList<LocalResourceRequest>();
      privList.add(reqPriv);

      LocalResourceRequest reqApp =
          new LocalResourceRequest(new Path("file:///tmp2"), 123L,
            LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, "");
      List<LocalResourceRequest> appList =
          new ArrayList<LocalResourceRequest>();
      appList.add(reqApp);

      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs =
          new HashMap<LocalResourceVisibility, Collection<LocalResourceRequest>>();
      rsrcs.put(LocalResourceVisibility.APPLICATION, appList);
      rsrcs.put(LocalResourceVisibility.PRIVATE, privList);

      dispatcher1.getEventHandler().handle(
        new ContainerLocalizationRequestEvent(container1, rsrcs));

      // Now waiting for resource download to start. Here actual will not start
      // Only the resources will be populated into pending list.
      Assert
        .assertTrue(waitForPrivateDownloadToStart(rls, localizerId1, 2, 500));

      // Validating user and application cache paths

      String userCachePath =
          StringUtils.join(Path.SEPARATOR, Arrays.asList(localDirs.get(0)
            .toUri().getRawPath(), ContainerLocalizer.USERCACHE, user,
            ContainerLocalizer.FILECACHE));
      String userAppCachePath =
          StringUtils.join(Path.SEPARATOR, Arrays.asList(localDirs.get(0)
            .toUri().getRawPath(), ContainerLocalizer.USERCACHE, user,
            ContainerLocalizer.APPCACHE, appId.toString(),
            ContainerLocalizer.FILECACHE));

      // Now the Application and private resources may come in any order
      // for download.
      // For User cahce :
      // returned destinationPath = user cache path + random number
      // For App cache :
      // returned destinationPath = user app cache path + random number

      int returnedResources = 0;
      boolean appRsrc = false, privRsrc = false;
      while (returnedResources < 2) {
        LocalizerHeartbeatResponse response =
            rls.heartbeat(createLocalizerStatus(localizerId1));
        for (ResourceLocalizationSpec resourceSpec : response
          .getResourceSpecs()) {
          returnedResources++;
          Path destinationDirectory =
              new Path(resourceSpec.getDestinationDirectory().getFile());
          if (resourceSpec.getResource().getVisibility() ==
              LocalResourceVisibility.APPLICATION) {
            appRsrc = true;
            Assert.assertEquals(userAppCachePath, destinationDirectory
              .getParent().toUri().toString());
          } else if (resourceSpec.getResource().getVisibility() == 
              LocalResourceVisibility.PRIVATE) {
            privRsrc = true;
            Assert.assertEquals(userCachePath, destinationDirectory.getParent()
              .toUri().toString());
          } else {
            throw new Exception("Unexpected resource recevied.");
          }
        }
      }
      // We should receive both the resources (Application and Private)
      Assert.assertTrue(appRsrc && privRsrc);
    } finally {
      if (dispatcher1 != null) {
        dispatcher1.stop();
      }
    }
  }

  private LocalizerStatus createLocalizerStatusForFailedResource(
      String localizerId, LocalResourceRequest req) {
    LocalizerStatus status = createLocalizerStatus(localizerId);
    LocalResourceStatus resourceStatus = new LocalResourceStatusPBImpl();
    resourceStatus.setException(SerializedException
      .newInstance(new YarnException("test")));
    resourceStatus.setStatus(ResourceStatusType.FETCH_FAILURE);
    resourceStatus.setResource(req);
    status.addResourceStatus(resourceStatus);
    return status;
  }

  private LocalizerStatus createLocalizerStatus(String localizerId1) {
    LocalizerStatus status = new LocalizerStatusPBImpl();
    status.setLocalizerId(localizerId1);
    return status;
  }

  private LocalizationEvent createApplicationLocalizationEvent(String user,
      ApplicationId appId) {
    Application app = mock(Application.class);
    when(app.getUser()).thenReturn(user);
    when(app.getAppId()).thenReturn(appId);
    return new ApplicationLocalizationEvent(
      LocalizationEventType.INIT_APPLICATION_RESOURCES, app);
  }

  @Test(timeout = 100000)
  @SuppressWarnings("unchecked")
  public void testParallelDownloadAttemptsForPublicResource() throws Exception {

    DrainDispatcher dispatcher1 = null;
    String user = "testuser";
    try {
      // creating one local directory
      List<Path> localDirs = new ArrayList<Path>();
      String[] sDirs = new String[1];
      for (int i = 0; i < 1; ++i) {
        localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
        sDirs[i] = localDirs.get(i).toString();
      }
      conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);

      // Registering event handlers
      EventHandler<ApplicationEvent> applicationBus = mock(EventHandler.class);
      dispatcher1 = new DrainDispatcher();
      dispatcher1.register(ApplicationEventType.class, applicationBus);
      EventHandler<ContainerEvent> containerBus = mock(EventHandler.class);
      dispatcher1.register(ContainerEventType.class, containerBus);

      ContainerExecutor exec = mock(ContainerExecutor.class);
      DeletionService delService = mock(DeletionService.class);
      LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
      // initializing directory handler.
      dirsHandler.init(conf);

      dispatcher1.init(conf);
      dispatcher1.start();

      // Creating and initializing ResourceLocalizationService but not starting
      // it as otherwise it will remove requests from pending queue.
      ResourceLocalizationService rawService =
          new ResourceLocalizationService(dispatcher1, exec, delService,
            dirsHandler);
      ResourceLocalizationService spyService = spy(rawService);
      dispatcher1.register(LocalizationEventType.class, spyService);
      spyService.init(conf);

      // Initially pending map should be empty for public localizer
      Assert.assertEquals(0, spyService.getPublicLocalizer().pending.size());

      LocalResourceRequest req =
          new LocalResourceRequest(new Path("/tmp"), 123L,
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, "");

      // Initializing application
      ApplicationImpl app = mock(ApplicationImpl.class);
      ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
      when(app.getAppId()).thenReturn(appId);
      when(app.getUser()).thenReturn(user);
      dispatcher1.getEventHandler().handle(
        new ApplicationLocalizationEvent(
          LocalizationEventType.INIT_APPLICATION_RESOURCES, app));

      // Container - 1

      // container requesting the resource
      ContainerImpl container1 = createMockContainer(user, 1);
      dispatcher1.getEventHandler().handle(
        createContainerLocalizationEvent(container1,
          LocalResourceVisibility.PUBLIC, req));

      // Waiting for resource to change into DOWNLOADING state.
      Assert.assertTrue(waitForResourceState(null, spyService, req,
        LocalResourceVisibility.PUBLIC, user, null, ResourceState.DOWNLOADING,
        200));

      // Waiting for download to start.
      Assert.assertTrue(waitForPublicDownloadToStart(spyService, 1, 200));

      LocalizedResource lr =
          getLocalizedResource(spyService, req, LocalResourceVisibility.PUBLIC,
            user, null);
      // Resource would now have moved into DOWNLOADING state
      Assert.assertEquals(ResourceState.DOWNLOADING, lr.getState());

      // pending should have this resource now.
      Assert.assertEquals(1, spyService.getPublicLocalizer().pending.size());
      // Now resource should have 0 permit.
      Assert.assertEquals(0, lr.sem.availablePermits());

      // Container - 2

      // Container requesting the same resource.
      ContainerImpl container2 = createMockContainer(user, 2);
      dispatcher1.getEventHandler().handle(
        createContainerLocalizationEvent(container2,
          LocalResourceVisibility.PUBLIC, req));

      // Waiting for download to start. This should return false as new download
      // will not start
      Assert.assertFalse(waitForPublicDownloadToStart(spyService, 2, 100));

      // Now Failing the resource download. As a part of it
      // resource state is changed and then lock is released.
      ResourceFailedLocalizationEvent locFailedEvent =
          new ResourceFailedLocalizationEvent(
              req,new Exception("test").toString());
      spyService.getLocalResourcesTracker(LocalResourceVisibility.PUBLIC, user,
        null).handle(locFailedEvent);

      // Waiting for resource to change into FAILED state.
      Assert.assertTrue(waitForResourceState(lr, spyService, req,
        LocalResourceVisibility.PUBLIC, user, null, ResourceState.FAILED, 200));
      // releasing lock as a part of download failed process.
      lr.unlock();
      // removing pending download request.
      spyService.getPublicLocalizer().pending.clear();

      // Now I need to simulate a race condition wherein Event is added to
      // dispatcher before resource state changes to either FAILED or LOCALIZED
      // Hence sending event directly to dispatcher.
      LocalizerResourceRequestEvent localizerEvent =
          new LocalizerResourceRequestEvent(lr, null,
            mock(LocalizerContext.class), null);

      dispatcher1.getEventHandler().handle(localizerEvent);
      // Waiting for download to start. This should return false as new download
      // will not start
      Assert.assertFalse(waitForPublicDownloadToStart(spyService, 1, 100));
      // Checking available permits now.
      Assert.assertEquals(1, lr.sem.availablePermits());

    } finally {
      if (dispatcher1 != null) {
        dispatcher1.stop();
      }
    }

  }

  private boolean waitForPrivateDownloadToStart(
      ResourceLocalizationService service, String localizerId, int size,
      int maxWaitTime) {
    List<LocalizerResourceRequestEvent> pending = null;
    // Waiting for localizer to be created.
    do {
      if (service.getPrivateLocalizers().get(localizerId) != null) {
        pending = service.getPrivateLocalizers().get(localizerId).pending;
      }
      if (pending == null) {
        try {
          maxWaitTime -= 20;
          Thread.sleep(20);
        } catch (Exception e) {
        }
      } else {
        break;
      }
    } while (maxWaitTime > 0);
    if (pending == null) {
      return false;
    }
    do {
      if (pending.size() == size) {
        return true;
      } else {
        try {
          maxWaitTime -= 20;
          Thread.sleep(20);
        } catch (Exception e) {
        }
      }
    } while (maxWaitTime > 0);
    return pending.size() == size;
  }

  private boolean waitForPublicDownloadToStart(
      ResourceLocalizationService service, int size, int maxWaitTime) {
    Map<Future<Path>, LocalizerResourceRequestEvent> pending = null;
    // Waiting for localizer to be created.
    do {
      if (service.getPublicLocalizer() != null) {
        pending = service.getPublicLocalizer().pending;
      }
      if (pending == null) {
        try {
          maxWaitTime -= 20;
          Thread.sleep(20);
        } catch (Exception e) {
        }
      } else {
        break;
      }
    } while (maxWaitTime > 0);
    if (pending == null) {
      return false;
    }
    do {
      if (pending.size() == size) {
        return true;
      } else {
        try {
          maxWaitTime -= 20;
          Thread.sleep(20);
        } catch (InterruptedException e) {
        }
      }
    } while (maxWaitTime > 0);
    return pending.size() == size;

  }

  private LocalizedResource getLocalizedResource(
      ResourceLocalizationService service, LocalResourceRequest req,
      LocalResourceVisibility vis, String user, ApplicationId appId) {
    return service.getLocalResourcesTracker(vis, user, appId)
      .getLocalizedResource(req);
  }

  private boolean waitForResourceState(LocalizedResource lr,
      ResourceLocalizationService service, LocalResourceRequest req,
      LocalResourceVisibility vis, String user, ApplicationId appId,
      ResourceState resourceState, long maxWaitTime) {
    LocalResourcesTracker tracker = null;
    // checking tracker is created
    do {
      if (tracker == null) {
        tracker = service.getLocalResourcesTracker(vis, user, appId);
      }
      if (tracker != null && lr == null) {
        lr = tracker.getLocalizedResource(req);
      }
      if (lr != null) {
        break;
      } else {
        try {
          maxWaitTime -= 20;
          Thread.sleep(20);
        } catch (InterruptedException e) {
        }
      }
    } while (maxWaitTime > 0);
    // this will wait till resource state is changed to (resourceState).
    if (lr == null) {
      return false;
    }
    do {
      if (!lr.getState().equals(resourceState)) {
        try {
          maxWaitTime -= 50;
          Thread.sleep(50);
        } catch (InterruptedException e) {
        }
      } else {
        break;
      }
    } while (maxWaitTime > 0);
    return lr.getState().equals(resourceState);
  }

  private ContainerLocalizationRequestEvent createContainerLocalizationEvent(
      ContainerImpl container, LocalResourceVisibility vis,
      LocalResourceRequest req) {
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> reqs =
        new HashMap<LocalResourceVisibility, Collection<LocalResourceRequest>>();
    List<LocalResourceRequest> resourceList =
        new ArrayList<LocalResourceRequest>();
    resourceList.add(req);
    reqs.put(vis, resourceList);
    return new ContainerLocalizationRequestEvent(container, reqs);
  }

  private ContainerImpl createMockContainer(String user, int containerId) {
    ContainerImpl container = mock(ContainerImpl.class);
    when(container.getContainerId()).thenReturn(
      BuilderUtils.newContainerId(1, 1, 1, containerId));
    when(container.getUser()).thenReturn(user);
    Credentials mockCredentials = mock(Credentials.class);
    when(container.getCredentials()).thenReturn(mockCredentials);
    return container;
  }

  private static URL getPath(String path) {
    URL url = BuilderUtils.newURL("file", null, 0, path);
    return url;
  }

  private static LocalResource getMockedResource(Random r, 
      LocalResourceVisibility vis) {
    String name = Long.toHexString(r.nextLong());
    URL url = getPath("/local/PRIVATE/" + name);
    LocalResource rsrc =
        BuilderUtils.newLocalResource(url, LocalResourceType.FILE, vis,
            r.nextInt(1024) + 1024L, r.nextInt(1024) + 2048L);
    return rsrc;
  }
  
  private static LocalResource getAppMockedResource(Random r) {
    return getMockedResource(r, LocalResourceVisibility.APPLICATION);
  }
  
  private static LocalResource getPublicMockedResource(Random r) {
    return getMockedResource(r, LocalResourceVisibility.PUBLIC);
  }
  
  private static LocalResource getPrivateMockedResource(Random r) {
    return getMockedResource(r, LocalResourceVisibility.PRIVATE);
  }

  private static Container getMockContainer(ApplicationId appId, int id) {
    Container c = mock(Container.class);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    ContainerId cId = BuilderUtils.newContainerId(appAttemptId, id);
    when(c.getUser()).thenReturn("user0");
    when(c.getContainerId()).thenReturn(cId);
    Credentials creds = new Credentials();
    creds.addToken(new Text("tok" + id), getToken(id));
    when(c.getCredentials()).thenReturn(creds);
    when(c.toString()).thenReturn(cId.toString());
    return c;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  static Token<? extends TokenIdentifier> getToken(int id) {
    return new Token(("ident" + id).getBytes(), ("passwd" + id).getBytes(),
        new Text("kind" + id), new Text("service" + id));
  }

}
