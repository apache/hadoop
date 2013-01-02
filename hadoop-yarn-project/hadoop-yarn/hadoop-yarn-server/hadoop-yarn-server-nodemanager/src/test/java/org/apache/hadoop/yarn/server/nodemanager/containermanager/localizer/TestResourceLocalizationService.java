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
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.ResourceStatusType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService.LocalizerTracker;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

public class TestResourceLocalizationService {

  static final Path basedir =
      new Path("target", TestResourceLocalizationService.class.getName());
  static Server mockServer;
  
  @BeforeClass
  public static void setup() {
    mockServer = mock(Server.class);
    doReturn(new InetSocketAddress(123)).when(mockServer).getListenerAddress();
  }
  
  @Test
  public void testLocalizationInit() throws Exception {
    final Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(new Configuration());

    ContainerExecutor exec = mock(ContainerExecutor.class);
    DeletionService delService = spy(new DeletionService(exec));
    delService.init(new Configuration());
    delService.start();

    AbstractFileSystem spylfs =
      spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    FileContext lfs = FileContext.getFileContext(spylfs, conf);
    doNothing().when(spylfs).mkdir(
        isA(Path.class), isA(FsPermission.class), anyBoolean());

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
    Configuration conf = new YarnConfiguration();
    AbstractFileSystem spylfs =
      spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    final FileContext lfs = FileContext.getFileContext(spylfs, conf);
    doNothing().when(spylfs).mkdir(
        isA(Path.class), isA(FsPermission.class), anyBoolean());

    List<Path> localDirs = new ArrayList<Path>();
    String[] sDirs = new String[4];
    for (int i = 0; i < 4; ++i) {
      localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
      sDirs[i] = localDirs.get(i).toString();
    }
    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);
    String logDir = lfs.makeQualified(new Path(basedir, "logdir " )).toString();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir);
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
    delService.init(null);
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
  
  @Test
  @SuppressWarnings("unchecked") // mocked generics
  public void testLocalizationHeartbeat() throws Exception {
    Configuration conf = new YarnConfiguration();
    AbstractFileSystem spylfs =
      spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    final FileContext lfs = FileContext.getFileContext(spylfs, conf);
    doNothing().when(spylfs).mkdir(
        isA(Path.class), isA(FsPermission.class), anyBoolean());

    List<Path> localDirs = new ArrayList<Path>();
    String[] sDirs = new String[4];
    for (int i = 0; i < 4; ++i) {
      localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
      sDirs[i] = localDirs.get(i).toString();
    }
    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS, sDirs);
    String logDir = lfs.makeQualified(new Path(basedir, "logdir " )).toString();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir);
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
      final LocalResource resource = getPrivateMockedResource(r);
      final LocalResourceRequest req = new LocalResourceRequest(resource);
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs =
        new HashMap<LocalResourceVisibility, 
                    Collection<LocalResourceRequest>>();
      rsrcs.put(LocalResourceVisibility.PRIVATE, Collections.singletonList(req));
      spyService.handle(new ContainerLocalizationRequestEvent(c, rsrcs));
      // Sigh. Thread init of private localizer not accessible
      Thread.sleep(1000);
      dispatcher.await();
      String appStr = ConverterUtils.toString(appId);
      String ctnrStr = c.getContainerID().toString();
      ArgumentCaptor<Path> tokenPathCaptor = ArgumentCaptor.forClass(Path.class);
      verify(exec).startLocalizer(tokenPathCaptor.capture(),
          isA(InetSocketAddress.class), eq("user0"), eq(appStr), eq(ctnrStr),
          isA(List.class), isA(List.class));
      Path localizationTokenPath = tokenPathCaptor.getValue();

      // heartbeat from localizer
      LocalResourceStatus rsrcStat = mock(LocalResourceStatus.class);
      LocalizerStatus stat = mock(LocalizerStatus.class);
      when(stat.getLocalizerId()).thenReturn(ctnrStr);
      when(rsrcStat.getResource()).thenReturn(resource);
      when(rsrcStat.getLocalSize()).thenReturn(4344L);
      URL locPath = getPath("/cache/private/blah");
      when(rsrcStat.getLocalPath()).thenReturn(locPath);
      when(rsrcStat.getStatus()).thenReturn(ResourceStatusType.FETCH_SUCCESS);
      when(stat.getResources())
        .thenReturn(Collections.<LocalResourceStatus>emptyList())
        .thenReturn(Collections.singletonList(rsrcStat))
        .thenReturn(Collections.<LocalResourceStatus>emptyList());

      // get rsrc
      LocalizerHeartbeatResponse response = spyService.heartbeat(stat);
      assertEquals(LocalizerAction.LIVE, response.getLocalizerAction());
      assertEquals(req, new LocalResourceRequest(response.getLocalResource(0)));

      // empty rsrc
      response = spyService.heartbeat(stat);
      assertEquals(LocalizerAction.LIVE, response.getLocalizerAction());
      assertEquals(0, response.getAllResources().size());

      // get shutdown
      response = spyService.heartbeat(stat);
      assertEquals(LocalizerAction.DIE, response.getLocalizerAction());

      // verify container notification
      ArgumentMatcher<ContainerEvent> matchesContainerLoc =
        new ArgumentMatcher<ContainerEvent>() {
          @Override
          public boolean matches(Object o) {
            ContainerEvent evt = (ContainerEvent) o;
            return evt.getType() == ContainerEventType.RESOURCE_LOCALIZED
              && c.getContainerID() == evt.getContainerID();
          }
        };
      dispatcher.await();
      verify(containerBus).handle(argThat(matchesContainerLoc));
      
      // Verify deletion of localization token.
      verify(delService).delete((String)isNull(), eq(localizationTokenPath));
    } finally {
      spyService.stop();
      dispatcher.stop();
      delService.stop();
    }
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
    when(c.getContainerID()).thenReturn(cId);
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
