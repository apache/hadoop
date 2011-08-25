package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.net.InetSocketAddress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.apache.avro.ipc.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import org.junit.Test;
import static org.junit.Assert.*;

import org.mockito.ArgumentMatcher;
import static org.mockito.Mockito.*;

import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOCAL_DIR;

public class TestResourceLocalizationService {

  static final Path basedir =
      new Path("target", TestResourceLocalizationService.class.getName());

  @Test
  public void testLocalizationInit() throws Exception {
    final Configuration conf = new Configuration();
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(null);

    ContainerExecutor exec = mock(ContainerExecutor.class);
    DeletionService delService = spy(new DeletionService(exec));
    delService.init(null);
    delService.start();

    AbstractFileSystem spylfs =
      spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    FileContext lfs = FileContext.getFileContext(spylfs, conf);
    doNothing().when(spylfs).mkdir(
        isA(Path.class), isA(FsPermission.class), anyBoolean());

    ResourceLocalizationService locService =
      spy(new ResourceLocalizationService(dispatcher, exec, delService));
    doReturn(lfs)
      .when(locService).getLocalFileContext(isA(Configuration.class));
    try {
      dispatcher.start();
      List<Path> localDirs = new ArrayList<Path>();
      String[] sDirs = new String[4];
      for (int i = 0; i < 4; ++i) {
        localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
        sDirs[i] = localDirs.get(i).toString();
      }
      conf.setStrings(NM_LOCAL_DIR, sDirs);

      // initialize ResourceLocalizationService
      locService.init(conf);

      // verify directory creation
      for (Path p : localDirs) {
        Path usercache = new Path(p, ContainerLocalizer.USERCACHE);
        verify(spylfs)
          .mkdir(eq(usercache), isA(FsPermission.class), eq(true));
        Path publicCache = new Path(p, ContainerLocalizer.FILECACHE);
        verify(spylfs)
          .mkdir(eq(publicCache), isA(FsPermission.class), eq(true));
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
  public void testLocalizationHeartbeat() throws Exception {
    Configuration conf = new Configuration();
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
    conf.setStrings(NM_LOCAL_DIR, sDirs);

    Server ignore = mock(Server.class);
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    EventHandler<ApplicationEvent> applicationBus = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, applicationBus);
    EventHandler<ContainerEvent> containerBus = mock(EventHandler.class);
    dispatcher.register(ContainerEventType.class, containerBus);

    ContainerExecutor exec = mock(ContainerExecutor.class);
    DeletionService delService = new DeletionService(exec);
    delService.init(null);
    delService.start();

    ResourceLocalizationService rawService =
      new ResourceLocalizationService(dispatcher, exec, delService);
    ResourceLocalizationService spyService = spy(rawService);
    doReturn(ignore).when(spyService).createServer();
    doReturn(lfs).when(spyService).getLocalFileContext(isA(Configuration.class));
    try {
      spyService.init(conf);
      spyService.start();

      // init application
      final Application app = mock(Application.class);
      final ApplicationId appId = mock(ApplicationId.class);
      when(appId.getClusterTimestamp()).thenReturn(314159265358979L);
      when(appId.getId()).thenReturn(3);
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
          anyLong(), isA(Progressable.class), anyInt(), anyBoolean());
      final LocalResource resource = getMockResource(r);
      final LocalResourceRequest req = new LocalResourceRequest(resource);
      spyService.handle(new ContainerLocalizationRequestEvent(
                c, Collections.singletonList(req),
                LocalResourceVisibility.PRIVATE));
      // Sigh. Thread init of private localizer not accessible
      Thread.sleep(500);
      dispatcher.await();
      String appStr = ConverterUtils.toString(appId);
      String ctnrStr = c.getContainerID().toString();
      verify(exec).startLocalizer(isA(Path.class), isA(InetSocketAddress.class),
            eq("user0"), eq(appStr), eq(ctnrStr), isA(List.class));

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
    } finally {
      delService.stop();
      dispatcher.stop();
      spyService.stop();
    }
  }

  static URL getPath(String path) {
    URL uri = mock(org.apache.hadoop.yarn.api.records.URL.class);
    when(uri.getScheme()).thenReturn("file");
    when(uri.getHost()).thenReturn(null);
    when(uri.getFile()).thenReturn(path);
    return uri;
  }

  static LocalResource getMockResource(Random r) {
    LocalResource rsrc = mock(LocalResource.class);

    String name = Long.toHexString(r.nextLong());
    URL uri = getPath("/local/PRIVATE/" + name);

    when(rsrc.getResource()).thenReturn(uri);
    when(rsrc.getSize()).thenReturn(r.nextInt(1024) + 1024L);
    when(rsrc.getTimestamp()).thenReturn(r.nextInt(1024) + 2048L);
    when(rsrc.getType()).thenReturn(LocalResourceType.FILE);
    when(rsrc.getVisibility()).thenReturn(LocalResourceVisibility.PRIVATE);
    return rsrc;
  }

  static Container getMockContainer(ApplicationId appId, int id) {
    Container c = mock(Container.class);
    ApplicationAttemptId appAttemptId = Records.newRecord(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(1);
    ContainerId cId = Records.newRecord(ContainerId.class);
    cId.setAppAttemptId(appAttemptId);
    cId.setAppId(appId);
    cId.setId(id);
    when(c.getUser()).thenReturn("user0");
    when(c.getContainerID()).thenReturn(cId);
    Credentials creds = new Credentials();
    creds.addToken(new Text("tok" + id), getToken(id));
    when(c.getCredentials()).thenReturn(creds);
    return c;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  static Token<? extends TokenIdentifier> getToken(int id) {
    return new Token(("ident" + id).getBytes(), ("passwd" + id).getBytes(),
        new Text("kind" + id), new Text("service" + id));
  }

}
