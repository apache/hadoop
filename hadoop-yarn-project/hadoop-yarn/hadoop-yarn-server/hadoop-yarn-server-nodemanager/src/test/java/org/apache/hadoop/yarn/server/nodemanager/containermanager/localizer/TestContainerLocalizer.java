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

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Supplier;

public class TestContainerLocalizer {

  static final Logger LOG =
       LoggerFactory.getLogger(TestContainerLocalizer.class);
  static final Path basedir =
      new Path("target", TestContainerLocalizer.class.getName());
  static final FsPermission CACHE_DIR_PERM = new FsPermission((short)0710);
  static final FsPermission USERCACHE_DIR_PERM = new FsPermission((short) 0755);

  static final String appUser = "yak";
  static final String appId = "app_RM_0";
  static final String containerId = "container_0";
  static final InetSocketAddress nmAddr =
      new InetSocketAddress("foobar", 8040);

  @After
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(new File(basedir.toUri().getRawPath()));
  }

  @Test
  public void testMain() throws Exception {
    ContainerLocalizerWrapper wrapper = new ContainerLocalizerWrapper();
    ContainerLocalizer localizer =
        wrapper.setupContainerLocalizerForTest();
    Random random = wrapper.random;
    List<Path> localDirs = wrapper.localDirs;
    Path tokenPath = wrapper.tokenPath;
    LocalizationProtocol nmProxy = wrapper.nmProxy;
    AbstractFileSystem spylfs = wrapper.spylfs;
    mockOutDownloads(localizer);

    // verify created cache
    List<Path> privCacheList = new ArrayList<Path>();
    List<Path> appCacheList = new ArrayList<Path>();
    for (Path p : localDirs) {
      Path base = new Path(new Path(p, ContainerLocalizer.USERCACHE), appUser);
      Path privcache = new Path(base, ContainerLocalizer.FILECACHE);
      privCacheList.add(privcache);
      Path appDir =
          new Path(base, new Path(ContainerLocalizer.APPCACHE, appId));
      Path appcache = new Path(appDir, ContainerLocalizer.FILECACHE);
      appCacheList.add(appcache);
    }

    // mock heartbeat responses from NM
    ResourceLocalizationSpec rsrcA =
        getMockRsrc(random, LocalResourceVisibility.PRIVATE,
          privCacheList.get(0));
    ResourceLocalizationSpec rsrcB =
        getMockRsrc(random, LocalResourceVisibility.PRIVATE,
          privCacheList.get(0));
    ResourceLocalizationSpec rsrcC =
        getMockRsrc(random, LocalResourceVisibility.APPLICATION,
          appCacheList.get(0));
    ResourceLocalizationSpec rsrcD =
        getMockRsrc(random, LocalResourceVisibility.PRIVATE,
          privCacheList.get(0));

    when(nmProxy.heartbeat(isA(LocalizerStatus.class)))
      .thenReturn(new MockLocalizerHeartbeatResponse(LocalizerAction.LIVE,
            Collections.singletonList(rsrcA)))
      .thenReturn(new MockLocalizerHeartbeatResponse(LocalizerAction.LIVE,
            Collections.singletonList(rsrcB)))
      .thenReturn(new MockLocalizerHeartbeatResponse(LocalizerAction.LIVE,
            Collections.singletonList(rsrcC)))
      .thenReturn(new MockLocalizerHeartbeatResponse(LocalizerAction.LIVE,
            Collections.singletonList(rsrcD)))
      .thenReturn(new MockLocalizerHeartbeatResponse(LocalizerAction.LIVE,
            Collections.<ResourceLocalizationSpec>emptyList()))
      .thenReturn(new MockLocalizerHeartbeatResponse(LocalizerAction.DIE,
            null));

    LocalResource tRsrcA = rsrcA.getResource();
    LocalResource tRsrcB = rsrcB.getResource();
    LocalResource tRsrcC = rsrcC.getResource();
    LocalResource tRsrcD = rsrcD.getResource();
    doReturn(
      new FakeDownload(rsrcA.getResource().getResource().getFile(), true))
      .when(localizer).download(isA(Path.class), eq(tRsrcA),
        isA(UserGroupInformation.class));
    doReturn(
      new FakeDownload(rsrcB.getResource().getResource().getFile(), true))
      .when(localizer).download(isA(Path.class), eq(tRsrcB),
        isA(UserGroupInformation.class));
    doReturn(
      new FakeDownload(rsrcC.getResource().getResource().getFile(), true))
      .when(localizer).download(isA(Path.class), eq(tRsrcC),
        isA(UserGroupInformation.class));
    doReturn(
      new FakeDownload(rsrcD.getResource().getResource().getFile(), true))
      .when(localizer).download(isA(Path.class), eq(tRsrcD),
        isA(UserGroupInformation.class));

    // run localization
    localizer.runLocalization(nmAddr);
    for (Path p : localDirs) {
      Path base = new Path(new Path(p, ContainerLocalizer.USERCACHE), appUser);
      Path privcache = new Path(base, ContainerLocalizer.FILECACHE);
      // $x/usercache/$user/filecache
      verify(spylfs).mkdir(eq(privcache), eq(CACHE_DIR_PERM), eq(false));
      Path appDir =
        new Path(base, new Path(ContainerLocalizer.APPCACHE, appId));
      // $x/usercache/$user/appcache/$appId/filecache
      Path appcache = new Path(appDir, ContainerLocalizer.FILECACHE);
      verify(spylfs).mkdir(eq(appcache), eq(CACHE_DIR_PERM), eq(false));
    }
    // verify tokens read at expected location
    verify(spylfs).open(tokenPath);

    // verify downloaded resources reported to NM
    verify(nmProxy).heartbeat(argThat(new HBMatches(rsrcA.getResource())));
    verify(nmProxy).heartbeat(argThat(new HBMatches(rsrcB.getResource())));
    verify(nmProxy).heartbeat(argThat(new HBMatches(rsrcC.getResource())));
    verify(nmProxy).heartbeat(argThat(new HBMatches(rsrcD.getResource())));

    // verify all HB use localizerID provided
    verify(nmProxy, never()).heartbeat(argThat(
        new ArgumentMatcher<LocalizerStatus>() {
          @Override
          public boolean matches(Object o) {
            LocalizerStatus status = (LocalizerStatus) o;
            return !containerId.equals(status.getLocalizerId());
          }
        }));
  }

  @Test(timeout = 15000)
  public void testMainFailure() throws Exception {
    ContainerLocalizerWrapper wrapper = new ContainerLocalizerWrapper();
    ContainerLocalizer localizer = wrapper.setupContainerLocalizerForTest();
    LocalizationProtocol nmProxy = wrapper.nmProxy;
    mockOutDownloads(localizer);

    // Assume the NM heartbeat fails say because of absent tokens.
    when(nmProxy.heartbeat(isA(LocalizerStatus.class))).thenThrow(
        new YarnException("Sigh, no token!"));

    // run localization, it should fail
    try {
      localizer.runLocalization(nmAddr);
      Assert.fail("Localization succeeded unexpectedly!");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Sigh, no token!"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLocalizerTokenIsGettingRemoved() throws Exception {
    ContainerLocalizerWrapper wrapper = new ContainerLocalizerWrapper();
    ContainerLocalizer localizer = wrapper.setupContainerLocalizerForTest();
    Path tokenPath = wrapper.tokenPath;
    AbstractFileSystem spylfs = wrapper.spylfs;
    mockOutDownloads(localizer);
    doNothing().when(localizer).localizeFiles(any(LocalizationProtocol.class),
        any(CompletionService.class), any(UserGroupInformation.class));
    localizer.runLocalization(nmAddr);
    verify(spylfs, times(1)).delete(tokenPath, false);
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generics
  public void testContainerLocalizerClosesFilesystems() throws Exception {

    // verify filesystems are closed when localizer doesn't fail
    ContainerLocalizerWrapper wrapper = new ContainerLocalizerWrapper();

    ContainerLocalizer localizer = wrapper.setupContainerLocalizerForTest();
    mockOutDownloads(localizer);
    doNothing().when(localizer).localizeFiles(any(LocalizationProtocol.class),
        any(CompletionService.class), any(UserGroupInformation.class));
    verify(localizer, never()).closeFileSystems(
        any(UserGroupInformation.class));

    localizer.runLocalization(nmAddr);
    verify(localizer).closeFileSystems(any(UserGroupInformation.class));

    // verify filesystems are closed when localizer fails
    localizer = wrapper.setupContainerLocalizerForTest();
    doThrow(new YarnRuntimeException("Forced Failure")).when(localizer).localizeFiles(
        any(LocalizationProtocol.class), any(CompletionService.class),
        any(UserGroupInformation.class));
    verify(localizer, never()).closeFileSystems(
        any(UserGroupInformation.class));
    try {
      localizer.runLocalization(nmAddr);
      Assert.fail("Localization succeeded unexpectedly!");
    } catch (IOException e) {
      verify(localizer).closeFileSystems(any(UserGroupInformation.class));
    }
  }

  @Test
  public void testMultipleLocalizers() throws Exception {
    FakeContainerLocalizerWrapper testA = new FakeContainerLocalizerWrapper();
    FakeContainerLocalizerWrapper testB = new FakeContainerLocalizerWrapper();

    FakeContainerLocalizer localizerA = testA.init();
    FakeContainerLocalizer localizerB = testB.init();

    // run localization
    Thread threadA = new Thread() {
      @Override
      public void run() {
        try {
          localizerA.runLocalization(nmAddr);
        } catch (Exception e) {
          LOG.warn(e.toString());
        }
      }
    };
    Thread threadB = new Thread() {
      @Override
      public void run() {
        try {
          localizerB.runLocalization(nmAddr);
        } catch (Exception e) {
          LOG.warn(e.toString());
        }
      }
    };
    ShellCommandExecutor shexcA = null;
    ShellCommandExecutor shexcB = null;
    try {
      threadA.start();
      threadB.start();

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          FakeContainerLocalizer.FakeLongDownload downloader =
              localizerA.getDownloader();
          return downloader != null && downloader.getShexc() != null &&
              downloader.getShexc().getProcess() != null;
        }
      }, 10, 30000);

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          FakeContainerLocalizer.FakeLongDownload downloader =
              localizerB.getDownloader();
          return downloader != null && downloader.getShexc() != null &&
              downloader.getShexc().getProcess() != null;
        }
      }, 10, 30000);

      shexcA = localizerA.getDownloader().getShexc();
      shexcB = localizerB.getDownloader().getShexc();

      assertTrue("Localizer A process not running, but should be",
          shexcA.getProcess().isAlive());
      assertTrue("Localizer B process not running, but should be",
          shexcB.getProcess().isAlive());

      // Stop heartbeat from giving anymore resources to download
      testA.heartbeatResponse++;
      testB.heartbeatResponse++;

      // Send DIE to localizerA. This should kill its subprocesses
      testA.heartbeatResponse++;

      threadA.join();
      shexcA.getProcess().waitFor(10000, TimeUnit.MILLISECONDS);

      assertFalse("Localizer A process is still running, but shouldn't be",
          shexcA.getProcess().isAlive());
      assertTrue("Localizer B process not running, but should be",
          shexcB.getProcess().isAlive());

    } finally {
      // Make sure everything gets cleaned up
      // Process A should already be dead
      shexcA.getProcess().destroy();
      shexcB.getProcess().destroy();
      shexcA.getProcess().waitFor(10000, TimeUnit.MILLISECONDS);
      shexcB.getProcess().waitFor(10000, TimeUnit.MILLISECONDS);

      threadA.join();
      // Send DIE to localizer B
      testB.heartbeatResponse++;
      threadB.join();
    }
  }



  private void mockOutDownloads(ContainerLocalizer localizer) {
    // return result instantly for deterministic test
    ExecutorService syncExec = mock(ExecutorService.class);
    CompletionService<Path> cs = mock(CompletionService.class);
    when(cs.submit(isA(Callable.class)))
      .thenAnswer(new Answer<Future<Path>>() {
          @Override
          public Future<Path> answer(InvocationOnMock invoc)
              throws Throwable {
            Future<Path> done = mock(Future.class);
            when(done.isDone()).thenReturn(true);
            FakeDownload d = (FakeDownload) invoc.getArguments()[0];
            when(done.get()).thenReturn(d.call());
            return done;
          }
        });
    doReturn(syncExec).when(localizer).createDownloadThreadPool();
    doReturn(cs).when(localizer).createCompletionService(syncExec);
  }

  static class HBMatches extends ArgumentMatcher<LocalizerStatus> {
    final LocalResource rsrc;
    HBMatches(LocalResource rsrc) {
      this.rsrc = rsrc;
    }
    @Override
    public boolean matches(Object o) {
      LocalizerStatus status = (LocalizerStatus) o;
      for (LocalResourceStatus localized : status.getResources()) {
        switch (localized.getStatus()) {
        case FETCH_SUCCESS:
          if (localized.getLocalPath().getFile().contains(
                rsrc.getResource().getFile())) {
            return true;
          }
          break;
        default:
          fail("Unexpected: " + localized.getStatus());
          break;
        }
      }
      return false;
    }
  }

  static class FakeDownload implements Callable<Path> {
    private final Path localPath;
    private final boolean succeed;
    FakeDownload(String absPath, boolean succeed) {
      this.localPath = new Path("file:///localcache" + absPath);
      this.succeed = succeed;
    }
    @Override
    public Path call() throws IOException {
      if (!succeed) {
        throw new IOException("FAIL " + localPath);
      }
      return localPath;
    }
  }

  class FakeContainerLocalizer extends ContainerLocalizer  {
    private FakeLongDownload downloader;

    FakeContainerLocalizer(FileContext lfs, String user, String appId,
        String localizerId, List<Path> localDirs,
        RecordFactory recordFactory) throws IOException {
      super(lfs, user, appId, localizerId, localDirs, recordFactory);
    }

    FakeLongDownload getDownloader() {
      return downloader;
    }

    @Override
    Callable<Path> download(Path path, LocalResource rsrc,
        UserGroupInformation ugi) throws IOException {
      downloader = new FakeLongDownload(Mockito.mock(FileContext.class), ugi,
          new Configuration(), path, rsrc);
      return downloader;
    }

    class FakeLongDownload extends ContainerLocalizer.FSDownloadWrapper {
      private final Path localPath;
      private Shell.ShellCommandExecutor shexc;
      FakeLongDownload(FileContext files, UserGroupInformation ugi,
          Configuration conf, Path destDirPath, LocalResource resource) {
        super(files, ugi, conf, destDirPath, resource);
        this.localPath = new Path("file:///localcache");
      }

      Shell.ShellCommandExecutor getShexc() {
        return shexc;
      }

      @Override
      public Path doDownloadCall() throws IOException {
        String sleepCommand = "sleep 30";
        String[] shellCmd = {"bash", "-c", sleepCommand};
        shexc = new Shell.ShellCommandExecutor(shellCmd);
        shexc.execute();

        return localPath;
      }
    }
  }

  class ContainerLocalizerWrapper {
    AbstractFileSystem spylfs;
    Random random;
    List<Path> localDirs;
    Path tokenPath;
    LocalizationProtocol nmProxy;

    @SuppressWarnings("unchecked") // mocked generics
    FakeContainerLocalizer setupContainerLocalizerForTest()
        throws Exception {

      FileContext fs = FileContext.getLocalFSFileContext();
      spylfs = spy(fs.getDefaultFileSystem());
      // don't actually create dirs
      doNothing().when(spylfs).mkdir(
          isA(Path.class), isA(FsPermission.class), anyBoolean());

      Configuration conf = new Configuration();
      FileContext lfs = FileContext.getFileContext(spylfs, conf);
      localDirs = new ArrayList<Path>();
      for (int i = 0; i < 4; ++i) {
        localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
      }
      RecordFactory mockRF = getMockLocalizerRecordFactory();
      FakeContainerLocalizer concreteLoc = new FakeContainerLocalizer(lfs,
          appUser, appId, containerId, localDirs, mockRF);
      FakeContainerLocalizer localizer = spy(concreteLoc);

      // return credential stream instead of opening local file
      random = new Random();
      long seed = random.nextLong();
      System.out.println("SEED: " + seed);
      random.setSeed(seed);
      DataInputBuffer appTokens = createFakeCredentials(random, 10);
      tokenPath =
        lfs.makeQualified(new Path(
              String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT,
                  containerId)));
      doReturn(new FSDataInputStream(new FakeFSDataInputStream(appTokens))
          ).when(spylfs).open(tokenPath);
      nmProxy = mock(LocalizationProtocol.class);
      doReturn(nmProxy).when(localizer).getProxy(nmAddr);
      doNothing().when(localizer).sleep(anyInt());

      return localizer;
    }

  }

  class FakeContainerLocalizerWrapper extends ContainerLocalizerWrapper{
    private int heartbeatResponse = 0;
    public FakeContainerLocalizer init() throws Exception {
      FileContext fs = FileContext.getLocalFSFileContext();
      FakeContainerLocalizer localizer = setupContainerLocalizerForTest();

      // verify created cache
      List<Path> privCacheList = new ArrayList<Path>();
      for (Path p : localDirs) {
        Path base = new Path(new Path(p, ContainerLocalizer.USERCACHE),
            appUser);
        Path privcache = new Path(base, ContainerLocalizer.FILECACHE);
        privCacheList.add(privcache);
      }

      ResourceLocalizationSpec rsrc = getMockRsrc(random,
          LocalResourceVisibility.PRIVATE, privCacheList.get(0));

      // mock heartbeat responses from NM
      doAnswer(new Answer<MockLocalizerHeartbeatResponse>() {
        @Override
        public MockLocalizerHeartbeatResponse answer(
            InvocationOnMock invocationOnMock) throws Throwable {
          if(heartbeatResponse == 0) {
            return new MockLocalizerHeartbeatResponse(LocalizerAction.LIVE,
                Collections.singletonList(rsrc));
          } else if (heartbeatResponse < 2) {
            return new MockLocalizerHeartbeatResponse(LocalizerAction.LIVE,
                Collections.<ResourceLocalizationSpec>emptyList());
          } else {
            return new MockLocalizerHeartbeatResponse(LocalizerAction.DIE,
                null);
          }
        }
      }).when(nmProxy).heartbeat(isA(LocalizerStatus.class));

      return localizer;
    }
  }

  static RecordFactory getMockLocalizerRecordFactory() {
    RecordFactory mockRF = mock(RecordFactory.class);
    when(mockRF.newRecordInstance(same(LocalResourceStatus.class)))
      .thenAnswer(new Answer<LocalResourceStatus>() {
          @Override
          public LocalResourceStatus answer(InvocationOnMock invoc)
              throws Throwable {
            return new MockLocalResourceStatus();
          }
        });
    when(mockRF.newRecordInstance(same(LocalizerStatus.class)))
      .thenAnswer(new Answer<LocalizerStatus>() {
          @Override
          public LocalizerStatus answer(InvocationOnMock invoc)
              throws Throwable {
            return new MockLocalizerStatus();
          }
        });
    return mockRF;
  }

  static ResourceLocalizationSpec getMockRsrc(Random r,
      LocalResourceVisibility vis, Path p) {
    ResourceLocalizationSpec resourceLocalizationSpec =
      mock(ResourceLocalizationSpec.class);

    LocalResource rsrc = mock(LocalResource.class);
    String name = Long.toHexString(r.nextLong());
    URL uri = mock(org.apache.hadoop.yarn.api.records.URL.class);
    when(uri.getScheme()).thenReturn("file");
    when(uri.getHost()).thenReturn(null);
    when(uri.getFile()).thenReturn("/local/" + vis + "/" + name);

    when(rsrc.getResource()).thenReturn(uri);
    when(rsrc.getSize()).thenReturn(r.nextInt(1024) + 1024L);
    when(rsrc.getTimestamp()).thenReturn(r.nextInt(1024) + 2048L);
    when(rsrc.getType()).thenReturn(LocalResourceType.FILE);
    when(rsrc.getVisibility()).thenReturn(vis);

    when(resourceLocalizationSpec.getResource()).thenReturn(rsrc);
    when(resourceLocalizationSpec.getDestinationDirectory()).
      thenReturn(URL.fromPath(p));
    return resourceLocalizationSpec;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
static DataInputBuffer createFakeCredentials(Random r, int nTok)
      throws IOException {
    Credentials creds = new Credentials();
    byte[] password = new byte[20];
    Text kind = new Text();
    Text service = new Text();
    Text alias = new Text();
    for (int i = 0; i < nTok; ++i) {
      byte[] identifier = ("idef" + i).getBytes();
      r.nextBytes(password);
      kind.set("kind" + i);
      service.set("service" + i);
      alias.set("token" + i);
      Token token = new Token(identifier, password, kind, service);
      creds.addToken(alias, token);
    }
    DataOutputBuffer buf = new DataOutputBuffer();
    creds.writeTokenStorageToStream(buf);
    DataInputBuffer ret = new DataInputBuffer();
    ret.reset(buf.getData(), 0, buf.getLength());
    return ret;
  }

  @Test(timeout = 10000)
  public void testUserCacheDirPermission() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    FileContext lfs = FileContext.getLocalFSFileContext(conf);
    Path fileCacheDir = lfs.makeQualified(new Path(basedir, "filecache"));
    lfs.mkdir(fileCacheDir, FsPermission.getDefault(), true);
    RecordFactory recordFactory = mock(RecordFactory.class);
    ContainerLocalizer localizer = new ContainerLocalizer(lfs,
        UserGroupInformation.getCurrentUser().getUserName(), "application_01",
        "container_01", new ArrayList<Path>(), recordFactory);
    LocalResource rsrc = mock(LocalResource.class);
    when(rsrc.getVisibility()).thenReturn(LocalResourceVisibility.PRIVATE);
    Path destDirPath = new Path(fileCacheDir, "0/0/85");
    //create one of the parent directories with the wrong permissions first
    FsPermission wrongPerm = new FsPermission((short) 0700);
    lfs.mkdir(destDirPath.getParent().getParent(), wrongPerm, false);
    lfs.mkdir(destDirPath.getParent(), wrongPerm, false);
    //Localize and check the directory permission are correct.
    localizer
        .download(destDirPath, rsrc, UserGroupInformation.getCurrentUser());
    Assert
        .assertEquals("Cache directory permissions filecache/0/0 is incorrect",
            USERCACHE_DIR_PERM,
            lfs.getFileStatus(destDirPath.getParent()).getPermission());
    Assert.assertEquals("Cache directory permissions filecache/0 is incorrect",
        USERCACHE_DIR_PERM,
        lfs.getFileStatus(destDirPath.getParent().getParent()).getPermission());
  }

}
