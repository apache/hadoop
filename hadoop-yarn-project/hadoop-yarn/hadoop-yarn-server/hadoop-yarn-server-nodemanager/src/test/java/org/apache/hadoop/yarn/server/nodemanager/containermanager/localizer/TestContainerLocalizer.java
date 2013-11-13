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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
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
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestContainerLocalizer {

  static final Log LOG = LogFactory.getLog(TestContainerLocalizer.class);
  static final Path basedir =
      new Path("target", TestContainerLocalizer.class.getName());
  static final FsPermission CACHE_DIR_PERM = new FsPermission((short)0710);

  static final String appUser = "yak";
  static final String appId = "app_RM_0";
  static final String containerId = "container_0";
  static final InetSocketAddress nmAddr =
      new InetSocketAddress("foobar", 8040);

  private AbstractFileSystem spylfs;
  private Random random;
  private List<Path> localDirs;
  private Path tokenPath;
  private LocalizationProtocol nmProxy;

  @Test
  public void testContainerLocalizerMain() throws Exception {
    FileContext fs = FileContext.getLocalFSFileContext();
    spylfs = spy(fs.getDefaultFileSystem());
    ContainerLocalizer localizer =
        setupContainerLocalizerForTest();

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
    assertEquals(0, localizer.runLocalization(nmAddr));
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
  
  @Test
  @SuppressWarnings("unchecked")
  public void testLocalizerTokenIsGettingRemoved() throws Exception {
    FileContext fs = FileContext.getLocalFSFileContext();
    spylfs = spy(fs.getDefaultFileSystem());
    ContainerLocalizer localizer = setupContainerLocalizerForTest();
    doNothing().when(localizer).localizeFiles(any(LocalizationProtocol.class),
        any(CompletionService.class), any(UserGroupInformation.class));
    localizer.runLocalization(nmAddr);
    verify(spylfs, times(1)).delete(tokenPath, false);
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generics
  public void testContainerLocalizerClosesFilesystems() throws Exception {
    // verify filesystems are closed when localizer doesn't fail
    FileContext fs = FileContext.getLocalFSFileContext();
    spylfs = spy(fs.getDefaultFileSystem());
    ContainerLocalizer localizer = setupContainerLocalizerForTest();
    doNothing().when(localizer).localizeFiles(any(LocalizationProtocol.class),
        any(CompletionService.class), any(UserGroupInformation.class));
    verify(localizer, never()).closeFileSystems(
        any(UserGroupInformation.class));
    localizer.runLocalization(nmAddr);
    verify(localizer).closeFileSystems(any(UserGroupInformation.class));

    spylfs = spy(fs.getDefaultFileSystem());
    // verify filesystems are closed when localizer fails
    localizer = setupContainerLocalizerForTest();
    doThrow(new YarnRuntimeException("Forced Failure")).when(localizer).localizeFiles(
        any(LocalizationProtocol.class), any(CompletionService.class),
        any(UserGroupInformation.class));
    verify(localizer, never()).closeFileSystems(
        any(UserGroupInformation.class));
    localizer.runLocalization(nmAddr);
    verify(localizer).closeFileSystems(any(UserGroupInformation.class));
  }

  @SuppressWarnings("unchecked") // mocked generics
  private ContainerLocalizer setupContainerLocalizerForTest()
      throws Exception {
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
    ContainerLocalizer concreteLoc = new ContainerLocalizer(lfs, appUser,
        appId, containerId, localDirs, mockRF);
    ContainerLocalizer localizer = spy(concreteLoc);

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

    return localizer;
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
      thenReturn(ConverterUtils.getYarnUrlFromPath(p));
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

}
