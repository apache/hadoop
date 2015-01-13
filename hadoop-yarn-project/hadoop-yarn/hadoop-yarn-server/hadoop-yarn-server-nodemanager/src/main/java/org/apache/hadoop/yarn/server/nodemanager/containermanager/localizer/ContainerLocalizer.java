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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.ResourceStatusType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.FSDownload;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ContainerLocalizer {

  static final Log LOG = LogFactory.getLog(ContainerLocalizer.class);

  public static final String FILECACHE = "filecache";
  public static final String APPCACHE = "appcache";
  public static final String USERCACHE = "usercache";
  public static final String OUTPUTDIR = "output";
  public static final String TOKEN_FILE_NAME_FMT = "%s.tokens";
  public static final String WORKDIR = "work";
  private static final String APPCACHE_CTXT_FMT = "%s.app.cache.dirs";
  private static final String USERCACHE_CTXT_FMT = "%s.user.cache.dirs";
  private static final FsPermission FILECACHE_PERMS =
      new FsPermission((short)0710);

  private final String user;
  private final String appId;
  private final List<Path> localDirs;
  private final String localizerId;
  private final FileContext lfs;
  private final Configuration conf;
  private final RecordFactory recordFactory;
  private final Map<LocalResource,Future<Path>> pendingResources;
  private final String appCacheDirContextName;

  public ContainerLocalizer(FileContext lfs, String user, String appId,
      String localizerId, List<Path> localDirs,
      RecordFactory recordFactory) throws IOException {
    if (null == user) {
      throw new IOException("Cannot initialize for null user");
    }
    if (null == localizerId) {
      throw new IOException("Cannot initialize for null containerId");
    }
    this.lfs = lfs;
    this.user = user;
    this.appId = appId;
    this.localDirs = localDirs;
    this.localizerId = localizerId;
    this.recordFactory = recordFactory;
    this.conf = new Configuration();
    this.appCacheDirContextName = String.format(APPCACHE_CTXT_FMT, appId);
    this.pendingResources = new HashMap<LocalResource,Future<Path>>();
  }

  LocalizationProtocol getProxy(final InetSocketAddress nmAddr) {
    YarnRPC rpc = YarnRPC.create(conf);
    return (LocalizationProtocol)
      rpc.getProxy(LocalizationProtocol.class, nmAddr, conf);
  }

  @SuppressWarnings("deprecation")
  public int runLocalization(final InetSocketAddress nmAddr)
      throws IOException, InterruptedException {
    // load credentials
    initDirs(conf, user, appId, lfs, localDirs);
    final Credentials creds = new Credentials();
    DataInputStream credFile = null;
    try {
      // assume credentials in cwd
      // TODO: Fix
      Path tokenPath =
          new Path(String.format(TOKEN_FILE_NAME_FMT, localizerId));
      credFile = lfs.open(tokenPath);
      creds.readTokenStorageStream(credFile);
      // Explicitly deleting token file.
      lfs.delete(tokenPath, false);      
    } finally  {
      if (credFile != null) {
        credFile.close();
      }
    }
    // create localizer context
    UserGroupInformation remoteUser =
      UserGroupInformation.createRemoteUser(user);
    remoteUser.addToken(creds.getToken(LocalizerTokenIdentifier.KIND));
    final LocalizationProtocol nodeManager =
        remoteUser.doAs(new PrivilegedAction<LocalizationProtocol>() {
          @Override
          public LocalizationProtocol run() {
            return getProxy(nmAddr);
          }
        });

    // create user context
    UserGroupInformation ugi =
      UserGroupInformation.createRemoteUser(user);
    for (Token<? extends TokenIdentifier> token : creds.getAllTokens()) {
      ugi.addToken(token);
    }

    ExecutorService exec = null;
    try {
      exec = createDownloadThreadPool();
      CompletionService<Path> ecs = createCompletionService(exec);
      localizeFiles(nodeManager, ecs, ugi);
      return 0;
    } catch (Throwable e) {
      // Print traces to stdout so that they can be logged by the NM address
      // space.
      e.printStackTrace(System.out);
      return -1;
    } finally {
      try {
        if (exec != null) {
          exec.shutdownNow();
        }
        LocalDirAllocator.removeContext(appCacheDirContextName);
      } finally {
        closeFileSystems(ugi);
      }
    }
  }

  ExecutorService createDownloadThreadPool() {
    return Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setNameFormat("ContainerLocalizer Downloader").build());
  }

  CompletionService<Path> createCompletionService(ExecutorService exec) {
    return new ExecutorCompletionService<Path>(exec);
  }

  Callable<Path> download(Path path, LocalResource rsrc,
      UserGroupInformation ugi) throws IOException {
    DiskChecker.checkDir(new File(path.toUri().getRawPath()));
    return new FSDownload(lfs, ugi, conf, path, rsrc);
  }

  static long getEstimatedSize(LocalResource rsrc) {
    if (rsrc.getSize() < 0) {
      return -1;
    }
    switch (rsrc.getType()) {
      case ARCHIVE:
      case PATTERN:
        return 5 * rsrc.getSize();
      case FILE:
      default:
        return rsrc.getSize();
    }
  }

  void sleep(int duration) throws InterruptedException {
    TimeUnit.SECONDS.sleep(duration);
  }

  protected void closeFileSystems(UserGroupInformation ugi) {
    try {
      FileSystem.closeAllForUGI(ugi);
    } catch (IOException e) {
      LOG.warn("Failed to close filesystems: ", e);
    }
  }

  protected void localizeFiles(LocalizationProtocol nodemanager,
      CompletionService<Path> cs, UserGroupInformation ugi)
      throws IOException {
    while (true) {
      try {
        LocalizerStatus status = createStatus();
        LocalizerHeartbeatResponse response = nodemanager.heartbeat(status);
        switch (response.getLocalizerAction()) {
        case LIVE:
          List<ResourceLocalizationSpec> newRsrcs = response.getResourceSpecs();
          for (ResourceLocalizationSpec newRsrc : newRsrcs) {
            if (!pendingResources.containsKey(newRsrc.getResource())) {
              pendingResources.put(newRsrc.getResource(), cs.submit(download(
                new Path(newRsrc.getDestinationDirectory().getFile()),
                newRsrc.getResource(), ugi)));
            }
          }
          break;
        case DIE:
          // killall running localizations
          for (Future<Path> pending : pendingResources.values()) {
            pending.cancel(true);
          }
          status = createStatus();
          // ignore response
          try {
            nodemanager.heartbeat(status);
          } catch (YarnException e) { }
          return;
        }
        cs.poll(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        return;
      } catch (YarnException e) {
        // TODO cleanup
        return;
      }
    }
  }

  /**
   * Create the payload for the HeartBeat. Mainly the list of
   * {@link LocalResourceStatus}es
   * 
   * @return a {@link LocalizerStatus} that can be sent via heartbeat.
   * @throws InterruptedException
   */
  private LocalizerStatus createStatus() throws InterruptedException {
    final List<LocalResourceStatus> currentResources =
      new ArrayList<LocalResourceStatus>();
    // TODO: Synchronization??
    for (Iterator<LocalResource> i = pendingResources.keySet().iterator();
         i.hasNext();) {
      LocalResource rsrc = i.next();
      LocalResourceStatus stat =
        recordFactory.newRecordInstance(LocalResourceStatus.class);
      stat.setResource(rsrc);
      Future<Path> fPath = pendingResources.get(rsrc);
      if (fPath.isDone()) {
        try {
          Path localPath = fPath.get();
          stat.setLocalPath(
              ConverterUtils.getYarnUrlFromPath(localPath));
          stat.setLocalSize(
              FileUtil.getDU(new File(localPath.getParent().toUri())));
          stat.setStatus(ResourceStatusType.FETCH_SUCCESS);
        } catch (ExecutionException e) {
          stat.setStatus(ResourceStatusType.FETCH_FAILURE);
          stat.setException(SerializedException.newInstance(e.getCause()));
        } catch (CancellationException e) {
          stat.setStatus(ResourceStatusType.FETCH_FAILURE);
          stat.setException(SerializedException.newInstance(e));
        }
        // TODO shouldn't remove until ACK
        i.remove();
      } else {
        stat.setStatus(ResourceStatusType.FETCH_PENDING);
      }
      currentResources.add(stat);
    }
    LocalizerStatus status =
      recordFactory.newRecordInstance(LocalizerStatus.class);
    status.setLocalizerId(localizerId);
    status.addAllResources(currentResources);
    return status;
  }
  
  /**
   * Adds the ContainerLocalizer arguments for a @{link ShellCommandExecutor},
   * as expected by ContainerLocalizer.main
   * @param command the current ShellCommandExecutor command line
   * @param user localization user
   * @param appId localized app id
   * @param locId localizer id
   * @param nmAddr nodemanager address
   * @param localDirs list of local dirs
   */
  public static void buildMainArgs(List<String> command,
      String user, String appId, String locId,
      InetSocketAddress nmAddr, List<String> localDirs) {
    
    command.add(ContainerLocalizer.class.getName());
    command.add(user);
    command.add(appId);
    command.add(locId);
    command.add(nmAddr.getHostName());
    command.add(Integer.toString(nmAddr.getPort()));
    for(String dir : localDirs) {
      command.add(dir);
    }
  }

  public static void main(String[] argv) throws Throwable {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    // usage: $0 user appId locId host port app_log_dir user_dir [user_dir]*
    // let $x = $x/usercache for $local.dir
    // MKDIR $x/$user/appcache/$appid
    // MKDIR $x/$user/appcache/$appid/output
    // MKDIR $x/$user/appcache/$appid/filecache
    // LOAD $x/$user/appcache/$appid/appTokens
    try {
      String user = argv[0];
      String appId = argv[1];
      String locId = argv[2];
      InetSocketAddress nmAddr =
          new InetSocketAddress(argv[3], Integer.parseInt(argv[4]));
      String[] sLocaldirs = Arrays.copyOfRange(argv, 5, argv.length);
      ArrayList<Path> localDirs = new ArrayList<Path>(sLocaldirs.length);
      for (String sLocaldir : sLocaldirs) {
        localDirs.add(new Path(sLocaldir));
      }

      final String uid =
          UserGroupInformation.getCurrentUser().getShortUserName();
      if (!user.equals(uid)) {
        // TODO: fail localization
        LOG.warn("Localization running as " + uid + " not " + user);
      }

      ContainerLocalizer localizer =
          new ContainerLocalizer(FileContext.getLocalFSFileContext(), user,
              appId, locId, localDirs,
              RecordFactoryProvider.getRecordFactory(null));
      int nRet = localizer.runLocalization(nmAddr);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("nRet: %d", nRet));
      }
      System.exit(nRet);
    } catch (Throwable e) {
      // Print error to stdout so that LCE can use it.
      e.printStackTrace(System.out);
      LOG.error("Exception in main:", e);
      throw e;
    }
  }

  private static void initDirs(Configuration conf, String user, String appId,
      FileContext lfs, List<Path> localDirs) throws IOException {
    if (null == localDirs || 0 == localDirs.size()) {
      throw new IOException("Cannot initialize without local dirs");
    }
    String[] appsFileCacheDirs = new String[localDirs.size()];
    String[] usersFileCacheDirs = new String[localDirs.size()];
    for (int i = 0, n = localDirs.size(); i < n; ++i) {
      // $x/usercache/$user
      Path base = lfs.makeQualified(
          new Path(new Path(localDirs.get(i), USERCACHE), user));
      // $x/usercache/$user/filecache
      Path userFileCacheDir = new Path(base, FILECACHE);
      usersFileCacheDirs[i] = userFileCacheDir.toString();
      createDir(lfs, userFileCacheDir, FILECACHE_PERMS, false);
      // $x/usercache/$user/appcache/$appId
      Path appBase = new Path(base, new Path(APPCACHE, appId));
      // $x/usercache/$user/appcache/$appId/filecache
      Path appFileCacheDir = new Path(appBase, FILECACHE);
      appsFileCacheDirs[i] = appFileCacheDir.toString();
      createDir(lfs, appFileCacheDir, FILECACHE_PERMS, false);
    }
    conf.setStrings(String.format(APPCACHE_CTXT_FMT, appId), appsFileCacheDirs);
    conf.setStrings(String.format(USERCACHE_CTXT_FMT, user), usersFileCacheDirs);
  }

  private static void createDir(FileContext lfs, Path dirPath,
      FsPermission perms, boolean createParent) throws IOException {
    lfs.mkdir(dirPath, perms, createParent);
    if (!perms.equals(perms.applyUMask(lfs.getUMask()))) {
      lfs.setPermission(dirPath, perms);
    }
  }
}
