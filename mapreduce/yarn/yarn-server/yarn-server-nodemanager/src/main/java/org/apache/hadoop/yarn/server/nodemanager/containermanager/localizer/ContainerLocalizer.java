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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.ResourceStatusType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerSecurityInfo;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenSecretManager;
import org.apache.hadoop.yarn.util.ConverterUtils;

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

  private final String user;
  private final String appId;
  private final List<Path> localDirs;
  private final String localizerId;
  private final FileContext lfs;
  private final Configuration conf;
  private final LocalDirAllocator appDirs;
  private final LocalDirAllocator userDirs;
  private final RecordFactory recordFactory;
  private final Map<LocalResource,Future<Path>> pendingResources;

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
    this.appDirs =
      new LocalDirAllocator(String.format(APPCACHE_CTXT_FMT, appId));
    this.userDirs =
      new LocalDirAllocator(String.format(USERCACHE_CTXT_FMT, appId));
    this.pendingResources = new HashMap<LocalResource,Future<Path>>();
  }

  LocalizationProtocol getProxy(final InetSocketAddress nmAddr) {
    Configuration localizerConf = new Configuration();
    YarnRPC rpc = YarnRPC.create(localizerConf);
    if (UserGroupInformation.isSecurityEnabled()) {
      localizerConf.setClass(
          CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
          LocalizerSecurityInfo.class, SecurityInfo.class);
    }
    return (LocalizationProtocol)
      rpc.getProxy(LocalizationProtocol.class, nmAddr, localizerConf);
  }

  public int runLocalization(final InetSocketAddress nmAddr)
      throws IOException, InterruptedException {
    // load credentials
    initDirs(conf, user, appId, lfs, localDirs);
    final Credentials creds = new Credentials();
    DataInputStream credFile = null;
    try {
      // assume credentials in cwd
      // TODO: Fix
      credFile = lfs.open(
          new Path(String.format(TOKEN_FILE_NAME_FMT, localizerId)));
      creds.readTokenStorageStream(credFile);
    } finally  {
      if (credFile != null) {
        credFile.close();
      }
    }
    // create localizer context
    UserGroupInformation remoteUser =
      UserGroupInformation.createRemoteUser(user);
    LocalizerTokenSecretManager secretManager =
      new LocalizerTokenSecretManager();
    LocalizerTokenIdentifier id = secretManager.createIdentifier();
    Token<LocalizerTokenIdentifier> localizerToken =
      new Token<LocalizerTokenIdentifier>(id, secretManager);
    remoteUser.addToken(localizerToken);
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
      localizeFiles(nodeManager, exec, ugi);
      return 0;
    } catch (Throwable e) {
      // Print traces to stdout so that they can be logged by the NM address
      // space.
      e.printStackTrace(System.out);
      return -1;
    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
  }

  ExecutorService createDownloadThreadPool() {
    return Executors.newSingleThreadExecutor();
  }

  Callable<Path> download(LocalDirAllocator lda, LocalResource rsrc,
      UserGroupInformation ugi) {
    return new FSDownload(lfs, ugi, conf, lda, rsrc, new Random());
  }

  void sleep(int duration) throws InterruptedException {
    TimeUnit.SECONDS.sleep(duration);
  }

  private void localizeFiles(LocalizationProtocol nodemanager, ExecutorService exec,
      UserGroupInformation ugi) {
    while (true) {
      try {
        LocalizerStatus status = createStatus();
        LocalizerHeartbeatResponse response = nodemanager.heartbeat(status);
        switch (response.getLocalizerAction()) {
        case LIVE:
          List<LocalResource> newResources = response.getAllResources();
          for (LocalResource r : newResources) {
            if (!pendingResources.containsKey(r)) {
              final LocalDirAllocator lda;
              switch (r.getVisibility()) {
              default:
                LOG.warn("Unknown visibility: " + r.getVisibility());
              case PUBLIC:
              case PRIVATE:
                lda = userDirs;
                break;
              case APPLICATION:
                lda = appDirs;
                break;
              }
              // TODO: Synchronization??
              pendingResources.put(r, exec.submit(download(lda, r, ugi)));
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
          } catch (YarnRemoteException e) { }
          return;
        }
        // TODO HB immediately when rsrc localized
        sleep(1);
      } catch (InterruptedException e) {
        return;
      } catch (YarnRemoteException e) {
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
              FileUtil.getDU(new File(localPath.getParent().toString())));
          stat.setStatus(ResourceStatusType.FETCH_SUCCESS);
        } catch (ExecutionException e) {
          stat.setStatus(ResourceStatusType.FETCH_FAILURE);
          stat.setException(RPCUtil.getRemoteException(e.getCause()));
        } catch (CancellationException e) {
          stat.setStatus(ResourceStatusType.FETCH_FAILURE);
          stat.setException(RPCUtil.getRemoteException(e));
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

  public static void main(String[] argv) throws Throwable {
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
      System.exit(localizer.runLocalization(nmAddr));
    } catch (Throwable e) {
      // Print error to stdout so that LCE can use it.
      e.printStackTrace(System.out);
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
      lfs.mkdir(userFileCacheDir, null, false);
      // $x/usercache/$user/appcache/$appId
      Path appBase = new Path(base, new Path(APPCACHE, appId));
      // $x/usercache/$user/appcache/$appId/filecache
      Path appFileCacheDir = new Path(appBase, FILECACHE);
      appsFileCacheDirs[i] = appFileCacheDir.toString();
      lfs.mkdir(appFileCacheDir, null, false);
      // $x/usercache/$user/appcache/$appId/output
      lfs.mkdir(new Path(appBase, OUTPUTDIR), null, false);
    }
    conf.setStrings(String.format(APPCACHE_CTXT_FMT, appId), appsFileCacheDirs);
    conf.setStrings(String.format(USERCACHE_CTXT_FMT, appId), usersFileCacheDirs);
  }

}
