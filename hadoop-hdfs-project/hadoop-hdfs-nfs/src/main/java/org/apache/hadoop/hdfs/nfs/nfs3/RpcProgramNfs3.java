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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.EnumSet;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.nfs.AccessPrivilege;
import org.apache.hadoop.nfs.NfsExports;
import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.NFSPROC3;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Interface;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.ACCESS3Request;
import org.apache.hadoop.nfs.nfs3.request.COMMIT3Request;
import org.apache.hadoop.nfs.nfs3.request.CREATE3Request;
import org.apache.hadoop.nfs.nfs3.request.FSINFO3Request;
import org.apache.hadoop.nfs.nfs3.request.FSSTAT3Request;
import org.apache.hadoop.nfs.nfs3.request.GETATTR3Request;
import org.apache.hadoop.nfs.nfs3.request.LOOKUP3Request;
import org.apache.hadoop.nfs.nfs3.request.MKDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.PATHCONF3Request;
import org.apache.hadoop.nfs.nfs3.request.READ3Request;
import org.apache.hadoop.nfs.nfs3.request.READDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.READDIRPLUS3Request;
import org.apache.hadoop.nfs.nfs3.request.READLINK3Request;
import org.apache.hadoop.nfs.nfs3.request.REMOVE3Request;
import org.apache.hadoop.nfs.nfs3.request.RENAME3Request;
import org.apache.hadoop.nfs.nfs3.request.RMDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.SETATTR3Request;
import org.apache.hadoop.nfs.nfs3.request.SYMLINK3Request;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3.SetAttrField;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.ACCESS3Response;
import org.apache.hadoop.nfs.nfs3.response.COMMIT3Response;
import org.apache.hadoop.nfs.nfs3.response.CREATE3Response;
import org.apache.hadoop.nfs.nfs3.response.FSINFO3Response;
import org.apache.hadoop.nfs.nfs3.response.FSSTAT3Response;
import org.apache.hadoop.nfs.nfs3.response.GETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.LOOKUP3Response;
import org.apache.hadoop.nfs.nfs3.response.MKDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.NFS3Response;
import org.apache.hadoop.nfs.nfs3.response.PATHCONF3Response;
import org.apache.hadoop.nfs.nfs3.response.READ3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response.DirList3;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response.Entry3;
import org.apache.hadoop.nfs.nfs3.response.READDIRPLUS3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIRPLUS3Response.DirListPlus3;
import org.apache.hadoop.nfs.nfs3.response.READLINK3Response;
import org.apache.hadoop.nfs.nfs3.response.REMOVE3Response;
import org.apache.hadoop.nfs.nfs3.response.RENAME3Response;
import org.apache.hadoop.nfs.nfs3.response.RMDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.SETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.SYMLINK3Response;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcCallCache;
import org.apache.hadoop.oncrpc.RpcDeniedReply;
import org.apache.hadoop.oncrpc.RpcInfo;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.RpcReply;
import org.apache.hadoop.oncrpc.RpcResponse;
import org.apache.hadoop.oncrpc.RpcUtil;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Credentials;
import org.apache.hadoop.oncrpc.security.CredentialsSys;
import org.apache.hadoop.oncrpc.security.RpcAuthInfo.AuthFlavor;
import org.apache.hadoop.oncrpc.security.SecurityHandler;
import org.apache.hadoop.oncrpc.security.SysSecurityHandler;
import org.apache.hadoop.oncrpc.security.Verifier;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.IdMappingConstant;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.ShellBasedIdMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC program corresponding to nfs daemon. See {@link Nfs3}.
 */
public class RpcProgramNfs3 extends RpcProgram implements Nfs3Interface {
  public static final int DEFAULT_UMASK = 0022;
  public static final FsPermission umask = new FsPermission(
      (short) DEFAULT_UMASK);

  static final Logger LOG = LoggerFactory.getLogger(RpcProgramNfs3.class);

  private final NfsConfiguration config;
  private final WriteManager writeManager;
  private final IdMappingServiceProvider iug;
  private final DFSClientCache clientCache;

  private final NfsExports exports;

  private final short replication;
  private final long blockSize;
  private final int bufferSize;
  private final boolean aixCompatMode;
  private String writeDumpDir; // The dir save dump files

  private final RpcCallCache rpcCallCache;
  private JvmPauseMonitor pauseMonitor;
  private Nfs3HttpServer infoServer = null;
  static Nfs3Metrics metrics;
  private String superuser;

  public RpcProgramNfs3(NfsConfiguration config, DatagramSocket registrationSocket,
      boolean allowInsecurePorts) throws IOException {
    super("NFS3", "localhost", config.getInt(
        NfsConfigKeys.DFS_NFS_SERVER_PORT_KEY,
        NfsConfigKeys.DFS_NFS_SERVER_PORT_DEFAULT), Nfs3Constant.PROGRAM,
        Nfs3Constant.VERSION, Nfs3Constant.VERSION, registrationSocket,
        allowInsecurePorts, config.getInt(
                NfsConfigKeys.NFS_UDP_CLIENT_PORTMAP_TIMEOUT_MILLIS_KEY,
                NfsConfigKeys.NFS_UDP_CLIENT_PORTMAP_TIMEOUT_MILLIS_DEFAULT));

    this.config = config;
    config.set(FsPermission.UMASK_LABEL, "000");
    iug = new ShellBasedIdMapping(config);

    aixCompatMode = config.getBoolean(
        NfsConfigKeys.AIX_COMPAT_MODE_KEY,
        NfsConfigKeys.AIX_COMPAT_MODE_DEFAULT);
    exports = NfsExports.getInstance(config);
    writeManager = new WriteManager(iug, config, aixCompatMode);
    clientCache = new DFSClientCache(config);
    replication = (short) config.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    blockSize = config.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    bufferSize = config.getInt(
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);

    writeDumpDir = config.get(NfsConfigKeys.DFS_NFS_FILE_DUMP_DIR_KEY,
        NfsConfigKeys.DFS_NFS_FILE_DUMP_DIR_DEFAULT);
    boolean enableDump = config.getBoolean(NfsConfigKeys.DFS_NFS_FILE_DUMP_KEY,
        NfsConfigKeys.DFS_NFS_FILE_DUMP_DEFAULT);
    UserGroupInformation.setConfiguration(config);
    SecurityUtil.login(config, NfsConfigKeys.DFS_NFS_KEYTAB_FILE_KEY,
        NfsConfigKeys.DFS_NFS_KERBEROS_PRINCIPAL_KEY);
    superuser = config.get(NfsConfigKeys.NFS_SUPERUSER_KEY,
        NfsConfigKeys.NFS_SUPERUSER_DEFAULT);
    LOG.info("Configured HDFS superuser is {}", superuser);

    if (!enableDump) {
      writeDumpDir = null;
    } else {
      clearDirectory(writeDumpDir);
    }

    rpcCallCache = new RpcCallCache("NFS3", 256);
    infoServer = new Nfs3HttpServer(config);
  }

  public static RpcProgramNfs3 createRpcProgramNfs3(NfsConfiguration config,
      DatagramSocket registrationSocket, boolean allowInsecurePorts)
      throws IOException {
    DefaultMetricsSystem.initialize("Nfs3");
    String displayName = DNS.getDefaultHost("default", "default")
        + config.getInt(NfsConfigKeys.DFS_NFS_SERVER_PORT_KEY,
            NfsConfigKeys.DFS_NFS_SERVER_PORT_DEFAULT);
    metrics = Nfs3Metrics.create(config, displayName);
    return new RpcProgramNfs3(config, registrationSocket, allowInsecurePorts);
  }
  
  private void clearDirectory(String writeDumpDir) throws IOException {
    File dumpDir = new File(writeDumpDir);
    if (dumpDir.exists()) {
      LOG.info("Delete current dump directory {}", writeDumpDir);
      if (!(FileUtil.fullyDelete(dumpDir))) {
        throw new IOException("Cannot remove current dump directory: "
            + dumpDir);
      }
    }
    LOG.info("Create new dump directory {}", writeDumpDir);
    if (!dumpDir.mkdirs()) {
      throw new IOException("Cannot create dump directory " + dumpDir);
    }
  }
  
  @Override
  public void startDaemons() {
    if (pauseMonitor == null) {
      pauseMonitor = new JvmPauseMonitor();
      pauseMonitor.init(config);
      pauseMonitor.start();
      metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);
    }
    writeManager.startAsyncDataService();
    try {
      infoServer.start();
    } catch (IOException e) {
      LOG.error("failed to start web server", e);
    }
  }

  @Override
  public void stopDaemons() {
    if (writeManager != null) {
      writeManager.shutdownAsyncDataService();
    }
    if (pauseMonitor != null) {
      pauseMonitor.stop();
    }
    // Stop the web server
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down web server", e);
      }
    }
  }
  
  @VisibleForTesting
  Nfs3HttpServer getInfoServer() {
    return this.infoServer;
  }

  // Checks the type of IOException and maps it to appropriate Nfs3Status code.
  private int mapErrorStatus(IOException e) {
    if (e instanceof FileNotFoundException) {
      return Nfs3Status.NFS3ERR_STALE;
    } else if (e instanceof AccessControlException) {
      return Nfs3Status.NFS3ERR_ACCES;
    } else {
      return Nfs3Status.NFS3ERR_IO;
    }
  }

  /******************************************************
   * RPC call handlers
   ******************************************************/

  @Override
  public NFS3Response nullProcedure() {
    LOG.debug("NFS NULL");
    return new NFS3Response(Nfs3Status.NFS3_OK);
  }

  @Override
  public GETATTR3Response getattr(XDR xdr, RpcInfo info) {
    return getattr(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  GETATTR3Response getattr(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    GETATTR3Response response = new GETATTR3Response(Nfs3Status.NFS3_OK);

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }

    GETATTR3Request request;
    try {
      request = GETATTR3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid GETATTR request");
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("GETATTR for fileHandle: {} client: {}",
          handle.dumpFileHandle(), remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    Nfs3FileAttributes attrs = null;
    try {
      attrs = writeManager.getFileAttr(dfsClient, handle, iug);
    } catch (RemoteException r) {
      LOG.warn("Exception", r);
      IOException io = r.unwrapRemoteException();
      /**
       * AuthorizationException can be thrown if the user can't be proxy'ed.
       */
      if (io instanceof AuthorizationException) {
        return new GETATTR3Response(Nfs3Status.NFS3ERR_ACCES);
      } else {
        return new GETATTR3Response(Nfs3Status.NFS3ERR_IO);
      }
    } catch (IOException e) {
      LOG.info("Can't get file attribute, fileId={}", handle.getFileId(), e);
      int status = mapErrorStatus(e);
      response.setStatus(status);
      return response;
    }
    if (attrs == null) {
      LOG.error("Can't get path for fileId: {}", handle.getFileId());
      response.setStatus(Nfs3Status.NFS3ERR_STALE);
      return response;
    }
    response.setPostOpAttr(attrs);
    return response;
  }

  // Set attribute, don't support setting "size". For file/dir creation, mode is
  // set during creation and setMode should be false here.
  private void setattrInternal(DFSClient dfsClient, String fileIdPath,
      SetAttr3 newAttr, boolean setMode) throws IOException {
    EnumSet<SetAttrField> updateFields = newAttr.getUpdateFields();

    if (setMode && updateFields.contains(SetAttrField.MODE)) {
      LOG.debug("set new mode: {}", newAttr.getMode());
      dfsClient.setPermission(fileIdPath,
          new FsPermission((short) (newAttr.getMode())));
    }
    if (updateFields.contains(SetAttrField.UID)
        || updateFields.contains(SetAttrField.GID)) {
      String uname = updateFields.contains(SetAttrField.UID) ? iug.getUserName(
          newAttr.getUid(), IdMappingConstant.UNKNOWN_USER) : null;
      String gname = updateFields.contains(SetAttrField.GID) ? iug
          .getGroupName(newAttr.getGid(), IdMappingConstant.UNKNOWN_GROUP) : null;
      dfsClient.setOwner(fileIdPath, uname, gname);
    }

    long atime = updateFields.contains(SetAttrField.ATIME) ? newAttr.getAtime()
        .getMilliSeconds() : -1;
    long mtime = updateFields.contains(SetAttrField.MTIME) ? newAttr.getMtime()
        .getMilliSeconds() : -1;
    if (atime != -1 || mtime != -1) {
      LOG.debug("set atime: {} mtime: {}", atime, mtime);
      dfsClient.setTimes(fileIdPath, mtime, atime);
    }
  }

  @Override
  public SETATTR3Response setattr(XDR xdr, RpcInfo info) {
    return setattr(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  SETATTR3Response setattr(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    SETATTR3Response response = new SETATTR3Response(Nfs3Status.NFS3_OK);

    SETATTR3Request request;
    try {
      request = SETATTR3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid SETATTR request");
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS SETATTR fileHandle: {} client: {}",
          handle.dumpFileHandle(), remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    if (request.getAttr().getUpdateFields().contains(SetAttrField.SIZE)) {
      LOG.error("Setting file size is not supported when setattr, fileId: {}",
          handle.getFileId());
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    String fileIdPath = Nfs3Utils.getFileIdPath(handle);
    Nfs3FileAttributes preOpAttr = null;
    try {
      preOpAttr = Nfs3Utils.getFileAttr(dfsClient, fileIdPath, iug);
      if (preOpAttr == null) {
        LOG.info("Can't get path for fileId: {}", handle.getFileId());
        response.setStatus(Nfs3Status.NFS3ERR_STALE);
        return response;
      }
      WccAttr preOpWcc = Nfs3Utils.getWccAttr(preOpAttr);
      if (request.isCheck()) {
        if (!preOpAttr.getCtime().equals(request.getCtime())) {
          WccData wccData = new WccData(preOpWcc, preOpAttr);
          return new SETATTR3Response(Nfs3Status.NFS3ERR_NOT_SYNC, wccData);
        }
      }

      // check the write access privilege
      if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_WRITE)) {
        return new SETATTR3Response(Nfs3Status.NFS3ERR_ACCES, new WccData(
            preOpWcc, preOpAttr));
      }

      setattrInternal(dfsClient, fileIdPath, request.getAttr(), true);
      Nfs3FileAttributes postOpAttr = Nfs3Utils.getFileAttr(dfsClient,
          fileIdPath, iug);
      WccData wccData = new WccData(preOpWcc, postOpAttr);
      return new SETATTR3Response(Nfs3Status.NFS3_OK, wccData);
    } catch (IOException e) {
      LOG.warn("Exception", e);
      WccData wccData = null;
      try {
        wccData = Nfs3Utils.createWccData(Nfs3Utils.getWccAttr(preOpAttr),
            dfsClient, fileIdPath, iug);
      } catch (IOException e1) {
        LOG.info("Can't get postOpAttr for fileIdPath: {}", fileIdPath, e1);
      }

      int status = mapErrorStatus(e);
      return new SETATTR3Response(status, wccData);
    }
  }

  @Override
  public LOOKUP3Response lookup(XDR xdr, RpcInfo info) {
    return lookup(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  LOOKUP3Response lookup(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    LOOKUP3Response response = new LOOKUP3Response(Nfs3Status.NFS3_OK);

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }

    LOOKUP3Request request;
    try {
      request = LOOKUP3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid LOOKUP request");
      return new LOOKUP3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle dirHandle = request.getHandle();
    String fileName = request.getName();
    int namenodeId = dirHandle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS LOOKUP dir fileHandle: {} name: {} client: {}",
          dirHandle.dumpFileHandle(), fileName, remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    try {
      String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
      Nfs3FileAttributes postOpObjAttr = writeManager.getFileAttr(dfsClient,
          dirHandle, fileName, namenodeId);
      if (postOpObjAttr == null) {
        LOG.debug("NFS LOOKUP fileId: {} name: {} does not exist",
            dirHandle.getFileId(), fileName);
        Nfs3FileAttributes postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient,
            dirFileIdPath, iug);
        return new LOOKUP3Response(Nfs3Status.NFS3ERR_NOENT, null, null,
            postOpDirAttr);
      }

      Nfs3FileAttributes postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient,
          dirFileIdPath, iug);
      if (postOpDirAttr == null) {
        LOG.info("Can't get path for dir fileId: {}", dirHandle.getFileId());
        return new LOOKUP3Response(Nfs3Status.NFS3ERR_STALE);
      }
      FileHandle fileHandle =
          new FileHandle(postOpObjAttr.getFileId(), namenodeId);
      return new LOOKUP3Response(Nfs3Status.NFS3_OK, fileHandle, postOpObjAttr,
          postOpDirAttr);

    } catch (IOException e) {
      LOG.warn("Exception", e);
      int status = mapErrorStatus(e);
      return new LOOKUP3Response(status);
    }
  }

  @Override
  public ACCESS3Response access(XDR xdr, RpcInfo info) {
    return access(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  ACCESS3Response access(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    ACCESS3Response response = new ACCESS3Response(Nfs3Status.NFS3_OK);

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }

    ACCESS3Request request;
    try {
      request = ACCESS3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid ACCESS request");
      return new ACCESS3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();

    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS ACCESS fileHandle: {} client: {}",
          handle.dumpFileHandle(), remoteAddress);
    }
    Nfs3FileAttributes attrs;
    try {
      attrs = writeManager.getFileAttr(dfsClient, handle, iug);

      if (attrs == null) {
        LOG.error("Can't get path for fileId: {}", handle.getFileId());
        return new ACCESS3Response(Nfs3Status.NFS3ERR_STALE);
      }
      if(iug.getUserName(securityHandler.getUid(), "unknown").equals(superuser)) {
        int access = Nfs3Constant.ACCESS3_LOOKUP | Nfs3Constant.ACCESS3_DELETE
            | Nfs3Constant.ACCESS3_EXECUTE | Nfs3Constant.ACCESS3_EXTEND
            | Nfs3Constant.ACCESS3_MODIFY | Nfs3Constant.ACCESS3_READ;
        return new ACCESS3Response(Nfs3Status.NFS3_OK, attrs, access);
      }
      int access = Nfs3Utils.getAccessRightsForUserGroup(
          securityHandler.getUid(), securityHandler.getGid(),
          securityHandler.getAuxGids(), attrs);

      return new ACCESS3Response(Nfs3Status.NFS3_OK, attrs, access);
    } catch (RemoteException r) {
      LOG.warn("Exception", r);
      IOException io = r.unwrapRemoteException();
      /**
       * AuthorizationException can be thrown if the user can't be proxy'ed.
       */
      if (io instanceof AuthorizationException) {
        return new ACCESS3Response(Nfs3Status.NFS3ERR_ACCES);
      } else {
        return new ACCESS3Response(Nfs3Status.NFS3ERR_IO);
      }
    } catch (IOException e) {
      LOG.warn("Exception", e);
      int status = mapErrorStatus(e);
      return new ACCESS3Response(status);
    }
  }

  @Override
  public READLINK3Response readlink(XDR xdr, RpcInfo info) {
    return readlink(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  READLINK3Response readlink(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    READLINK3Response response = new READLINK3Response(Nfs3Status.NFS3_OK);

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }

    READLINK3Request request;

    try {
      request = READLINK3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid READLINK request");
      return new READLINK3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS READLINK fileHandle: {} client: {}",
          handle.dumpFileHandle(), remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    String fileIdPath = Nfs3Utils.getFileIdPath(handle);
    try {
      String target = dfsClient.getLinkTarget(fileIdPath);

      Nfs3FileAttributes postOpAttr = Nfs3Utils.getFileAttr(dfsClient,
          fileIdPath, iug);
      if (postOpAttr == null) {
        LOG.info("Can't get path for fileId: {}", handle.getFileId());
        return new READLINK3Response(Nfs3Status.NFS3ERR_STALE);
      }
      if (postOpAttr.getType() != NfsFileType.NFSLNK.toValue()) {
        LOG.error("Not a symlink, fileId: {}", handle.getFileId());
        return new READLINK3Response(Nfs3Status.NFS3ERR_INVAL);
      }
      if (target == null) {
        LOG.error("Symlink target should not be null, fileId: {}",
            handle.getFileId());
        return new READLINK3Response(Nfs3Status.NFS3ERR_SERVERFAULT);
      }
      int rtmax = config.getInt(NfsConfigKeys.DFS_NFS_MAX_READ_TRANSFER_SIZE_KEY,
          NfsConfigKeys.DFS_NFS_MAX_READ_TRANSFER_SIZE_DEFAULT);
      if (rtmax < target.getBytes(Charset.forName("UTF-8")).length) {
        LOG.error("Link size: {} is larger than max transfer size: {}",
            target.getBytes(Charset.forName("UTF-8")).length, rtmax);
        return new READLINK3Response(Nfs3Status.NFS3ERR_IO, postOpAttr,
            new byte[0]);
      }

      return new READLINK3Response(Nfs3Status.NFS3_OK, postOpAttr,
          target.getBytes(Charset.forName("UTF-8")));

    } catch (IOException e) {
      LOG.warn("Readlink error", e);
      int status = mapErrorStatus(e);
      return new READLINK3Response(status);
    }
  }

  @Override
  public READ3Response read(XDR xdr, RpcInfo info) {
    return read(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  READ3Response read(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    READ3Response response = new READ3Response(Nfs3Status.NFS3_OK);
    final String userName = securityHandler.getUser();

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }

    READ3Request request;

    try {
      request = READ3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid READ request");
      return new READ3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    long offset = request.getOffset();
    int count = request.getCount();

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS READ fileHandle: {} offset: {} count: {} client: {}",
          handle.dumpFileHandle(), offset, count, remoteAddress);
    }
    DFSClient dfsClient = clientCache.getDfsClient(userName, namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    Nfs3FileAttributes attrs;
    boolean eof;
    if (count == 0) {
      // Only do access check.
      try {
        // Don't read from cache. Client may not have read permission.
        attrs = Nfs3Utils.getFileAttr(dfsClient,
            Nfs3Utils.getFileIdPath(handle), iug);
      } catch (IOException e) {
        LOG.debug("Get error accessing file, fileId: {}",
            handle.getFileId(), e);
        return new READ3Response(Nfs3Status.NFS3ERR_IO);
      }
      if (attrs == null) {
        LOG.debug("Can't get path for fileId: {}", handle.getFileId());
        return new READ3Response(Nfs3Status.NFS3ERR_NOENT);
      }
      int access = Nfs3Utils.getAccessRightsForUserGroup(
          securityHandler.getUid(), securityHandler.getGid(),
          securityHandler.getAuxGids(), attrs);
      if ((access & Nfs3Constant.ACCESS3_READ) != 0) {
        eof = offset >= attrs.getSize();
        return new READ3Response(Nfs3Status.NFS3_OK, attrs, 0, eof,
            ByteBuffer.wrap(new byte[0]));
      } else {
        return new READ3Response(Nfs3Status.NFS3ERR_ACCES);
      }
    }

    // In case there is buffered data for the same file, flush it. This can be
    // optimized later by reading from the cache.
    int ret = writeManager.commitBeforeRead(dfsClient, handle, offset + count);
    if (ret != Nfs3Status.NFS3_OK) {
      LOG.warn("commitBeforeRead didn't succeed with ret={}. " +
          "Read may not get most recent data.", ret);
    }

    try {
      int rtmax = config.getInt(NfsConfigKeys.DFS_NFS_MAX_READ_TRANSFER_SIZE_KEY,
          NfsConfigKeys.DFS_NFS_MAX_READ_TRANSFER_SIZE_DEFAULT);
      int buffSize = Math.min(rtmax, count);
      byte[] readbuffer = new byte[buffSize];

      int readCount = 0;
      /**
       * Retry exactly once because the DFSInputStream can be stale.
       */
      for (int i = 0; i < 1; ++i) {
        FSDataInputStream fis = clientCache.getDfsInputStream(userName,
            Nfs3Utils.getFileIdPath(handle), namenodeId);

        if (fis == null) {
            return new READ3Response(Nfs3Status.NFS3ERR_ACCES);
        }

        try {
          readCount = fis.read(offset, readbuffer, 0, count);
          metrics.incrBytesRead(readCount);
        } catch (IOException e) {
          // TODO: A cleaner way is to throw a new type of exception
          // which requires incompatible changes.
          if (e.getMessage().equals("Stream closed")) {
            clientCache.invalidateDfsInputStream(userName,
                Nfs3Utils.getFileIdPath(handle), namenodeId);
            continue;
          } else {
            throw e;
          }
        }
      }

      attrs = Nfs3Utils.getFileAttr(dfsClient, Nfs3Utils.getFileIdPath(handle),
          iug);
      if (readCount < count) {
        LOG.info("Partial read. Asked offset: {} count: {} and read back: {} " +
                "file size: {}", offset, count, readCount, attrs.getSize());
      }
      // HDFS returns -1 for read beyond file size.
      if (readCount < 0) {
        readCount = 0;
      }
      eof = (offset + readCount) >= attrs.getSize();
      return new READ3Response(Nfs3Status.NFS3_OK, attrs, readCount, eof,
          ByteBuffer.wrap(readbuffer));

    } catch (IOException e) {
      LOG.warn("Read error. Offset: {} count: {}", offset, count, e);
      int status = mapErrorStatus(e);
      return new READ3Response(status);
    }
  }

  @Override
  public WRITE3Response write(XDR xdr, RpcInfo info) {
    SecurityHandler securityHandler = getSecurityHandler(info);
    RpcCall rpcCall = (RpcCall) info.header();
    int xid = rpcCall.getXid();
    SocketAddress remoteAddress = info.remoteAddress();
    return write(xdr, info.channel(), xid, securityHandler, remoteAddress);
  }

  @VisibleForTesting
  WRITE3Response write(XDR xdr, Channel channel, int xid,
      SecurityHandler securityHandler, SocketAddress remoteAddress) {
    WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK);


    WRITE3Request request;

    try {
      request = WRITE3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid WRITE request");
      return new WRITE3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    long offset = request.getOffset();
    int count = request.getCount();
    WriteStableHow stableHow = request.getStableHow();
    byte[] data = request.getData().array();
    if (data.length < count) {
      LOG.error("Invalid argument, data size is less than count in request");
      return new WRITE3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS WRITE fileHandle: {} offset: {} length: {} " +
              "stableHow: {} xid: {} client: {}",
          handle.dumpFileHandle(), offset, count, stableHow.getValue(), xid,
          remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    Nfs3FileAttributes preOpAttr = null;
    try {
      preOpAttr = writeManager.getFileAttr(dfsClient, handle, iug);
      if (preOpAttr == null) {
        LOG.error("Can't get path for fileId: {}", handle.getFileId());
        return new WRITE3Response(Nfs3Status.NFS3ERR_STALE);
      }

      if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_WRITE)) {
        return new WRITE3Response(Nfs3Status.NFS3ERR_ACCES, new WccData(
            Nfs3Utils.getWccAttr(preOpAttr), preOpAttr), 0, stableHow,
            Nfs3Constant.WRITE_COMMIT_VERF);
      }

      LOG.debug("requested offset={} and current filesize={}",
          offset, preOpAttr.getSize());

      writeManager.handleWrite(dfsClient, request, channel, xid, preOpAttr);

    } catch (IOException e) {
      LOG.info("Error writing to fileId {} at offset {} and length {}",
          handle.getFileId(), offset, data.length, e);
      // Try to return WccData
      Nfs3FileAttributes postOpAttr = null;
      try {
        postOpAttr = writeManager.getFileAttr(dfsClient, handle, iug);
      } catch (IOException e1) {
        LOG.info("Can't get postOpAttr for fileId: {}", e1);
      }
      WccAttr attr = preOpAttr == null ? null : Nfs3Utils.getWccAttr(preOpAttr);
      WccData fileWcc = new WccData(attr, postOpAttr);

      int status = mapErrorStatus(e);
      return new WRITE3Response(status, fileWcc, 0, request.getStableHow(),
          Nfs3Constant.WRITE_COMMIT_VERF);
    }

    return null;
  }

  @Override
  public CREATE3Response create(XDR xdr, RpcInfo info) {
    return create(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  CREATE3Response create(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    CREATE3Response response = new CREATE3Response(Nfs3Status.NFS3_OK);

    CREATE3Request request;

    try {
      request = CREATE3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid CREATE request");
      return new CREATE3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle dirHandle = request.getHandle();
    String fileName = request.getName();
    int namenodeId = dirHandle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS CREATE dir fileHandle: {} filename: {} client: {}",
          dirHandle.dumpFileHandle(), fileName, remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    int createMode = request.getMode();
    if ((createMode != Nfs3Constant.CREATE_EXCLUSIVE)
        && request.getObjAttr().getUpdateFields().contains(SetAttrField.SIZE)
        && request.getObjAttr().getSize() != 0) {
      LOG.error("Setting file size is not supported when creating file: {} " +
          "dir fileId: {}", fileName, dirHandle.getFileId());
      return new CREATE3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    HdfsDataOutputStream fos = null;
    String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
    Nfs3FileAttributes preOpDirAttr = null;
    Nfs3FileAttributes postOpObjAttr = null;
    FileHandle fileHandle = null;
    WccData dirWcc = null;
    try {
      preOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
      if (preOpDirAttr == null) {
        LOG.error("Can't get path for dirHandle: {}", dirHandle);
        return new CREATE3Response(Nfs3Status.NFS3ERR_STALE);
      }

      if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_WRITE)) {
        return new CREATE3Response(Nfs3Status.NFS3ERR_ACCES, null,
            preOpDirAttr, new WccData(Nfs3Utils.getWccAttr(preOpDirAttr),
                preOpDirAttr));
      }

      String fileIdPath = Nfs3Utils.getFileIdPath(dirHandle) + "/" + fileName;
      SetAttr3 setAttr3 = request.getObjAttr();
      assert (setAttr3 != null);
      FsPermission permission = setAttr3.getUpdateFields().contains(
          SetAttrField.MODE) ? new FsPermission((short) setAttr3.getMode())
          : FsPermission.getDefault().applyUMask(umask);

      EnumSet<CreateFlag> flag = (createMode != Nfs3Constant.CREATE_EXCLUSIVE) ? 
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE) : 
          EnumSet.of(CreateFlag.CREATE);

      fos = dfsClient.createWrappedOutputStream(
          dfsClient.create(fileIdPath, permission, flag, false, replication,
              blockSize, null, bufferSize, null),
          null);

      if ((createMode == Nfs3Constant.CREATE_UNCHECKED)
          || (createMode == Nfs3Constant.CREATE_GUARDED)) {
        // Set group if it's not specified in the request.
        if (!setAttr3.getUpdateFields().contains(SetAttrField.GID)) {
          setAttr3.getUpdateFields().add(SetAttrField.GID);
          setAttr3.setGid(securityHandler.getGid());
        }
        setattrInternal(dfsClient, fileIdPath, setAttr3, false);
      }

      postOpObjAttr = Nfs3Utils.getFileAttr(dfsClient, fileIdPath, iug);
      dirWcc = Nfs3Utils.createWccData(Nfs3Utils.getWccAttr(preOpDirAttr),
          dfsClient, dirFileIdPath, iug);

      // Add open stream
      OpenFileCtx openFileCtx = new OpenFileCtx(fos, postOpObjAttr,
          writeDumpDir + "/" + postOpObjAttr.getFileId(), dfsClient, iug,
          aixCompatMode, config);
      fileHandle = new FileHandle(postOpObjAttr.getFileId(), namenodeId);
      if (!writeManager.addOpenFileStream(fileHandle, openFileCtx)) {
        LOG.warn("Can't add more stream, close it."
            + " Future write will become append");
        fos.close();
        fos = null;
      } else {
        LOG.debug("Opened stream for file: {}, fileId: {}",
            fileName, fileHandle.getFileId());
      }

    } catch (IOException e) {
      LOG.error("Exception", e);
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e1) {
          LOG.error("Can't close stream for dirFileId: {} filename: {}",
              dirHandle.getFileId(), fileName, e1);
        }
      }
      if (dirWcc == null) {
        try {
          dirWcc = Nfs3Utils.createWccData(Nfs3Utils.getWccAttr(preOpDirAttr),
              dfsClient, dirFileIdPath, iug);
        } catch (IOException e1) {
          LOG.error("Can't get postOpDirAttr for dirFileId: {}",
              dirHandle.getFileId(), e1);
        }
      }

      int status = mapErrorStatus(e);
      return new CREATE3Response(status, fileHandle, postOpObjAttr, dirWcc);
    }

    return new CREATE3Response(Nfs3Status.NFS3_OK, fileHandle, postOpObjAttr,
        dirWcc);
  }

  @Override
  public MKDIR3Response mkdir(XDR xdr, RpcInfo info) {
    return mkdir(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  MKDIR3Response mkdir(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    MKDIR3Response response = new MKDIR3Response(Nfs3Status.NFS3_OK);

    MKDIR3Request request;

    try {
      request = MKDIR3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid MKDIR request");
      return new MKDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    FileHandle dirHandle = request.getHandle();
    String fileName = request.getName();
    int namenodeId = dirHandle.getNamenodeId();

    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS MKDIR dirHandle: {} filename: {} client: {}",
          dirHandle.dumpFileHandle(), fileName, remoteAddress);
    }
    if (request.getObjAttr().getUpdateFields().contains(SetAttrField.SIZE)) {
      LOG.error("Setting file size is not supported when mkdir: " +
          "{} in dirHandle {}", fileName, dirHandle);
      return new MKDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
    Nfs3FileAttributes preOpDirAttr = null;
    Nfs3FileAttributes postOpDirAttr = null;
    Nfs3FileAttributes postOpObjAttr = null;
    FileHandle objFileHandle = null;
    try {
      preOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
      if (preOpDirAttr == null) {
        LOG.info("Can't get path for dir fileId: {}", dirHandle.getFileId());
        return new MKDIR3Response(Nfs3Status.NFS3ERR_STALE);
      }

      if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_WRITE)) {
        return new MKDIR3Response(Nfs3Status.NFS3ERR_ACCES, null, preOpDirAttr,
            new WccData(Nfs3Utils.getWccAttr(preOpDirAttr), preOpDirAttr));
      }

      final String fileIdPath = dirFileIdPath + "/" + fileName;
      SetAttr3 setAttr3 = request.getObjAttr();
      FsPermission permission = setAttr3.getUpdateFields().contains(
          SetAttrField.MODE) ? new FsPermission((short) setAttr3.getMode())
          : FsPermission.getDefault().applyUMask(umask);

      if (!dfsClient.mkdirs(fileIdPath, permission, false)) {
        WccData dirWcc = Nfs3Utils.createWccData(
            Nfs3Utils.getWccAttr(preOpDirAttr), dfsClient, dirFileIdPath, iug);
        return new MKDIR3Response(Nfs3Status.NFS3ERR_IO, null, null, dirWcc);
      }

      // Set group if it's not specified in the request.
      if (!setAttr3.getUpdateFields().contains(SetAttrField.GID)) {
        setAttr3.getUpdateFields().add(SetAttrField.GID);
        setAttr3.setGid(securityHandler.getGid());
      }
      setattrInternal(dfsClient, fileIdPath, setAttr3, false);

      postOpObjAttr = Nfs3Utils.getFileAttr(dfsClient, fileIdPath, iug);
      objFileHandle = new FileHandle(postOpObjAttr.getFileId(), namenodeId);
      WccData dirWcc = Nfs3Utils.createWccData(
          Nfs3Utils.getWccAttr(preOpDirAttr), dfsClient, dirFileIdPath, iug);
      return new MKDIR3Response(Nfs3Status.NFS3_OK, new FileHandle(
          postOpObjAttr.getFileId(), namenodeId), postOpObjAttr, dirWcc);
    } catch (IOException e) {
      LOG.warn("Exception", e);
      // Try to return correct WccData
      if (postOpDirAttr == null) {
        try {
          postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
        } catch (IOException e1) {
          LOG.info("Can't get postOpDirAttr for {}", dirFileIdPath, e);
        }
      }

      WccData dirWcc = new WccData(Nfs3Utils.getWccAttr(preOpDirAttr),
          postOpDirAttr);
      int status = mapErrorStatus(e);
      return new MKDIR3Response(status, objFileHandle, postOpObjAttr, dirWcc);
    }
  }

  @Override
  public READDIR3Response mknod(XDR xdr, RpcInfo info) {
    return new READDIR3Response(Nfs3Status.NFS3ERR_NOTSUPP);
  }

  @Override
  public REMOVE3Response remove(XDR xdr, RpcInfo info) {
    return remove(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  REMOVE3Response remove(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    REMOVE3Response response = new REMOVE3Response(Nfs3Status.NFS3_OK);

    REMOVE3Request request;
    try {
      request = REMOVE3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid REMOVE request");
      return new REMOVE3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    FileHandle dirHandle = request.getHandle();
    int namenodeId = dirHandle.getNamenodeId();

    String fileName = request.getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS REMOVE dir fileHandle: {} fileName: {} client: {}",
          dirHandle.dumpFileHandle(), fileName, remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
    Nfs3FileAttributes preOpDirAttr = null;
    Nfs3FileAttributes postOpDirAttr = null;
    try {
      preOpDirAttr =  Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
      if (preOpDirAttr == null) {
        LOG.info("Can't get path for dir fileId: {}", dirHandle.getFileId());
        return new REMOVE3Response(Nfs3Status.NFS3ERR_STALE);
      }

      WccData errWcc = new WccData(Nfs3Utils.getWccAttr(preOpDirAttr),
          preOpDirAttr);
      if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_WRITE)) {
        return new REMOVE3Response(Nfs3Status.NFS3ERR_ACCES, errWcc);
      }

      String fileIdPath = dirFileIdPath + "/" + fileName;
      HdfsFileStatus fstat = Nfs3Utils.getFileStatus(dfsClient, fileIdPath);
      if (fstat == null) {
        return new REMOVE3Response(Nfs3Status.NFS3ERR_NOENT, errWcc);
      }
      if (fstat.isDirectory()) {
        return new REMOVE3Response(Nfs3Status.NFS3ERR_ISDIR, errWcc);
      }

      boolean result = dfsClient.delete(fileIdPath, false);
      WccData dirWcc = Nfs3Utils.createWccData(
          Nfs3Utils.getWccAttr(preOpDirAttr), dfsClient, dirFileIdPath, iug);

      if (!result) {
        return new REMOVE3Response(Nfs3Status.NFS3ERR_ACCES, dirWcc);
      }
      return new REMOVE3Response(Nfs3Status.NFS3_OK, dirWcc);
    } catch (IOException e) {
      LOG.warn("Exception", e);
      // Try to return correct WccData
      if (postOpDirAttr == null) {
        try {
          postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
        } catch (IOException e1) {
          LOG.info("Can't get postOpDirAttr for {}", dirFileIdPath, e1);
        }
      }

      WccData dirWcc = new WccData(Nfs3Utils.getWccAttr(preOpDirAttr),
          postOpDirAttr);
      int status = mapErrorStatus(e);
      return new REMOVE3Response(status, dirWcc);
    }
  }

  @Override
  public RMDIR3Response rmdir(XDR xdr, RpcInfo info) {
    return rmdir(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  RMDIR3Response rmdir(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    RMDIR3Response response = new RMDIR3Response(Nfs3Status.NFS3_OK);

    RMDIR3Request request;
    try {
      request = RMDIR3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid RMDIR request");
      return new RMDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    FileHandle dirHandle = request.getHandle();
    String fileName = request.getName();
    int namenodeId = dirHandle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS RMDIR dir fileHandle: {} fileName: {} client: {}",
          dirHandle.dumpFileHandle(), fileName, remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
    Nfs3FileAttributes preOpDirAttr = null;
    Nfs3FileAttributes postOpDirAttr = null;
    try {
      preOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
      if (preOpDirAttr == null) {
        LOG.info("Can't get path for dir fileId: {}", dirHandle.getFileId());
        return new RMDIR3Response(Nfs3Status.NFS3ERR_STALE);
      }

      WccData errWcc = new WccData(Nfs3Utils.getWccAttr(preOpDirAttr),
          preOpDirAttr);
      if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_WRITE)) {
        return new RMDIR3Response(Nfs3Status.NFS3ERR_ACCES, errWcc); 
      }

      String fileIdPath = dirFileIdPath + "/" + fileName;
      HdfsFileStatus fstat = Nfs3Utils.getFileStatus(dfsClient, fileIdPath);
      if (fstat == null) {
        return new RMDIR3Response(Nfs3Status.NFS3ERR_NOENT, errWcc);
      }
      if (!fstat.isDirectory()) {
        return new RMDIR3Response(Nfs3Status.NFS3ERR_NOTDIR, errWcc);
      }

      if (fstat.getChildrenNum() > 0) {
        return new RMDIR3Response(Nfs3Status.NFS3ERR_NOTEMPTY, errWcc);
      }

      boolean result = dfsClient.delete(fileIdPath, false);
      WccData dirWcc = Nfs3Utils.createWccData(
          Nfs3Utils.getWccAttr(preOpDirAttr), dfsClient, dirFileIdPath, iug);
      if (!result) {
        return new RMDIR3Response(Nfs3Status.NFS3ERR_ACCES, dirWcc);
      }

      return new RMDIR3Response(Nfs3Status.NFS3_OK, dirWcc);
    } catch (IOException e) {
      LOG.warn("Exception", e);
      // Try to return correct WccData
      if (postOpDirAttr == null) {
        try {
          postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
        } catch (IOException e1) {
          LOG.info("Can't get postOpDirAttr for {}", dirFileIdPath, e1);
        }
      }

      WccData dirWcc = new WccData(Nfs3Utils.getWccAttr(preOpDirAttr),
          postOpDirAttr);
      int status = mapErrorStatus(e);
      return new RMDIR3Response(status, dirWcc);
    }
  }

  @Override
  public RENAME3Response rename(XDR xdr, RpcInfo info) {
    return rename(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  RENAME3Response rename(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    RENAME3Response response = new RENAME3Response(Nfs3Status.NFS3_OK);

    RENAME3Request request = null;
    try {
      request = RENAME3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid RENAME request");
      return new RENAME3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle fromHandle = request.getFromDirHandle();
    int fromNamenodeId = fromHandle.getNamenodeId();
    String fromName = request.getFromName();
    FileHandle toHandle = request.getToDirHandle();
    int toNamenodeId = toHandle.getNamenodeId();
    String toName = request.getToName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS RENAME from: {}/{} to: {}/{} client: {}",
          fromHandle.dumpFileHandle(), fromName, toHandle.dumpFileHandle(),
          toName, remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), fromNamenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    if (fromNamenodeId != toNamenodeId) {
      // renaming file from one namenode to another is not supported
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    String fromDirFileIdPath = Nfs3Utils.getFileIdPath(fromHandle);
    String toDirFileIdPath = Nfs3Utils.getFileIdPath(toHandle);
    Nfs3FileAttributes fromPreOpAttr = null;
    Nfs3FileAttributes toPreOpAttr = null;
    WccData fromDirWcc = null;
    WccData toDirWcc = null;
    try {
      fromPreOpAttr = Nfs3Utils.getFileAttr(dfsClient, fromDirFileIdPath, iug);
      if (fromPreOpAttr == null) {
        LOG.info("Can't get path for fromHandle fileId: {}",
            fromHandle.getFileId());
        return new RENAME3Response(Nfs3Status.NFS3ERR_STALE);
      }

      toPreOpAttr = Nfs3Utils.getFileAttr(dfsClient, toDirFileIdPath, iug);
      if (toPreOpAttr == null) {
        LOG.info("Can't get path for toHandle fileId: {}",
            toHandle.getFileId());
        return new RENAME3Response(Nfs3Status.NFS3ERR_STALE);
      }

      if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_WRITE)) {
        WccData fromWcc = new WccData(Nfs3Utils.getWccAttr(fromPreOpAttr),
            fromPreOpAttr);
        WccData toWcc = new WccData(Nfs3Utils.getWccAttr(toPreOpAttr),
            toPreOpAttr);
        return new RENAME3Response(Nfs3Status.NFS3ERR_ACCES, fromWcc, toWcc);
      }

      String src = fromDirFileIdPath + "/" + fromName;
      String dst = toDirFileIdPath + "/" + toName;

      dfsClient.rename(src, dst, Options.Rename.NONE);

      // Assemble the reply
      fromDirWcc = Nfs3Utils.createWccData(Nfs3Utils.getWccAttr(fromPreOpAttr),
          dfsClient, fromDirFileIdPath, iug);
      toDirWcc = Nfs3Utils.createWccData(Nfs3Utils.getWccAttr(toPreOpAttr),
          dfsClient, toDirFileIdPath, iug);
      return new RENAME3Response(Nfs3Status.NFS3_OK, fromDirWcc, toDirWcc);
    } catch (IOException e) {
      LOG.warn("Exception", e);
      // Try to return correct WccData
      try {
        fromDirWcc = Nfs3Utils.createWccData(
            Nfs3Utils.getWccAttr(fromPreOpAttr), dfsClient, fromDirFileIdPath,
            iug);
        toDirWcc = Nfs3Utils.createWccData(Nfs3Utils.getWccAttr(toPreOpAttr),
            dfsClient, toDirFileIdPath, iug);
      } catch (IOException e1) {
        LOG.info("Can't get postOpDirAttr for {} or {}",
            fromDirFileIdPath, toDirFileIdPath, e1);
      }

      int status = mapErrorStatus(e);
      return new RENAME3Response(status, fromDirWcc, toDirWcc);
    }
  }

  @Override
  public SYMLINK3Response symlink(XDR xdr, RpcInfo info) {
    return symlink(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  SYMLINK3Response symlink(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    SYMLINK3Response response = new SYMLINK3Response(Nfs3Status.NFS3_OK);

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_WRITE)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }

    SYMLINK3Request request;
    try {
      request = SYMLINK3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid SYMLINK request");
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    FileHandle dirHandle = request.getHandle();
    String name = request.getName();
    String symData = request.getSymData();
    String linkDirIdPath = Nfs3Utils.getFileIdPath(dirHandle);
    int namenodeId = dirHandle.getNamenodeId();

    // Don't do any name check to source path, just leave it to HDFS
    String linkIdPath = linkDirIdPath + "/" + name;
    LOG.debug("NFS SYMLINK, target: {} link: {} namenodeId: {} client: {}",
        symData, linkIdPath, namenodeId, remoteAddress);

    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    try {
      WccData dirWcc = response.getDirWcc();
      WccAttr preOpAttr = Nfs3Utils.getWccAttr(dfsClient, linkDirIdPath);
      dirWcc.setPreOpAttr(preOpAttr);

      dfsClient.createSymlink(symData, linkIdPath, false);
      // Set symlink attr is considered as to change the attr of the target
      // file. So no need to set symlink attr here after it's created.

      HdfsFileStatus linkstat = dfsClient.getFileLinkInfo(linkIdPath);
      Nfs3FileAttributes objAttr = Nfs3Utils.getNfs3FileAttrFromFileStatus(
          linkstat, iug);
      dirWcc
          .setPostOpAttr(Nfs3Utils.getFileAttr(dfsClient, linkDirIdPath, iug));

      return new SYMLINK3Response(Nfs3Status.NFS3_OK, new FileHandle(
          objAttr.getFileId(), namenodeId), objAttr, dirWcc);

    } catch (IOException e) {
      LOG.warn("Exception", e);
      int status = mapErrorStatus(e);
      response.setStatus(status);
      return response;
    }
  }

  @Override
  public READDIR3Response link(XDR xdr, RpcInfo info) {
    return new READDIR3Response(Nfs3Status.NFS3ERR_NOTSUPP);
  }

  /**
   * Used by readdir and readdirplus to get dirents. It retries the listing if
   * the startAfter can't be found anymore.
   */
  private DirectoryListing listPaths(DFSClient dfsClient, String dirFileIdPath,
      byte[] startAfter) throws IOException {
    DirectoryListing dlisting;
    try {
      dlisting = dfsClient.listPaths(dirFileIdPath, startAfter);
    } catch (RemoteException e) {
      IOException io = e.unwrapRemoteException();
      if (!(io instanceof DirectoryListingStartAfterNotFoundException)) {
        throw io;
      }
      // This happens when startAfter was just deleted
      LOG.info("Cookie couldn't be found: {}, do listing from beginning",
          new String(startAfter, Charset.forName("UTF-8")));
      dlisting = dfsClient
          .listPaths(dirFileIdPath, HdfsFileStatus.EMPTY_NAME);
    }
    return dlisting;
  }

  @Override
  public READDIR3Response readdir(XDR xdr, RpcInfo info) {
    return readdir(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  public READDIR3Response readdir(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    READDIR3Response response = new READDIR3Response(Nfs3Status.NFS3_OK);

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }

    READDIR3Request request;
    try {
      request = READDIR3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid READDIR request");
      return new READDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();

    long cookie = request.getCookie();
    if (cookie < 0) {
      LOG.error("Invalid READDIR request, with negative cookie: {}", cookie);
      return new READDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    long count = request.getCount();
    if (count <= 0) {
      LOG.info("Nonpositive count in invalid READDIR request: {}", count);
      return new READDIR3Response(Nfs3Status.NFS3_OK);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS READDIR fileHandle: {} cookie: {} count: {} client: {}",
          handle.dumpFileHandle(), cookie, count, remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    HdfsFileStatus dirStatus;
    DirectoryListing dlisting;
    Nfs3FileAttributes postOpAttr;
    long dotdotFileId = 0;
    try {
      String dirFileIdPath = Nfs3Utils.getFileIdPath(handle);
      dirStatus = dfsClient.getFileInfo(dirFileIdPath);
      if (dirStatus == null) {
        LOG.info("Can't get path for fileId: {}", handle.getFileId());
        return new READDIR3Response(Nfs3Status.NFS3ERR_STALE);
      }
      if (!dirStatus.isDirectory()) {
        LOG.error("Can't readdir for regular file, fileId: {}",
            handle.getFileId());
        return new READDIR3Response(Nfs3Status.NFS3ERR_NOTDIR);
      }
      long cookieVerf = request.getCookieVerf();
      if ((cookieVerf != 0) && (cookieVerf != dirStatus.getModificationTime())) {
        if (aixCompatMode) {
          // The AIX NFS client misinterprets RFC-1813 and will repeatedly send
          // the same cookieverf value even across VFS-level readdir calls,
          // instead of getting a new cookieverf for every VFS-level readdir
          // call, and reusing the cookieverf only in the event that multiple
          // incremental NFS-level readdir calls must be made to fetch all of
          // the directory entries. This means that whenever a readdir call is
          // made by an AIX NFS client for a given directory, and that directory
          // is subsequently modified, thus changing its mtime, no later readdir
          // calls will succeed from AIX for that directory until the FS is
          // unmounted/remounted. See HDFS-6549 for more info.
          LOG.warn("AIX compatibility mode enabled, ignoring cookieverf " +
              "mismatches.");
        } else {
          LOG.error("CookieVerf mismatch. request cookieVerf: {} " +
              "dir cookieVerf: {}",
              cookieVerf, dirStatus.getModificationTime());
          return new READDIR3Response(
              Nfs3Status.NFS3ERR_BAD_COOKIE,
              Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug));
        }
      }

      if (cookie == 0) {
        // Get dotdot fileId
        String dotdotFileIdPath = dirFileIdPath + "/..";
        HdfsFileStatus dotdotStatus = dfsClient.getFileInfo(dotdotFileIdPath);

        if (dotdotStatus == null) {
          // This should not happen
          throw new IOException("Can't get path for handle path: "
              + dotdotFileIdPath);
        }
        dotdotFileId = dotdotStatus.getFileId();
      }

      // Get the list from the resume point
      byte[] startAfter;
      if(cookie == 0 ) {
        startAfter = HdfsFileStatus.EMPTY_NAME;
      } else {
        String inodeIdPath = Nfs3Utils.getFileIdPath(cookie);
        startAfter = inodeIdPath.getBytes(Charset.forName("UTF-8"));
      }

      dlisting = listPaths(dfsClient, dirFileIdPath, startAfter);
      postOpAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
      if (postOpAttr == null) {
        LOG.error("Can't get path for fileId: {}", handle.getFileId());
        return new READDIR3Response(Nfs3Status.NFS3ERR_STALE);
      }
    } catch (IOException e) {
      LOG.warn("Exception", e);
      int status = mapErrorStatus(e);
      return new READDIR3Response(status);
    }

    /**
     * Set up the dirents in the response. fileId is used as the cookie with one
     * exception. Linux client can either be stuck with "ls" command (on REHL)
     * or report "Too many levels of symbolic links" (Ubuntu).
     *
     * The problem is that, only two items returned, "." and ".." when the
     * namespace is empty. Both of them are "/" with the same cookie(root
     * fileId). Linux client doesn't think such a directory is a real directory.
     * Even though NFS protocol specifies cookie is an opaque data, Linux client
     * somehow doesn't like an empty dir returns same cookie for both "." and
     * "..".
     *
     * The workaround is to use 0 as the cookie for "." and always return "." as
     * the first entry in readdir/readdirplus response.
     */
    HdfsFileStatus[] fstatus = dlisting.getPartialListing();    
    int n = (int) Math.min(fstatus.length, count-2);
    boolean eof = (n >= fstatus.length) && !dlisting.hasMore();

    Entry3[] entries;
    if (cookie == 0) {
      entries = new Entry3[n + 2];
      entries[0] = new READDIR3Response.Entry3(postOpAttr.getFileId(), ".", 0);
      entries[1] = new READDIR3Response.Entry3(dotdotFileId, "..", dotdotFileId);

      for (int i = 2; i < n + 2; i++) {
        entries[i] = new READDIR3Response.Entry3(fstatus[i - 2].getFileId(),
            fstatus[i - 2].getLocalName(), fstatus[i - 2].getFileId());
      }
    } else {
      // Resume from last readdirplus. If the cookie is "..", the result
      // list is up the directory content since HDFS uses name as resume point.    
      entries = new Entry3[n];    
      for (int i = 0; i < n; i++) {
        entries[i] = new READDIR3Response.Entry3(fstatus[i].getFileId(),
            fstatus[i].getLocalName(), fstatus[i].getFileId());
      }
    }

    DirList3 dirList = new READDIR3Response.DirList3(entries, eof);
    return new READDIR3Response(Nfs3Status.NFS3_OK, postOpAttr,
        dirStatus.getModificationTime(), dirList);
  }

  @Override
  public READDIRPLUS3Response readdirplus(XDR xdr, RpcInfo info) {
    return readdirplus(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  READDIRPLUS3Response readdirplus(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_ACCES);
    }


    READDIRPLUS3Request request = null;
    try {
      request = READDIRPLUS3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid READDIRPLUS request");
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();
    long cookie = request.getCookie();
    if (cookie < 0) {
      LOG.error("Invalid READDIRPLUS request, with negative cookie: {}",
          cookie);
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    long dirCount = request.getDirCount();
    if (dirCount <= 0) {
      LOG.info("Nonpositive dircount in invalid READDIRPLUS request: {}",
          dirCount);
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    int maxCount = request.getMaxCount();
    if (maxCount <= 0) {
      LOG.info("Nonpositive maxcount in invalid READDIRPLUS request: {}",
          maxCount);
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS READDIRPLUS fileHandle: {} cookie: {} dirCount: {} " +
              "maxCount: {} client: {}",
          handle.dumpFileHandle(), cookie, dirCount, maxCount, remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_SERVERFAULT);
    }

    HdfsFileStatus dirStatus;
    DirectoryListing dlisting;
    Nfs3FileAttributes postOpDirAttr;
    long dotdotFileId = 0;
    HdfsFileStatus dotdotStatus = null;
    try {
      String dirFileIdPath = Nfs3Utils.getFileIdPath(handle);
      dirStatus = dfsClient.getFileInfo(dirFileIdPath);
      if (dirStatus == null) {
        LOG.info("Can't get path for fileId: {}", handle.getFileId());
        return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_STALE);
      }
      if (!dirStatus.isDirectory()) {
        LOG.error("Can't readdirplus for regular file, fileId: {}",
            handle.getFileId());
        return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_NOTDIR);
      }
      long cookieVerf = request.getCookieVerf();
      if ((cookieVerf != 0) && (cookieVerf != dirStatus.getModificationTime())) {
        if (aixCompatMode) {
          // The AIX NFS client misinterprets RFC-1813 and will repeatedly send
          // the same cookieverf value even across VFS-level readdir calls,
          // instead of getting a new cookieverf for every VFS-level readdir
          // call. This means that whenever a readdir call is made by an AIX NFS
          // client for a given directory, and that directory is subsequently
          // modified, thus changing its mtime, no later readdir calls will
          // succeed for that directory from AIX until the FS is
          // unmounted/remounted. See HDFS-6549 for more info.
          LOG.warn("AIX compatibility mode enabled, ignoring cookieverf " +
              "mismatches.");
        } else {
          LOG.error("cookieverf mismatch. request cookieverf: {} " +
                  "dir cookieverf: {}",
              cookieVerf, dirStatus.getModificationTime());
          return new READDIRPLUS3Response(
              Nfs3Status.NFS3ERR_BAD_COOKIE,
              Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug),
              0, null);
        }
      }

      if (cookie == 0) {
        // Get dotdot fileId
        String dotdotFileIdPath = dirFileIdPath + "/..";
        dotdotStatus = dfsClient.getFileInfo(dotdotFileIdPath);

        if (dotdotStatus == null) {
          // This should not happen
          throw new IOException("Can't get path for handle path: "
              + dotdotFileIdPath);
        }
        dotdotFileId = dotdotStatus.getFileId();
      }

      // Get the list from the resume point
      byte[] startAfter;
      if (cookie == 0) {
        startAfter = HdfsFileStatus.EMPTY_NAME;
      } else {
        String inodeIdPath = Nfs3Utils.getFileIdPath(cookie);
        startAfter = inodeIdPath.getBytes(Charset.forName("UTF-8"));
      }

      dlisting = listPaths(dfsClient, dirFileIdPath, startAfter);
      postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
      if (postOpDirAttr == null) {
        LOG.info("Can't get path for fileId: {}", handle.getFileId());
        return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_STALE);
      }
    } catch (IOException e) {
      LOG.warn("Exception", e);
      int status = mapErrorStatus(e);
      return new READDIRPLUS3Response(status);
    }

    // Set up the dirents in the response
    HdfsFileStatus[] fstatus = dlisting.getPartialListing();
    int n = (int) Math.min(fstatus.length, dirCount-2);
    boolean eof = (n >= fstatus.length) && !dlisting.hasMore();

    READDIRPLUS3Response.EntryPlus3[] entries;
    if (cookie == 0) {
      entries = new READDIRPLUS3Response.EntryPlus3[n+2];

      entries[0] = new READDIRPLUS3Response.EntryPlus3(
          postOpDirAttr.getFileId(), ".", 0, postOpDirAttr, new FileHandle(
              postOpDirAttr.getFileId(), namenodeId));
      entries[1] = new READDIRPLUS3Response.EntryPlus3(dotdotFileId, "..",
          dotdotFileId, Nfs3Utils.getNfs3FileAttrFromFileStatus(dotdotStatus,
              iug), new FileHandle(dotdotFileId, namenodeId));

      for (int i = 2; i < n + 2; i++) {
        long fileId = fstatus[i - 2].getFileId();
        FileHandle childHandle = new FileHandle(fileId, namenodeId);
        Nfs3FileAttributes attr;
        try {
          attr = writeManager.getFileAttr(dfsClient, childHandle, iug);
        } catch (IOException e) {
          LOG.error("Can't get file attributes for fileId: {}", fileId, e);
          continue;
        }
        entries[i] = new READDIRPLUS3Response.EntryPlus3(fileId,
            fstatus[i - 2].getLocalName(), fileId, attr, childHandle);
      }
    } else {
      // Resume from last readdirplus. If the cookie is "..", the result
      // list is up the directory content since HDFS uses name as resume point.
      entries = new READDIRPLUS3Response.EntryPlus3[n]; 
      for (int i = 0; i < n; i++) {
        long fileId = fstatus[i].getFileId();
        FileHandle childHandle = new FileHandle(fileId, namenodeId);
        Nfs3FileAttributes attr;
        try {
          attr = writeManager.getFileAttr(dfsClient, childHandle, iug);
        } catch (IOException e) {
          LOG.error("Can't get file attributes for fileId: {}", fileId, e);
          continue;
        }
        entries[i] = new READDIRPLUS3Response.EntryPlus3(fileId,
            fstatus[i].getLocalName(), fileId, attr, childHandle);
      }
    }

    DirListPlus3 dirListPlus = new READDIRPLUS3Response.DirListPlus3(entries,
        eof);
    return new READDIRPLUS3Response(Nfs3Status.NFS3_OK, postOpDirAttr,
        dirStatus.getModificationTime(), dirListPlus);
  }

  @Override
  public FSSTAT3Response fsstat(XDR xdr, RpcInfo info) {
    return fsstat(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  FSSTAT3Response fsstat(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    FSSTAT3Response response = new FSSTAT3Response(Nfs3Status.NFS3_OK);

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }


    FSSTAT3Request request;
    try {
      request = FSSTAT3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid FSSTAT request");
      return new FSSTAT3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS FSSTAT fileHandle: {} client: {}",
          handle.dumpFileHandle(), remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    try {
      FsStatus fsStatus = dfsClient.getDiskStatus();
      long totalBytes = fsStatus.getCapacity();
      long freeBytes = fsStatus.getRemaining();

      Nfs3FileAttributes attrs = writeManager.getFileAttr(dfsClient, handle,
          iug);
      if (attrs == null) {
        LOG.info("Can't get path for fileId: {}", handle.getFileId());
        return new FSSTAT3Response(Nfs3Status.NFS3ERR_STALE);
      }

      long maxFsObjects = config.getLong("dfs.max.objects", 0);
      if (maxFsObjects == 0) {
        // A value of zero in HDFS indicates no limit to the number
        // of objects that dfs supports. Using Integer.MAX_VALUE instead of
        // Long.MAX_VALUE so 32bit client won't complain.
        maxFsObjects = Integer.MAX_VALUE;
      }

      return new FSSTAT3Response(Nfs3Status.NFS3_OK, attrs, totalBytes,
          freeBytes, freeBytes, maxFsObjects, maxFsObjects, maxFsObjects, 0);
    } catch (RemoteException r) {
      LOG.warn("Exception", r);
      IOException io = r.unwrapRemoteException();
      /**
       * AuthorizationException can be thrown if the user can't be proxy'ed.
       */
      if (io instanceof AuthorizationException) {
        return new FSSTAT3Response(Nfs3Status.NFS3ERR_ACCES);
      } else {
        return new FSSTAT3Response(Nfs3Status.NFS3ERR_IO);
      }
    } catch (IOException e) {
      LOG.warn("Exception", e);
      int status = mapErrorStatus(e);
      return new FSSTAT3Response(status);
    }
  }

  @Override
  public FSINFO3Response fsinfo(XDR xdr, RpcInfo info) {
    return fsinfo(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  FSINFO3Response fsinfo(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    FSINFO3Response response = new FSINFO3Response(Nfs3Status.NFS3_OK);

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }

    FSINFO3Request request;
    try {
      request = FSINFO3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid FSINFO request");
      return new FSINFO3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS FSINFO fileHandle: {} client: {}", remoteAddress,
          handle.dumpFileHandle(), remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    try {
      int rtmax = config.getInt(
          NfsConfigKeys.DFS_NFS_MAX_READ_TRANSFER_SIZE_KEY,
          NfsConfigKeys.DFS_NFS_MAX_READ_TRANSFER_SIZE_DEFAULT);
      int wtmax = config.getInt(
          NfsConfigKeys.DFS_NFS_MAX_WRITE_TRANSFER_SIZE_KEY,
          NfsConfigKeys.DFS_NFS_MAX_WRITE_TRANSFER_SIZE_DEFAULT);
      int dtperf = config.getInt(
          NfsConfigKeys.DFS_NFS_MAX_READDIR_TRANSFER_SIZE_KEY,
          NfsConfigKeys.DFS_NFS_MAX_READDIR_TRANSFER_SIZE_DEFAULT);

      Nfs3FileAttributes attrs = Nfs3Utils.getFileAttr(dfsClient,
          Nfs3Utils.getFileIdPath(handle), iug);
      if (attrs == null) {
        LOG.info("Can't get path for fileId: {}", handle.getFileId());
        return new FSINFO3Response(Nfs3Status.NFS3ERR_STALE);
      }

      int fsProperty = Nfs3Constant.FSF3_CANSETTIME
          | Nfs3Constant.FSF3_HOMOGENEOUS;

      return new FSINFO3Response(Nfs3Status.NFS3_OK, attrs, rtmax, rtmax, 1,
          wtmax, wtmax, 1, dtperf, Long.MAX_VALUE, new NfsTime(1), fsProperty);
    } catch (IOException e) {
      LOG.warn("Exception", e);
      int status = mapErrorStatus(e);
      return new FSINFO3Response(status);
    }
  }

  @Override
  public PATHCONF3Response pathconf(XDR xdr, RpcInfo info) {
    return pathconf(xdr, getSecurityHandler(info), info.remoteAddress());
  }

  @VisibleForTesting
  PATHCONF3Response pathconf(XDR xdr, SecurityHandler securityHandler,
      SocketAddress remoteAddress) {
    PATHCONF3Response response = new PATHCONF3Response(Nfs3Status.NFS3_OK);

    if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_ONLY)) {
      response.setStatus(Nfs3Status.NFS3ERR_ACCES);
      return response;
    }

    PATHCONF3Request request;
    try {
      request = PATHCONF3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid PATHCONF request");
      return new PATHCONF3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    Nfs3FileAttributes attrs;
    int namenodeId = handle.getNamenodeId();

    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS PATHCONF fileHandle: {} client: {}",
          handle.dumpFileHandle(), remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    try {
      attrs = Nfs3Utils.getFileAttr(dfsClient, Nfs3Utils.getFileIdPath(handle),
          iug);
      if (attrs == null) {
        LOG.info("Can't get path for fileId: {}", handle.getFileId());
        return new PATHCONF3Response(Nfs3Status.NFS3ERR_STALE);
      }

      return new PATHCONF3Response(Nfs3Status.NFS3_OK, attrs, 0,
          HdfsServerConstants.MAX_PATH_LENGTH, true, false, false, true);
    } catch (IOException e) {
      LOG.warn("Exception", e);
      int status = mapErrorStatus(e);
      return new PATHCONF3Response(status);
    }
  }

  @Override
  public COMMIT3Response commit(XDR xdr, RpcInfo info) {
    SecurityHandler securityHandler = getSecurityHandler(info);
    RpcCall rpcCall = (RpcCall) info.header();
    int xid = rpcCall.getXid();
    SocketAddress remoteAddress = info.remoteAddress();
    return commit(xdr, info.channel(), xid, securityHandler, remoteAddress);
  }

  @VisibleForTesting
  COMMIT3Response commit(XDR xdr, Channel channel, int xid,
      SecurityHandler securityHandler, SocketAddress remoteAddress) {
    COMMIT3Response response = new COMMIT3Response(Nfs3Status.NFS3_OK);

    COMMIT3Request request;
    try {
      request = COMMIT3Request.deserialize(xdr);
    } catch (IOException e) {
      LOG.error("Invalid COMMIT request");
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    FileHandle handle = request.getHandle();
    int namenodeId = handle.getNamenodeId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS COMMIT fileHandle: {} offset={} count={} client: {}",
          handle.dumpFileHandle(), request.getOffset(), request.getCount(),
          remoteAddress);
    }
    DFSClient dfsClient =
        clientCache.getDfsClient(securityHandler.getUser(), namenodeId);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }

    String fileIdPath = Nfs3Utils.getFileIdPath(handle);
    Nfs3FileAttributes preOpAttr = null;
    try {
      preOpAttr = Nfs3Utils.getFileAttr(dfsClient, fileIdPath, iug);
      if (preOpAttr == null) {
        LOG.info("Can't get path for fileId: {}", handle.getFileId());
        return new COMMIT3Response(Nfs3Status.NFS3ERR_STALE);
      }

      if (!checkAccessPrivilege(remoteAddress, AccessPrivilege.READ_WRITE)) {
        return new COMMIT3Response(Nfs3Status.NFS3ERR_ACCES, new WccData(
            Nfs3Utils.getWccAttr(preOpAttr), preOpAttr),
            Nfs3Constant.WRITE_COMMIT_VERF);
      }

      long commitOffset = (request.getCount() == 0) ? 0
          : (request.getOffset() + request.getCount());

      // Insert commit as an async request
      writeManager.handleCommit(dfsClient, handle, commitOffset, channel, xid,
          preOpAttr, namenodeId);
      return null;
    } catch (IOException e) {
      LOG.warn("Exception", e);
      Nfs3FileAttributes postOpAttr = null;
      try {
        postOpAttr = writeManager.getFileAttr(dfsClient, handle, iug);
      } catch (IOException e1) {
        LOG.info("Can't get postOpAttr for fileId: {}", handle.getFileId(), e1);
      }

      WccData fileWcc = new WccData(Nfs3Utils.getWccAttr(preOpAttr), postOpAttr);
      int status = mapErrorStatus(e);
      return new COMMIT3Response(status, fileWcc,
          Nfs3Constant.WRITE_COMMIT_VERF);
    }
  }

  private SecurityHandler getSecurityHandler(Credentials credentials,
      Verifier verifier) {
    if (credentials instanceof CredentialsSys) {
      return new SysSecurityHandler((CredentialsSys) credentials, iug);
    } else {
      // TODO: support GSS and handle other cases
      return null;
    }
  }

  private SecurityHandler getSecurityHandler(RpcInfo info) {
    RpcCall rpcCall = (RpcCall) info.header();
    return getSecurityHandler(rpcCall.getCredential(), rpcCall.getVerifier());
  }

  @Override
  public void handleInternal(ChannelHandlerContext ctx, RpcInfo info) {
    RpcCall rpcCall = (RpcCall) info.header();
    final NFSPROC3 nfsproc3 = NFSPROC3.fromValue(rpcCall.getProcedure());    
    int xid = rpcCall.getXid();
    byte[] data = new byte[info.data().readableBytes()];
    info.data().readBytes(data);
    XDR xdr = new XDR(data);
    XDR out = new XDR();
    InetAddress client = ((InetSocketAddress) info.remoteAddress())
        .getAddress();
    Credentials credentials = rpcCall.getCredential();

    // Ignore auth only for NFSPROC3_NULL, especially for Linux clients.
    if (nfsproc3 != NFSPROC3.NULL) {
      if (credentials.getFlavor() != AuthFlavor.AUTH_SYS
          && credentials.getFlavor() != AuthFlavor.RPCSEC_GSS) {
        LOG.info("Wrong RPC AUTH flavor, {} is not AUTH_SYS or RPCSEC_GSS.",
            credentials.getFlavor());
        XDR reply = new XDR();
        RpcDeniedReply rdr = new RpcDeniedReply(xid,
            RpcReply.ReplyState.MSG_ACCEPTED,
            RpcDeniedReply.RejectState.AUTH_ERROR, new VerifierNone());
        rdr.write(reply);

        ChannelBuffer buf = ChannelBuffers.wrappedBuffer(reply.asReadOnlyWrap()
            .buffer());
        RpcResponse rsp = new RpcResponse(buf, info.remoteAddress());
        RpcUtil.sendRpcResponse(ctx, rsp);
        return;
      }
    }

    if (!isIdempotent(rpcCall)) {
      RpcCallCache.CacheEntry entry = rpcCallCache.checkOrAddToCache(client,
          xid);
      if (entry != null) { // in cache
        if (entry.isCompleted()) {
          LOG.info("Sending the cached reply to retransmitted request {}",
              xid);
          RpcUtil.sendRpcResponse(ctx, entry.getResponse());
          return;
        } else { // else request is in progress
          LOG.info("Retransmitted request, transaction still in progress {}",
              xid);
          // Ignore the request and do nothing
          return;
        }
      }
    }
    
    // Since write and commit could be async, they use their own startTime and
    // only record success requests.
    final long startTime = System.nanoTime();
    
    NFS3Response response = null;
    if (nfsproc3 == NFSPROC3.NULL) {
      response = nullProcedure();
    } else if (nfsproc3 == NFSPROC3.GETATTR) {
      response = getattr(xdr, info);
      metrics.addGetattr(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.SETATTR) {
      response = setattr(xdr, info);
      metrics.addSetattr(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.LOOKUP) {
      response = lookup(xdr, info);
      metrics.addLookup(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.ACCESS) {
      response = access(xdr, info);
      metrics.addAccess(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.READLINK) {
      response = readlink(xdr, info);
      metrics.addReadlink(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.READ) {
      LOG.debug("{}{}", Nfs3Utils.READ_RPC_START, xid);
      response = read(xdr, info);
      LOG.debug("{}{}", Nfs3Utils.READ_RPC_END, xid);
      metrics.addRead(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.WRITE) {
      LOG.debug("{}{}", Nfs3Utils.WRITE_RPC_START, xid);
      response = write(xdr, info);
      // Write end debug trace is in Nfs3Utils.writeChannel
    } else if (nfsproc3 == NFSPROC3.CREATE) {
      response = create(xdr, info);
      metrics.addCreate(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.MKDIR) {
      response = mkdir(xdr, info);
      metrics.addMkdir(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.SYMLINK) {
      response = symlink(xdr, info);
      metrics.addSymlink(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.MKNOD) {
      response = mknod(xdr, info);
      metrics.addMknod(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.REMOVE) {
      response = remove(xdr, info);
      metrics.addRemove(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.RMDIR) {
      response = rmdir(xdr, info);
      metrics.addRmdir(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.RENAME) {
      response = rename(xdr, info);
      metrics.addRename(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.LINK) {
      response = link(xdr, info);
      metrics.addLink(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.READDIR) {
      response = readdir(xdr, info);
      metrics.addReaddir(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.READDIRPLUS) {
      response = readdirplus(xdr, info);
      metrics.addReaddirplus(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.FSSTAT) {
      response = fsstat(xdr, info);
      metrics.addFsstat(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.FSINFO) {
      response = fsinfo(xdr, info);
      metrics.addFsinfo(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.PATHCONF) {
      response = pathconf(xdr, info);
      metrics.addPathconf(Nfs3Utils.getElapsedTime(startTime));
    } else if (nfsproc3 == NFSPROC3.COMMIT) {
      response = commit(xdr, info);
    } else {
      // Invalid procedure
      RpcAcceptedReply.getInstance(xid,
          RpcAcceptedReply.AcceptState.PROC_UNAVAIL, new VerifierNone()).write(
          out);
    }
    if (response == null) {
      LOG.debug("No sync response, expect an async response for request XID={}",
          rpcCall.getXid());
      return;
    }
    // TODO: currently we just return VerifierNone
    out = response.serialize(out, xid, new VerifierNone());
    ChannelBuffer buf = ChannelBuffers.wrappedBuffer(out.asReadOnlyWrap()
        .buffer());
    RpcResponse rsp = new RpcResponse(buf, info.remoteAddress());

    if (!isIdempotent(rpcCall)) {
      rpcCallCache.callCompleted(client, xid, rsp);
    }

    RpcUtil.sendRpcResponse(ctx, rsp);
  }

  @Override
  protected boolean isIdempotent(RpcCall call) {
    final NFSPROC3 nfsproc3 = NFSPROC3.fromValue(call.getProcedure());
    return nfsproc3 == null || nfsproc3.isIdempotent();
  }

  private boolean checkAccessPrivilege(SocketAddress remoteAddress,
      final AccessPrivilege expected) {
    // Port monitoring
    if (!doPortMonitoring(remoteAddress)) {
      return false;
    }

    // Check export table
    if (exports == null) {
        return false;
    }
    InetAddress client = ((InetSocketAddress) remoteAddress).getAddress();
    AccessPrivilege access = exports.getAccessPrivilege(client);
    if (access == AccessPrivilege.NONE) {
      return false;
    }
    if (access == AccessPrivilege.READ_ONLY
        && expected == AccessPrivilege.READ_WRITE) {
      return false;
    }
    return true;
  }

  @VisibleForTesting
  WriteManager getWriteManager() {
    return this.writeManager;
  }
}
