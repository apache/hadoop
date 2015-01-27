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

import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.nfs.nfs3.OpenFileCtx.COMMIT_STATUS;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.COMMIT3Response;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.jboss.netty.channel.Channel;

import com.google.common.annotations.VisibleForTesting;

/**
 * Manage the writes and responds asynchronously.
 */
public class WriteManager {
  public static final Log LOG = LogFactory.getLog(WriteManager.class);

  private final NfsConfiguration config;
  private final IdMappingServiceProvider iug;
 
  private AsyncDataService asyncDataService;
  private boolean asyncDataServiceStarted = false;

  private final int maxStreams;
  private final boolean aixCompatMode;

  /**
   * The time limit to wait for accumulate reordered sequential writes to the
   * same file before the write is considered done.
   */
  private long streamTimeout;

  private final OpenFileCtxCache fileContextCache;

  static public class MultipleCachedStreamException extends IOException {
    private static final long serialVersionUID = 1L;

    public MultipleCachedStreamException(String msg) {
      super(msg);
    }
  }

  boolean addOpenFileStream(FileHandle h, OpenFileCtx ctx) {
    return fileContextCache.put(h, ctx);
  }
  
  WriteManager(IdMappingServiceProvider iug, final NfsConfiguration config,
      boolean aixCompatMode) {
    this.iug = iug;
    this.config = config;
    this.aixCompatMode = aixCompatMode;
    streamTimeout = config.getLong(NfsConfigKeys.DFS_NFS_STREAM_TIMEOUT_KEY,
        NfsConfigKeys.DFS_NFS_STREAM_TIMEOUT_DEFAULT);
    LOG.info("Stream timeout is " + streamTimeout + "ms.");
    if (streamTimeout < NfsConfigKeys.DFS_NFS_STREAM_TIMEOUT_MIN_DEFAULT) {
      LOG.info("Reset stream timeout to minimum value "
          + NfsConfigKeys.DFS_NFS_STREAM_TIMEOUT_MIN_DEFAULT + "ms.");
      streamTimeout = NfsConfigKeys.DFS_NFS_STREAM_TIMEOUT_MIN_DEFAULT;
    }
    maxStreams = config.getInt(NfsConfigKeys.DFS_NFS_MAX_OPEN_FILES_KEY,
        NfsConfigKeys.DFS_NFS_MAX_OPEN_FILES_DEFAULT);
    LOG.info("Maximum open streams is "+ maxStreams);
    this.fileContextCache = new OpenFileCtxCache(config, streamTimeout);
  }

  void startAsyncDataService() {
    if (asyncDataServiceStarted) {
      return;
    }
    fileContextCache.start();
    this.asyncDataService = new AsyncDataService();
    asyncDataServiceStarted = true;
  }

  void shutdownAsyncDataService() {
    if (!asyncDataServiceStarted) {
      return;
    }
    asyncDataServiceStarted = false;
    asyncDataService.shutdown();
    fileContextCache.shutdown();
  }

  void handleWrite(DFSClient dfsClient, WRITE3Request request, Channel channel,
      int xid, Nfs3FileAttributes preOpAttr) throws IOException {
    int count = request.getCount();
    byte[] data = request.getData().array();
    if (data.length < count) {
      WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_INVAL);
      Nfs3Utils.writeChannel(channel, response.serialize(
          new XDR(), xid, new VerifierNone()), xid);
      return;
    }

    FileHandle handle = request.getHandle();
    if (LOG.isDebugEnabled()) {
      LOG.debug("handleWrite " + request);
    }

    // Check if there is a stream to write
    FileHandle fileHandle = request.getHandle();
    OpenFileCtx openFileCtx = fileContextCache.get(fileHandle);
    if (openFileCtx == null) {
      LOG.info("No opened stream for fileId: " + fileHandle.getFileId());

      String fileIdPath = Nfs3Utils.getFileIdPath(fileHandle.getFileId());
      HdfsDataOutputStream fos = null;
      Nfs3FileAttributes latestAttr = null;
      try {
        int bufferSize = config.getInt(
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
        
        fos = dfsClient.append(fileIdPath, bufferSize,
            EnumSet.of(CreateFlag.APPEND), null, null);

        latestAttr = Nfs3Utils.getFileAttr(dfsClient, fileIdPath, iug);
      } catch (RemoteException e) {
        IOException io = e.unwrapRemoteException();
        if (io instanceof AlreadyBeingCreatedException) {
          LOG.warn("Can't append file: " + fileIdPath
              + ". Possibly the file is being closed. Drop the request: "
              + request + ", wait for the client to retry...");
          return;
        }
        throw e;
      } catch (IOException e) {
        LOG.error("Can't append to file: " + fileIdPath, e);
        if (fos != null) {
          fos.close();
        }
        WccData fileWcc = new WccData(Nfs3Utils.getWccAttr(preOpAttr),
            preOpAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO,
            fileWcc, count, request.getStableHow(),
            Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.serialize(
            new XDR(), xid, new VerifierNone()), xid);
        return;
      }

      // Add open stream
      String writeDumpDir = config.get(NfsConfigKeys.DFS_NFS_FILE_DUMP_DIR_KEY,
          NfsConfigKeys.DFS_NFS_FILE_DUMP_DIR_DEFAULT);
      openFileCtx = new OpenFileCtx(fos, latestAttr, writeDumpDir + "/"
          + fileHandle.getFileId(), dfsClient, iug, aixCompatMode, config);

      if (!addOpenFileStream(fileHandle, openFileCtx)) {
        LOG.info("Can't add new stream. Close it. Tell client to retry.");
        try {
          fos.close();
        } catch (IOException e) {
          LOG.error("Can't close stream for fileId: " + handle.getFileId(), e);
        }
        // Notify client to retry
        WccData fileWcc = new WccData(latestAttr.getWccAttr(), latestAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_JUKEBOX,
            fileWcc, 0, request.getStableHow(), Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel,
            response.serialize(new XDR(), xid, new VerifierNone()),
            xid);
        return;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Opened stream for appending file: " + fileHandle.getFileId());
      }
    }

    // Add write into the async job queue
    openFileCtx.receivedNewWrite(dfsClient, request, channel, xid,
        asyncDataService, iug);
    return;
  }

  // Do a possible commit before read request in case there is buffered data
  // inside DFSClient which has been flushed but not synced.
  int commitBeforeRead(DFSClient dfsClient, FileHandle fileHandle,
      long commitOffset) {
    int status;
    OpenFileCtx openFileCtx = fileContextCache.get(fileHandle);

    if (openFileCtx == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No opened stream for fileId: " + fileHandle.getFileId()
            + " commitOffset=" + commitOffset
            + ". Return success in this case.");
      }
      status = Nfs3Status.NFS3_OK;

    } else {
      // commit request triggered by read won't create pending comment obj
      COMMIT_STATUS ret = openFileCtx.checkCommit(dfsClient, commitOffset,
          null, 0, null, true);
      switch (ret) {
      case COMMIT_FINISHED:
      case COMMIT_INACTIVE_CTX:
        status = Nfs3Status.NFS3_OK;
        break;
      case COMMIT_INACTIVE_WITH_PENDING_WRITE:
      case COMMIT_ERROR:
        status = Nfs3Status.NFS3ERR_IO;
        break;
      case COMMIT_WAIT:
      case COMMIT_SPECIAL_WAIT:
        /**
         * This should happen rarely in some possible cases, such as read
         * request arrives before DFSClient is able to quickly flush data to DN,
         * or Prerequisite writes is not available. Won't wait since we don't
         * want to block read.
         */     
        status = Nfs3Status.NFS3ERR_JUKEBOX;
        break;
      case COMMIT_SPECIAL_SUCCESS:
        // Read beyond eof could result in partial read
        status = Nfs3Status.NFS3_OK;
        break;
      default:
        LOG.error("Should not get commit return code: " + ret.name());
        throw new RuntimeException("Should not get commit return code: "
            + ret.name());
      }
    }
    return status;
  }
  
  void handleCommit(DFSClient dfsClient, FileHandle fileHandle,
      long commitOffset, Channel channel, int xid, Nfs3FileAttributes preOpAttr) {
    long startTime = System.nanoTime();
    int status;
    OpenFileCtx openFileCtx = fileContextCache.get(fileHandle);

    if (openFileCtx == null) {
      LOG.info("No opened stream for fileId: " + fileHandle.getFileId()
          + " commitOffset=" + commitOffset + ". Return success in this case.");
      status = Nfs3Status.NFS3_OK;
      
    } else {
      COMMIT_STATUS ret = openFileCtx.checkCommit(dfsClient, commitOffset,
          channel, xid, preOpAttr, false);
      switch (ret) {
      case COMMIT_FINISHED:
      case COMMIT_INACTIVE_CTX:
        status = Nfs3Status.NFS3_OK;
        break;
      case COMMIT_INACTIVE_WITH_PENDING_WRITE:
      case COMMIT_ERROR:
        status = Nfs3Status.NFS3ERR_IO;
        break;
      case COMMIT_WAIT:
        // Do nothing. Commit is async now.
        return;
      case COMMIT_SPECIAL_WAIT:
        status = Nfs3Status.NFS3ERR_JUKEBOX;
        break;
      case COMMIT_SPECIAL_SUCCESS:
        status = Nfs3Status.NFS3_OK;
        break;
      default:
        LOG.error("Should not get commit return code: " + ret.name());
        throw new RuntimeException("Should not get commit return code: "
            + ret.name());
      }
    }
    
    // Send out the response
    Nfs3FileAttributes postOpAttr = null;
    try {
      postOpAttr = getFileAttr(dfsClient, new FileHandle(preOpAttr.getFileId()), iug);
    } catch (IOException e1) {
      LOG.info("Can't get postOpAttr for fileId: " + preOpAttr.getFileId(), e1);
    }
    WccData fileWcc = new WccData(Nfs3Utils.getWccAttr(preOpAttr), postOpAttr);
    COMMIT3Response response = new COMMIT3Response(status, fileWcc,
        Nfs3Constant.WRITE_COMMIT_VERF);
    RpcProgramNfs3.metrics.addCommit(Nfs3Utils.getElapsedTime(startTime));
    Nfs3Utils.writeChannelCommit(channel,
        response.serialize(new XDR(), xid, new VerifierNone()), xid);
  }

  /**
   * If the file is in cache, update the size based on the cached data size
   */
  Nfs3FileAttributes getFileAttr(DFSClient client, FileHandle fileHandle,
      IdMappingServiceProvider iug) throws IOException {
    String fileIdPath = Nfs3Utils.getFileIdPath(fileHandle);
    Nfs3FileAttributes attr = Nfs3Utils.getFileAttr(client, fileIdPath, iug);
    if (attr != null) {
      OpenFileCtx openFileCtx = fileContextCache.get(fileHandle);
      if (openFileCtx != null) {
        attr.setSize(openFileCtx.getNextOffset());
        attr.setUsed(openFileCtx.getNextOffset());
      }
    }
    return attr;
  }

  Nfs3FileAttributes getFileAttr(DFSClient client, FileHandle dirHandle,
      String fileName) throws IOException {
    String fileIdPath = Nfs3Utils.getFileIdPath(dirHandle) + "/" + fileName;
    Nfs3FileAttributes attr = Nfs3Utils.getFileAttr(client, fileIdPath, iug);

    if ((attr != null) && (attr.getType() == NfsFileType.NFSREG.toValue())) {
      OpenFileCtx openFileCtx = fileContextCache.get(new FileHandle(attr
          .getFileId()));

      if (openFileCtx != null) {
        attr.setSize(openFileCtx.getNextOffset());
        attr.setUsed(openFileCtx.getNextOffset());
      }
    }
    return attr;
  }

  @VisibleForTesting
  OpenFileCtxCache getOpenFileCtxCache() {
    return this.fileContextCache;
  }
}
