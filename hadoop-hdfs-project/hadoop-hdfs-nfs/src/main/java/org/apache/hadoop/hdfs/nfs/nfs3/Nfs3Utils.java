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

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

/**
 * Utility/helper methods related to NFS
 */
public class Nfs3Utils {
  public final static String INODEID_PATH_PREFIX = "/.reserved/.inodes/";

  
  public final static String READ_RPC_START =  "READ_RPC_CALL_START____";
  public final static String READ_RPC_END =    "READ_RPC_CALL_END______";
  public final static String WRITE_RPC_START = "WRITE_RPC_CALL_START____";
  public final static String WRITE_RPC_END =   "WRITE_RPC_CALL_END______";
  
  public static String getFileIdPath(FileHandle handle) {
    return getFileIdPath(handle.getFileId());
  }

  public static String getFileIdPath(long fileId) {
    return INODEID_PATH_PREFIX + fileId;
  }

  public static HdfsFileStatus getFileStatus(DFSClient client, String fileIdPath)
      throws IOException {
    return client.getFileLinkInfo(fileIdPath);
  }

  public static Nfs3FileAttributes getNfs3FileAttrFromFileStatus(
      HdfsFileStatus fs, IdMappingServiceProvider iug) {
    /**
     * Some 32bit Linux client has problem with 64bit fileId: it seems the 32bit
     * client takes only the lower 32bit of the fileId and treats it as signed
     * int. When the 32th bit is 1, the client considers it invalid.
     */
    NfsFileType fileType = fs.isDir() ? NfsFileType.NFSDIR : NfsFileType.NFSREG;
    fileType = fs.isSymlink() ? NfsFileType.NFSLNK : fileType;
    int nlink = (fileType == NfsFileType.NFSDIR) ? fs.getChildrenNum() + 2 : 1;
    long size = (fileType == NfsFileType.NFSDIR) ? getDirSize(fs
        .getChildrenNum()) : fs.getLen();
    return new Nfs3FileAttributes(fileType, nlink,
        fs.getPermission().toShort(), iug.getUidAllowingUnknown(fs.getOwner()),
        iug.getGidAllowingUnknown(fs.getGroup()), size, 0 /* fsid */,
        fs.getFileId(), fs.getModificationTime(), fs.getAccessTime(),
        new Nfs3FileAttributes.Specdata3());
  }

  public static Nfs3FileAttributes getFileAttr(DFSClient client,
      String fileIdPath, IdMappingServiceProvider iug) throws IOException {
    HdfsFileStatus fs = getFileStatus(client, fileIdPath);
    return fs == null ? null : getNfs3FileAttrFromFileStatus(fs, iug);
  }

  /**
   * HDFS directory size is always zero. Try to return something meaningful
   * here. Assume each child take 32bytes.
   */
  public static long getDirSize(int childNum) {
    return (childNum + 2) * 32;
  }

  public static WccAttr getWccAttr(DFSClient client, String fileIdPath)
      throws IOException {
    HdfsFileStatus fstat = getFileStatus(client, fileIdPath);
    if (fstat == null) {
      return null;
    }

    long size = fstat.isDir() ? getDirSize(fstat.getChildrenNum()) : fstat
        .getLen();
    return new WccAttr(size, new NfsTime(fstat.getModificationTime()),
        new NfsTime(fstat.getModificationTime()));
  }

  public static WccAttr getWccAttr(Nfs3FileAttributes attr) {
    return attr == null ? new WccAttr() : new WccAttr(attr.getSize(),
        attr.getMtime(), attr.getCtime());
  }

  // TODO: maybe not efficient
  public static WccData createWccData(final WccAttr preOpAttr,
      DFSClient dfsClient, final String fileIdPath,
      final IdMappingServiceProvider iug)
      throws IOException {
    Nfs3FileAttributes postOpDirAttr = getFileAttr(dfsClient, fileIdPath, iug);
    return new WccData(preOpAttr, postOpDirAttr);
  }

  /**
   * Send a write response to the netty network socket channel
   */
  public static void writeChannel(Channel channel, XDR out, int xid) {
    if (channel == null) {
      RpcProgramNfs3.LOG
          .info("Null channel should only happen in tests. Do nothing.");
      return;
    }
    
    if (RpcProgramNfs3.LOG.isDebugEnabled()) {
      RpcProgramNfs3.LOG.debug(WRITE_RPC_END + xid);
    }
    ChannelBuffer outBuf = XDR.writeMessageTcp(out, true);
    channel.write(outBuf);
  }
  
  public static void writeChannelCommit(Channel channel, XDR out, int xid) {
    if (RpcProgramNfs3.LOG.isDebugEnabled()) {
      RpcProgramNfs3.LOG.debug("Commit done:" + xid);
    }
    ChannelBuffer outBuf = XDR.writeMessageTcp(out, true);
    channel.write(outBuf);
  }

  private static boolean isSet(int access, int bits) {
    return (access & bits) == bits;
  }

  public static int getAccessRights(int mode, int type) {
    int rtn = 0;
    if (isSet(mode, Nfs3Constant.ACCESS_MODE_READ)) {
      rtn |= Nfs3Constant.ACCESS3_READ;
      // LOOKUP is only meaningful for dir
      if (type == NfsFileType.NFSDIR.toValue()) {
        rtn |= Nfs3Constant.ACCESS3_LOOKUP;
      }
    }
    if (isSet(mode, Nfs3Constant.ACCESS_MODE_WRITE)) {
      rtn |= Nfs3Constant.ACCESS3_MODIFY;
      rtn |= Nfs3Constant.ACCESS3_EXTEND;
      // Set delete bit, UNIX may ignore it for regular file since it's up to
      // parent dir op permission
      rtn |= Nfs3Constant.ACCESS3_DELETE;
    }
    if (isSet(mode, Nfs3Constant.ACCESS_MODE_EXECUTE)) {
      if (type == NfsFileType.NFSREG.toValue()) {
        rtn |= Nfs3Constant.ACCESS3_EXECUTE;
      } else {
        rtn |= Nfs3Constant.ACCESS3_LOOKUP;
      }
    }
    return rtn;
  }

  public static int getAccessRightsForUserGroup(int uid, int gid,
      int[] auxGids, Nfs3FileAttributes attr) {
    int mode = attr.getMode();
    if (uid == attr.getUid()) {
      return getAccessRights(mode >> 6, attr.getType());
    }
    if (gid == attr.getGid()) {
      return getAccessRights(mode >> 3, attr.getType());
    }
    // Check for membership in auxiliary groups
    if (auxGids != null) {
      for (int auxGid : auxGids) {
        if (attr.getGid() == auxGid) {
          return getAccessRights(mode >> 3, attr.getType());
        }
      }
    }
    return getAccessRights(mode, attr.getType());
  }
  
  public static long bytesToLong(byte[] data) {
    long n = 0xffL & data[0];
    for (int i = 1; i < 8; i++) {
      n = (n << 8) | (0xffL & data[i]);
    }
    return n;
  }

  public static byte[] longToByte(long v) {
    byte[] data = new byte[8];
    data[0] = (byte) (v >>> 56);
    data[1] = (byte) (v >>> 48);
    data[2] = (byte) (v >>> 40);
    data[3] = (byte) (v >>> 32);
    data[4] = (byte) (v >>> 24);
    data[5] = (byte) (v >>> 16);
    data[6] = (byte) (v >>> 8);
    data[7] = (byte) (v >>> 0);
    return data;
  }
  
  public static long getElapsedTime(long startTimeNano) {
    return System.nanoTime() - startTimeNano;
  }
}
