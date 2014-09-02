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
package org.apache.hadoop.nfs.nfs3;

import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.oncrpc.XDR;

/**
 * File attrbutes reported in NFS.
 */
public class Nfs3FileAttributes {
  private int type;
  private int mode;
  private int nlink;
  private int uid;
  private int gid;
  private long size;
  private long used;
  private Specdata3 rdev;
  private long fsid;
  private long fileId;
  private NfsTime atime;
  private NfsTime mtime;
  private NfsTime ctime;

  /*
   * The interpretation of the two words depends on the type of file system
   * object. For a block special (NF3BLK) or character special (NF3CHR) file,
   * specdata1 and specdata2 are the major and minor device numbers,
   * respectively. (This is obviously a UNIX-specific interpretation.) For all
   * other file types, these two elements should either be set to 0 or the
   * values should be agreed upon by the client and server. If the client and
   * server do not agree upon the values, the client should treat these fields
   * as if they are set to 0.
   * <br>
   * For Hadoop, currently this field is always zero.
   */
  public static class Specdata3 {
    final int specdata1;
    final int specdata2;

    public Specdata3() {
      specdata1 = 0;
      specdata2 = 0;
    }
    
    public Specdata3(int specdata1, int specdata2) {
      this.specdata1 = specdata1;
      this.specdata2 = specdata2;
    }
    
    public int getSpecdata1() {
      return specdata1;
    }

    public int getSpecdata2() {
      return specdata2;
    }
    
    @Override
    public String toString() {
      return "(Specdata3: specdata1" + specdata1 + ", specdata2:" + specdata2
          + ")";
    }
  }
   
  public Nfs3FileAttributes() {
    this(NfsFileType.NFSREG, 0, (short)0, 0, 0, 0, 0, 0, 0, 0);
  }

  public Nfs3FileAttributes(NfsFileType nfsType, int nlink, short mode, int uid,
      int gid, long size, long fsid, long fileId, long mtime, long atime) {
    this.type = nfsType.toValue();
    this.mode = mode;
    this.nlink = (type == NfsFileType.NFSDIR.toValue()) ? (nlink + 2) : 1;
    this.uid = uid;
    this.gid = gid;
    this.size = size;
    if(type == NfsFileType.NFSDIR.toValue()) {
      this.size = getDirSize(nlink);
    }
    this.used = this.size;
    this.rdev = new Specdata3();
    this.fsid = fsid;
    this.fileId = fileId;
    this.mtime = new NfsTime(mtime);
    this.atime = atime != 0 ? new NfsTime(atime) : this.mtime;
    this.ctime = this.mtime;
  }
  
  public Nfs3FileAttributes(Nfs3FileAttributes other) {
    this.type = other.getType();
    this.mode = other.getMode();
    this.nlink = other.getNlink();
    this.uid = other.getUid();
    this.gid = other.getGid();
    this.size = other.getSize();
    this.used = other.getUsed();
    this.rdev = new Specdata3();
    this.fsid = other.getFsid();
    this.fileId = other.getFileId();
    this.mtime = new NfsTime(other.getMtime());
    this.atime = new NfsTime(other.getAtime());
    this.ctime = new NfsTime(other.getCtime());
  }

  public void serialize(XDR xdr) {
    xdr.writeInt(type);
    xdr.writeInt(mode);
    xdr.writeInt(nlink);
    xdr.writeInt(uid);
    xdr.writeInt(gid);
    xdr.writeLongAsHyper(size);
    xdr.writeLongAsHyper(used);
    xdr.writeInt(rdev.getSpecdata1());
    xdr.writeInt(rdev.getSpecdata2());
    xdr.writeLongAsHyper(fsid);
    xdr.writeLongAsHyper(fileId);
    atime.serialize(xdr);
    mtime.serialize(xdr);
    ctime.serialize(xdr);
  }
  
  public static Nfs3FileAttributes deserialize(XDR xdr) {
    Nfs3FileAttributes attr = new Nfs3FileAttributes();
    attr.type = xdr.readInt();
    attr.mode = xdr.readInt();
    attr.nlink = xdr.readInt();
    attr.uid = xdr.readInt();
    attr.gid = xdr.readInt();
    attr.size = xdr.readHyper();
    attr.used = xdr.readHyper();
    // Ignore rdev
    xdr.readInt();
    xdr.readInt();
    attr.rdev = new Specdata3();
    attr.fsid = xdr.readHyper();
    attr.fileId = xdr.readHyper();
    attr.atime = NfsTime.deserialize(xdr);
    attr.mtime = NfsTime.deserialize(xdr);
    attr.ctime = NfsTime.deserialize(xdr);
    return attr;
  }
  
  @Override
  public String toString() {
    return String.format("type:%d, mode:%d, nlink:%d, uid:%d, gid:%d, " + 
            "size:%d, used:%d, rdev:%s, fsid:%d, fileid:%d, atime:%s, " + 
            "mtime:%s, ctime:%s",
            type, mode, nlink, uid, gid, size, used, rdev, fsid, fileId, atime,
            mtime, ctime);
  }

  public int getNlink() {
    return nlink;
  }

  public long getUsed() {
    return used;
  }

  public long getFsid() {
    return fsid;
  }

  public long getFileId() {
    return fileId;
  }

  public NfsTime getAtime() {
    return atime;
  }

  public NfsTime getMtime() {
    return mtime;
  }

  public NfsTime getCtime() {
    return ctime;
  }

  public int getType() {
    return type;
  }
  
  public WccAttr getWccAttr() {
    return new WccAttr(size, mtime, ctime);
  }
  
  public long getSize() {
    return size;
  }
  
  public void setSize(long size) {
    this.size = size;
  }
  
  public void setUsed(long used) {
    this.used = used;
  }
  
  public int getMode() {
    return this.mode;
  }
  
  public int getUid() {
    return this.uid;
  }
  
  public int getGid() {
    return this.gid;
  }
  
  /**
   * HDFS directory size is always zero. Try to return something meaningful
   * here. Assume each child take 32bytes.
   */
  public static long getDirSize(int childNum) {
    return (childNum + 2) * 32;
  }
}
