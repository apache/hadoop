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
package org.apache.hadoop.nfs.nfs3.response;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * READDIR3 Response
 */
public class READDIR3Response extends NFS3Response {
  private final Nfs3FileAttributes postOpDirAttr;
  private final long cookieVerf;
  private final DirList3 dirList;

  public static class Entry3 {
    private final long fileId;
    private final String name;
    private final long cookie;
    
    public Entry3(long fileId, String name, long cookie) {
      this.fileId = fileId;
      this.name = name;
      this.cookie = cookie;
    }

    long getFileId() {
      return fileId;
    }

    @VisibleForTesting
    public String getName() {
      return name;
    }

    long getCookie() {
      return cookie;
    }

    static Entry3 deserialzie(XDR xdr) {
      long fileId = xdr.readHyper();
      String name = xdr.readString();
      long cookie = xdr.readHyper();
      return new Entry3(fileId, name, cookie);
    }

    void seralize(XDR xdr) {
      xdr.writeLongAsHyper(getFileId());
      xdr.writeString(getName());
      xdr.writeLongAsHyper(getCookie());
    }
  }

  public static class DirList3 {
    final List<Entry3> entries;
    final boolean eof;
    
    public DirList3(Entry3[] entries, boolean eof) {
      this.entries = Collections.unmodifiableList(Arrays.asList(entries));
      this.eof = eof;
    }
    
    @VisibleForTesting
    public List<Entry3> getEntries() {
      return this.entries;
    }
  }

  public READDIR3Response(int status) {
    this(status, new Nfs3FileAttributes());
  }

  public READDIR3Response(int status, Nfs3FileAttributes postOpAttr) {
    this(status, postOpAttr, 0, null); 
  }

  public READDIR3Response(int status, Nfs3FileAttributes postOpAttr,
      final long cookieVerf, final DirList3 dirList) {
    super(status);
    this.postOpDirAttr = postOpAttr;
    this.cookieVerf = cookieVerf;
    this.dirList = dirList;
  }

  public Nfs3FileAttributes getPostOpAttr() {
    return postOpDirAttr;
  }

  public long getCookieVerf() {
    return cookieVerf;
  }

  public DirList3 getDirList() {
    return dirList;
  }

  public static READDIR3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    xdr.readBoolean();
    Nfs3FileAttributes postOpDirAttr = Nfs3FileAttributes.deserialize(xdr);
    long cookieVerf = 0;
    ArrayList<Entry3> entries = new ArrayList<Entry3>();
    DirList3 dirList = null;

    if (status == Nfs3Status.NFS3_OK) {
      cookieVerf = xdr.readHyper();
      while (xdr.readBoolean()) {
        Entry3 e =  Entry3.deserialzie(xdr);
        entries.add(e);
      }
      boolean eof = xdr.readBoolean();
      Entry3[] allEntries = new Entry3[entries.size()];
      entries.toArray(allEntries);
      dirList = new DirList3(allEntries, eof);
    }
    return new READDIR3Response(status, postOpDirAttr, cookieVerf, dirList);
  }

  @Override
  public XDR serialize(XDR xdr, int xid, Verifier verifier) {
    super.serialize(xdr, xid, verifier);
    xdr.writeBoolean(true); // Attributes follow
    postOpDirAttr.serialize(xdr);

    if (getStatus() == Nfs3Status.NFS3_OK) {
      xdr.writeLongAsHyper(cookieVerf);
      for (Entry3 e : dirList.entries) {
        xdr.writeBoolean(true); // Value follows
        e.seralize(xdr);
      }

      xdr.writeBoolean(false);
      xdr.writeBoolean(dirList.eof);
    }
    return xdr;
  }
}
