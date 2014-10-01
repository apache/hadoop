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

import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * FSSTAT3 Response
 */
public class FSSTAT3Response extends NFS3Response {
  private Nfs3FileAttributes postOpAttr;

  // The total size, in bytes, of the file system.
  private final long tbytes;

  // The amount of free space, in bytes, in the file system.
  private final long fbytes;

  /*
   * The amount of free space, in bytes, available to the user identified by the
   * authentication information in the RPC. (This reflects space that is
   * reserved by the file system; it does not reflect any quota system
   * implemented by the server.)
   */
  private final long abytes;

  /*
   * The total number of file slots in the file system. (On a UNIX server, this
   * often corresponds to the number of inodes configured.)
   */
  private final long tfiles;

  /* The number of free file slots in the file system. */
  private final long ffiles;

  /*
   * The number of free file slots that are available to the user corresponding
   * to the authentication information in the RPC. (This reflects slots that are
   * reserved by the file system; it does not reflect any quota system
   * implemented by the server.)
   */
  private final long afiles;

  /*
   * A measure of file system volatility: this is the number of seconds for
   * which the file system is not expected to change. For a volatile, frequently
   * updated file system, this will be 0. For an immutable file system, such as
   * a CD-ROM, this would be the largest unsigned integer. For file systems that
   * are infrequently modified, for example, one containing local executable
   * programs and on-line documentation, a value corresponding to a few hours or
   * days might be used. The client may use this as a hint in tuning its cache
   * management. Note however, this measure is assumed to be dynamic and may
   * change at any time.
   */
  private final int invarsec;

  public FSSTAT3Response(int status) {
    this(status, null, 0, 0, 0, 0, 0, 0, 0);
  }

  public FSSTAT3Response(int status, Nfs3FileAttributes postOpAttr,
      long tbytes, long fbytes, long abytes, long tfiles, long ffiles,
      long afiles, int invarsec) {
    super(status);
    this.postOpAttr = postOpAttr;
    this.tbytes = tbytes;
    this.fbytes = fbytes;
    this.abytes = abytes;
    this.tfiles = tfiles;
    this.ffiles = ffiles;
    this.afiles = afiles;
    this.invarsec = invarsec;
  }

  public static FSSTAT3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    xdr.readBoolean();
    Nfs3FileAttributes postOpAttr = Nfs3FileAttributes.deserialize(xdr);
    long tbytes = 0;
    long fbytes = 0;
    long abytes = 0;
    long tfiles = 0;
    long ffiles = 0;
    long afiles = 0;
    int invarsec = 0;

    if (status == Nfs3Status.NFS3_OK) {
      tbytes = xdr.readHyper();
      fbytes = xdr.readHyper();
      abytes = xdr.readHyper();
      tfiles = xdr.readHyper();
      ffiles = xdr.readHyper();
      afiles = xdr.readHyper();
      invarsec = xdr.readInt();
    }
    return new FSSTAT3Response(status, postOpAttr, tbytes, fbytes, abytes,
        tfiles, ffiles, afiles, invarsec);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    out.writeBoolean(true);
    if (postOpAttr == null) {
      postOpAttr = new Nfs3FileAttributes();
    }
    postOpAttr.serialize(out);

    if (getStatus() == Nfs3Status.NFS3_OK) {
      out.writeLongAsHyper(tbytes);
      out.writeLongAsHyper(fbytes);
      out.writeLongAsHyper(abytes);
      out.writeLongAsHyper(tfiles);
      out.writeLongAsHyper(ffiles);
      out.writeLongAsHyper(afiles);
      out.writeInt(invarsec);
    }
    return out;

  }
}
