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

import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * FSINFO3 Response
 */
public class FSINFO3Response extends NFS3Response {
  private final Nfs3FileAttributes postOpAttr;
  /*
   * The maximum size in bytes of a READ request supported by the server. Any
   * READ with a number greater than rtmax will result in a short read of rtmax
   * bytes or less.
   */
  private final int rtmax;
  /*
   * The preferred size of a READ request. This should be the same as rtmax
   * unless there is a clear benefit in performance or efficiency.
   */
  private final int rtpref;
  /* The suggested multiple for the size of a READ request. */
  private final int rtmult;
  /*
   * The maximum size of a WRITE request supported by the server. In general,
   * the client is limited by wtmax since there is no guarantee that a server
   * can handle a larger write. Any WRITE with a count greater than wtmax will
   * result in a short write of at most wtmax bytes.
   */
  private final int wtmax;
  /*
   * The preferred size of a WRITE request. This should be the same as wtmax
   * unless there is a clear benefit in performance or efficiency.
   */
  private final int wtpref;
  /*
   * The suggested multiple for the size of a WRITE request.
   */
  private final int wtmult;
  /* The preferred size of a READDIR request. */
  private final int dtpref;
  /* The maximum size of a file on the file system. */
  private final long maxFileSize;
  /*
   * The server time granularity. When setting a file time using SETATTR, the
   * server guarantees only to preserve times to this accuracy. If this is {0,
   * 1}, the server can support nanosecond times, {0, 1000000} denotes
   * millisecond precision, and {1, 0} indicates that times are accurate only to
   * the nearest second.
   */
  private final NfsTime timeDelta;
  /*
   * A bit mask of file system properties. The following values are defined:
   * 
   * FSF_LINK If this bit is 1 (TRUE), the file system supports hard links.
   * 
   * FSF_SYMLINK If this bit is 1 (TRUE), the file system supports symbolic
   * links.
   * 
   * FSF_HOMOGENEOUS If this bit is 1 (TRUE), the information returned by
   * PATHCONF is identical for every file and directory in the file system. If
   * it is 0 (FALSE), the client should retrieve PATHCONF information for each
   * file and directory as required.
   * 
   * FSF_CANSETTIME If this bit is 1 (TRUE), the server will set the times for a
   * file via SETATTR if requested (to the accuracy indicated by time_delta). If
   * it is 0 (FALSE), the server cannot set times as requested.
   */
  private final int properties;

  public FSINFO3Response(int status) {
    this(status, new Nfs3FileAttributes(), 0, 0, 0, 0, 0, 0, 0, 0, null, 0);
  }

  public FSINFO3Response(int status, Nfs3FileAttributes postOpAttr, int rtmax,
      int rtpref, int rtmult, int wtmax, int wtpref, int wtmult, int dtpref,
      long maxFileSize, NfsTime timeDelta, int properties) {
    super(status);
    this.postOpAttr = postOpAttr;
    this.rtmax = rtmax;
    this.rtpref = rtpref;
    this.rtmult = rtmult;
    this.wtmax = wtmax;
    this.wtpref = wtpref;
    this.wtmult = wtmult;
    this.dtpref = dtpref;
    this.maxFileSize = maxFileSize;
    this.timeDelta = timeDelta;
    this.properties = properties;
  }

  public static FSINFO3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    xdr.readBoolean();
    Nfs3FileAttributes postOpObjAttr = Nfs3FileAttributes.deserialize(xdr);
    int rtmax = 0;
    int rtpref = 0;
    int rtmult = 0;
    int wtmax = 0;
    int wtpref = 0;
    int wtmult = 0;
    int dtpref = 0;
    long maxFileSize = 0;
    NfsTime timeDelta = null;
    int properties = 0;

    if (status == Nfs3Status.NFS3_OK) {
      rtmax = xdr.readInt();
      rtpref = xdr.readInt();
      rtmult = xdr.readInt();
      wtmax = xdr.readInt();
      wtpref = xdr.readInt();
      wtmult = xdr.readInt();
      dtpref = xdr.readInt();
      maxFileSize = xdr.readHyper();
      timeDelta = NfsTime.deserialize(xdr);
      properties = xdr.readInt();
    }
    return new FSINFO3Response(status, postOpObjAttr, rtmax, rtpref, rtmult,
        wtmax, wtpref, wtmult, dtpref, maxFileSize, timeDelta, properties);

  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    out.writeBoolean(true);
    postOpAttr.serialize(out);

    if (getStatus() == Nfs3Status.NFS3_OK) {
      out.writeInt(rtmax);
      out.writeInt(rtpref);
      out.writeInt(rtmult);
      out.writeInt(wtmax);
      out.writeInt(wtpref);
      out.writeInt(wtmult);
      out.writeInt(dtpref);
      out.writeLongAsHyper(maxFileSize);
      timeDelta.serialize(out);
      out.writeInt(properties);
   }
    return out;
  }
}
