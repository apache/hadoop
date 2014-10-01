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

import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * PATHCONF3 Response
 */
public class PATHCONF3Response extends NFS3Response {
  private final Nfs3FileAttributes postOpAttr;

  /* The maximum number of hard links to an object. */
  private final int linkMax;

  /* The maximum length of a component of a filename. */
  private final int nameMax;

  /*
   * If TRUE, the server will reject any request that includes a name longer
   * than name_max with the error, NFS3ERR_NAMETOOLONG. If FALSE, any length
   * name over name_max bytes will be silently truncated to name_max bytes.
   */
  private final boolean noTrunc;

  /*
   * If TRUE, the server will reject any request to change either the owner or
   * the group associated with a file if the caller is not the privileged user.
   * (Uid 0.)
   */
  private final boolean chownRestricted;

  /*
   * If TRUE, the server file system does not distinguish case when interpreting
   * filenames.
   */
  private final boolean caseInsensitive;

  /*
   * If TRUE, the server file system will preserve the case of a name during a
   * CREATE, MKDIR, MKNOD, SYMLINK, RENAME, or LINK operation.
   */
  private final boolean casePreserving;

  public PATHCONF3Response(int status) {
    this(status, new Nfs3FileAttributes(), 0, 0, false, false, false, false);
  }

  public PATHCONF3Response(int status, Nfs3FileAttributes postOpAttr,
      int linkMax, int nameMax, boolean noTrunc, boolean chownRestricted,
      boolean caseInsensitive, boolean casePreserving) {
    super(status);
    this.postOpAttr = postOpAttr;
    this.linkMax = linkMax;
    this.nameMax = nameMax;
    this.noTrunc = noTrunc;
    this.chownRestricted = chownRestricted;
    this.caseInsensitive = caseInsensitive;
    this.casePreserving = casePreserving;
  }

  public static PATHCONF3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    xdr.readBoolean();
    Nfs3FileAttributes objPostOpAttr = Nfs3FileAttributes.deserialize(xdr);
    int linkMax = 0;
    int nameMax = 0;
    boolean noTrunc = false;
    boolean chownRestricted = false;
    boolean caseInsensitive = false;
    boolean casePreserving = false;

    if (status == Nfs3Status.NFS3_OK) {
      linkMax = xdr.readInt();
      nameMax = xdr.readInt();
      noTrunc = xdr.readBoolean();
      chownRestricted = xdr.readBoolean();
      caseInsensitive = xdr.readBoolean();
      casePreserving = xdr.readBoolean();
    }
    return new PATHCONF3Response(status, objPostOpAttr, linkMax, nameMax,
        noTrunc, chownRestricted, caseInsensitive, casePreserving);
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    out.writeBoolean(true);
    postOpAttr.serialize(out);

    if (getStatus() == Nfs3Status.NFS3_OK) {
      out.writeInt(linkMax);
      out.writeInt(nameMax);
      out.writeBoolean(noTrunc);
      out.writeBoolean(chownRestricted);
      out.writeBoolean(caseInsensitive);
      out.writeBoolean(casePreserving);
    }
    return out;
  }
}
