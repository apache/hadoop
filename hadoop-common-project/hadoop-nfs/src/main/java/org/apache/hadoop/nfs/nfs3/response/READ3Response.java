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

import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

/**
 * READ3 Response
 */
public class READ3Response extends NFS3Response {
  private final Nfs3FileAttributes postOpAttr;
  private final int count; // The real bytes of read data
  private final boolean eof;
  private final ByteBuffer data;

  public READ3Response(int status) {
    this(status, new Nfs3FileAttributes(), 0, false, null);
  }
  
  public READ3Response(int status, Nfs3FileAttributes postOpAttr, int count,
      boolean eof, ByteBuffer data) {
    super(status);
    this.postOpAttr = postOpAttr;
    this.count = count;
    this.eof = eof;
    this.data = data;
  }

  public Nfs3FileAttributes getPostOpAttr() {
    return postOpAttr;
  }

  public int getCount() {
    return count;
  }

  public boolean isEof() {
    return eof;
  }

  public ByteBuffer getData() {
    return data;
  }

  public static READ3Response deserialize(XDR xdr) {
    int status = xdr.readInt();
    xdr.readBoolean();
    Nfs3FileAttributes postOpAttr = Nfs3FileAttributes.deserialize(xdr);
    int count = 0;
    boolean eof = false;
    byte[] data = new byte[0];

    if (status == Nfs3Status.NFS3_OK) {
      count = xdr.readInt();
      eof = xdr.readBoolean();
      int len = xdr.readInt();
      assert (len == count);
      data = xdr.readFixedOpaque(count);
    }

    return new READ3Response(status, postOpAttr, count, eof,
        ByteBuffer.wrap(data));
  }

  @Override
  public XDR serialize(XDR out, int xid, Verifier verifier) {
    super.serialize(out, xid, verifier);
    out.writeBoolean(true); // Attribute follows
    postOpAttr.serialize(out);

    if (getStatus() == Nfs3Status.NFS3_OK) {
      out.writeInt(count);
      out.writeBoolean(eof);
      out.writeInt(count);
      out.writeFixedOpaque(data.array(), count);
    }
    return out;
  }
}