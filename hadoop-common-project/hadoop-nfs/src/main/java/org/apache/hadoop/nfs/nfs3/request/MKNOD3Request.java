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
package org.apache.hadoop.nfs.nfs3.request;

import java.io.IOException;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes.Specdata3;
import org.apache.hadoop.oncrpc.XDR;

/**
 * MKNOD3 Request
 */
public class MKNOD3Request extends RequestWithHandle {
  private final String name;
  private int type;
  private SetAttr3 objAttr = null;
  private Specdata3 spec = null;

  public MKNOD3Request(FileHandle handle, String name, int type,
      SetAttr3 objAttr, Specdata3 spec) {
    super(handle);
    this.name = name;
    this.type = type;
    this.objAttr = objAttr;
    this.spec = spec;
  }

  public static MKNOD3Request deserialize(XDR xdr) throws IOException {
    FileHandle handle = readHandle(xdr);
    String name = xdr.readString();
    int type = xdr.readInt();
    SetAttr3 objAttr =  new SetAttr3();
    Specdata3 spec = null;
    if (type == NfsFileType.NFSCHR.toValue()
        || type == NfsFileType.NFSBLK.toValue()) {
      objAttr.deserialize(xdr);
      spec = new Specdata3(xdr.readInt(), xdr.readInt());
    } else if (type == NfsFileType.NFSSOCK.toValue()
        || type == NfsFileType.NFSFIFO.toValue()) {
      objAttr.deserialize(xdr);
    }
    return new MKNOD3Request(handle, name, type, objAttr, spec);
  }

  public String getName() {
    return name;
  }

  public int getType() {
    return type;
  }

  public SetAttr3 getObjAttr() {
    return objAttr;
  }

  public Specdata3 getSpec() {
    return spec;
  }

  @Override
  public void serialize(XDR xdr) {
    handle.serialize(xdr);
    xdr.writeInt(name.length());
    xdr.writeFixedOpaque(name.getBytes(Charsets.UTF_8), name.length());
    objAttr.serialize(xdr);
    if (spec != null) {
      xdr.writeInt(spec.getSpecdata1());
      xdr.writeInt(spec.getSpecdata2());
    }
  }
}
