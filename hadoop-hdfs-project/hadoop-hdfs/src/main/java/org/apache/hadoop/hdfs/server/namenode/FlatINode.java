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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.util.LongBitFormat;

import java.io.IOException;

/**
 * In-memory representation of an INode.
 */
public final class FlatINode extends FlatObject {
  private FlatINode(ByteString data) {
    super(data);
  }

  private FlatINode(byte[] data) {
    super(data);
  }

  public static FlatINode wrap(byte[] data) {
    return new FlatINode(data);
  }

  public static FlatINode wrap(ByteString data) {
    return new FlatINode(data);
  }

  public enum Type {
    DIRECTORY,
    FILE,
    SYMLINK;

    private static final Type[] VALUES = Type.values();
    static Type fromValue(int v) { return VALUES[v]; }

    public int value() {
      return ordinal();
    }
  }

  private enum Header {
    TYPE(null, 2, 0),
    PERMISSION(TYPE, 10, 0),
    USER(PERMISSION, 20, 0),
    GROUP(USER, 20, 0);

    private final LongBitFormat BITS;

    Header(Header prev, int length, long min) {
      BITS = new LongBitFormat(name(), prev == null ? null : prev.BITS, length,
        min);
    }

    static int get(Header h, long bits) {
      return (int) h.BITS.retrieve(bits);
    }

    static long set(Header h, long old, int v) {
      return h.BITS.combine(v, old);
    }
  }

  public Type type() {
    return Type.fromValue(Header.get(Header.TYPE, header()));
  }

  public boolean isFile() {
    return type() == Type.FILE;
  }

  public boolean isDirectory() {
    return type() == Type.DIRECTORY;
  }

  public int userId() {
    return Header.get(Header.USER, header());
  }

  public int groupId() {
    return Header.get(Header.GROUP, header());
  }

  public PermissionStatus permissionStatus(StringMap ugid) {
    return new PermissionStatus(ugid.get(userId()), ugid.get(groupId()),
                                new FsPermission(permission()));
  }

  public short permission() {
    return (short) Header.get(Header.PERMISSION, header());
  }

  private long header() {
    return data.getLong(0);
  }

  long id() {
    return data.getLong(Encoding.SIZEOF_LONG);
  }

  long parentId() {
    return data.getLong(Encoding.SIZEOF_LONG * 2);
  }

  long atime() {
    return data.getLong(Encoding.SIZEOF_LONG * 3);
  }
  long mtime() {
    return data.getLong(Encoding.SIZEOF_LONG * 4);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("INode[");
    sb.append(isFile() ? "file" : "dir")
        .append(", id=" + id())
        .append("]");
    return sb.toString();
  }

  public static class Builder {
    private long header;
    private long id;
    private long parentId;
    private long atime;
    private long mtime;

    Builder type(Type type) {
      this.header = Header.set(Header.TYPE, header, type.value());
      return this;
    }

    Builder userId(int uid) {
      this.header = Header.set(Header.USER, header, uid);
      return this;
    }

    Builder groupId(int gid) {
      this.header = Header.set(Header.GROUP, header, gid);
      return this;
    }

    Builder permission(short perm) {
      this.header = Header.set(Header.PERMISSION, header, perm);
      return this;
    }

    Builder id(long id) {
      this.id = id;
      return this;
    }

    long id() {
      return id;
    }

    Builder parentId(long parentId) {
      this.parentId = parentId;
      return this;
    }

    Builder atime(long atime) {
      this.atime = atime;
      return this;
    }

    long atime() {
      return this.atime;
    }

    Builder mtime(long mtime) {
      this.mtime = mtime;
      return this;
    }

    Builder mergeFrom(FlatINode o) {
      header = o.header();
      id(o.id()).parentId(o.parentId()).mtime(o.mtime()).atime(o.atime());
      return this;
    }

    ByteString build() {
      Preconditions.checkState(id != 0);
      byte[] res = new byte[4 * Encoding.SIZEOF_LONG];
      CodedOutputStream o = CodedOutputStream.newInstance(res);
      try {
        o.writeFixed64NoTag(header);
        o.writeFixed64NoTag(id);
        o.writeFixed64NoTag(parentId);
        o.writeFixed64NoTag(atime);
        o.writeFixed64NoTag(mtime);
        o.flush();
      } catch (IOException ignored) {
      }
      return ByteString.copyFrom(res);
    }
  }
}
