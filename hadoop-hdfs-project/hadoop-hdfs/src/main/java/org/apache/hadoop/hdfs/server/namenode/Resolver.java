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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;

class Resolver {
  enum Error {
    OK,
    NOT_FOUND,
    INVALID_PATH,
  }

  static final class Result {
    final FlatINodesInPath iip;

    final String src;
    // The offset that points to the last unresolved component.
    final int offset;
    final Error errorCode;

    private Result(
        ImmutableList<Map.Entry<ByteString, FlatINode>> iip,
        String src, int
        offset, final Error errorCode) {
      this.src = src;
      this.iip = new FlatINodesInPath(iip);
      this.offset = offset;
      this.errorCode = errorCode;
    }

//    byte[] getLastKey() {
//      return inodes.get(inodes.size() - 1).getKey();
//    }
    FlatINode getLastINode(int offset) {
      return iip.getLastINode(offset);
    }

    public boolean ok() {
      return errorCode == Error.OK;
    }

    public boolean invalidPath() {
      return errorCode == Error.INVALID_PATH;
    }

    public boolean notFound() {
      return errorCode == Error.NOT_FOUND;
    }

//    byte[] getLastComponent() { return Encoding.parseInodeKey(getLastKey())
//      .getValue(); }

    FlatINodesInPath inodesInPath() {
      return iip;
    }

    String path() {
      return iip.path();
    }
  }

  static Result resolve(Transaction tx, final String path)
      throws IOException {
    long parentId = INodeId.ROOT_INODE_ID;
    int offset = 0;
    ImmutableList.Builder<Map.Entry<ByteString, FlatINode>> paths = ImmutableList
      .builder();
    paths.add(newPathEntry(Encoding.INODE_ROOT,
                           tx.getINode(INodeId.ROOT_INODE_ID)));
    int end = path.length();
    while (end > 0 && path.charAt(end - 1) == '/') {
      --end;
    }
    while (offset < end) {
      if (path.charAt(offset) != '/') {
        return new Result(paths.build(), path, offset, Error.INVALID_PATH);
      }

      int next = path.indexOf('/', offset + 1);
      if (next == -1) {
        next = end;
      }
      String component = path.substring(offset + 1, next);
      long childId = tx.getChild(parentId, ByteBuffer.wrap(component.getBytes
          ()));
      if (childId == INodeId.INVALID_INODE_ID) {
        return new Result(paths.build(), path, offset, Error.NOT_FOUND);
      }
      FlatINode child = tx.getINode(childId);
      if (child == null) {
        return new Result(paths.build(), path, offset, Error.NOT_FOUND);
      }
      paths.add(newPathEntry(component, child));
      FlatINode inode = child;
      offset = next;
      parentId = inode.id();
    }

    return new Result(paths.build(), path, offset, Error.OK);
  }

  static Result resolveNoSymlink(Transaction tx, final String path)
      throws IOException {
    return resolve(tx, path);
  }

  public static Result resolveById(Transaction tx, long id) {
    throw new IllegalArgumentException("Unimplemented");
  }

//  public static Result getInodeById(Transaction tx, long id)
//    throws IOException {
//    byte[] inodeKey = Encoding.inodeIdKey(id);
//    byte[] realKey = tx.get(inodeKey);
//    if (realKey == null) {
//      return new Result(EMPTY_PATHS, 0, Error.NOT_FOUND);
//    } else {
//      byte[] value = tx.get(realKey);
//      assert value != null;
//      ImmutableList<Map.Entry<byte[], INode>> inodes = ImmutableList.of
//        (newPathEntry(realKey, value));
//      return new Result(inodes, 0, Error.OK);
//    }
//  }

  private static Map.Entry<ByteString, FlatINode> newPathEntry(
      ByteString key, FlatINode value) {
    return new AbstractMap.SimpleImmutableEntry<>(key, value);
  }

  static Map.Entry<ByteString, FlatINode> newPathEntry(String key,
      FlatINode value) {
    return newPathEntry(ByteString.copyFromUtf8(key), value);
  }
}
