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
package org.apache.hadoop.hdfs.web;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.ipc.RemoteException;
import org.mortbay.util.ajax.JSON;

/** JSON Utilities */
public class JsonUtil {
  private static final ThreadLocal<Map<String, Object>> jsonMap
      = new ThreadLocal<Map<String, Object>>() {
    @Override
    protected Map<String, Object> initialValue() {
      return new TreeMap<String, Object>();
    }

    @Override
    public Map<String, Object> get() {
      final Map<String, Object> m = super.get();
      m.clear();
      return m;
    }
  };

  /** Convert an exception object to a Json string. */
  public static String toJsonString(final Exception e) {
    final Map<String, Object> m = jsonMap.get();
    m.put("className", e.getClass().getName());
    m.put("message", e.getMessage());
    return JSON.toString(m);
  }

  /** Convert a Json map to a RemoteException. */
  public static RemoteException toRemoteException(final Map<String, Object> m) {
    final String className = (String)m.get("className");
    final String message = (String)m.get("message");
    return new RemoteException(className, message);
  }

  /** Convert a key-value pair to a Json string. */
  public static String toJsonString(final Object key, final Object value) {
    final Map<String, Object> m = jsonMap.get();
    m.put(key instanceof String ? (String) key : key.toString(), value);
    return JSON.toString(m);
  }

  /** Convert a FsPermission object to a string. */
  public static String toString(final FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  /** Convert a string to a FsPermission object. */
  public static FsPermission toFsPermission(final String s) {
    return new FsPermission(Short.parseShort(s, 8));
  }

  /** Convert a HdfsFileStatus object to a Json string. */
  public static String toJsonString(final HdfsFileStatus status) {
    final Map<String, Object> m = jsonMap.get();
    if (status == null) {
      m.put("isNull", true);
    } else {
      m.put("isNull", false);
      m.put("localName", status.getLocalName());
      m.put("isDir", status.isDir());
      m.put("isSymlink", status.isSymlink());
      if (status.isSymlink()) {
        m.put("symlink", status.getSymlink());
      }

      m.put("len", status.getLen());
      m.put("owner", status.getOwner());
      m.put("group", status.getGroup());
      m.put("permission", toString(status.getPermission()));
      m.put("accessTime", status.getAccessTime());
      m.put("modificationTime", status.getModificationTime());
      m.put("blockSize", status.getBlockSize());
      m.put("replication", status.getReplication());
    }
    return JSON.toString(m);
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> parse(String jsonString) {
    return (Map<String, Object>) JSON.parse(jsonString);
  }

  /** Convert a Json string to a HdfsFileStatus object. */
  public static HdfsFileStatus toFileStatus(final Map<String, Object> m) {
    if ((Boolean)m.get("isNull")) {
      return null;
    }

    final String localName = (String) m.get("localName");
    final boolean isDir = (Boolean) m.get("isDir");
    final boolean isSymlink = (Boolean) m.get("isSymlink");
    final byte[] symlink = isSymlink?
        DFSUtil.string2Bytes((String)m.get("symlink")): null;

    final long len = (Long) m.get("len");
    final String owner = (String) m.get("owner");
    final String group = (String) m.get("group");
    final FsPermission permission = toFsPermission((String) m.get("permission"));
    final long aTime = (Long) m.get("accessTime");
    final long mTime = (Long) m.get("modificationTime");
    final long blockSize = (Long) m.get("blockSize");
    final short replication = (short) (long) (Long) m.get("replication");
    return new HdfsFileStatus(len, isDir, replication, blockSize, mTime, aTime,
        permission, owner, group,
        symlink, DFSUtil.string2Bytes(localName));
  }
}