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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Arrays;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class INodeAttributeProvider {

  public static class AuthorizationContext {
    private String fsOwner;
    private String supergroup;
    private UserGroupInformation callerUgi;
    private INodeAttributes[] inodeAttrs;
    private INode[] inodes;
    private byte[][] pathByNameArr;
    private int snapshotId;
    private String path;
    private int ancestorIndex;
    private boolean doCheckOwner;
    private FsAction ancestorAccess;
    private FsAction parentAccess;
    private FsAction access;
    private FsAction subAccess;
    private boolean ignoreEmptyDir;
    private String operationName;
    private CallerContext callerContext;

    public String getFsOwner() {
      return fsOwner;
    }

    public void setFsOwner(String fsOwner) {
      this.fsOwner = fsOwner;
    }

    public String getSupergroup() {
      return supergroup;
    }

    public void setSupergroup(String supergroup) {
      this.supergroup = supergroup;
    }

    public UserGroupInformation getCallerUgi() {
      return callerUgi;
    }

    public void setCallerUgi(UserGroupInformation callerUgi) {
      this.callerUgi = callerUgi;
    }

    public INodeAttributes[] getInodeAttrs() {
      return inodeAttrs;
    }

    public void setInodeAttrs(INodeAttributes[] inodeAttrs) {
      this.inodeAttrs = inodeAttrs;
    }

    public INode[] getInodes() {
      return inodes;
    }

    public void setInodes(INode[] inodes) {
      this.inodes = inodes;
    }

    public byte[][] getPathByNameArr() {
      return pathByNameArr;
    }

    public void setPathByNameArr(byte[][] pathByNameArr) {
      this.pathByNameArr = pathByNameArr;
    }

    public int getSnapshotId() {
      return snapshotId;
    }

    public void setSnapshotId(int snapshotId) {
      this.snapshotId = snapshotId;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public int getAncestorIndex() {
      return ancestorIndex;
    }

    public void setAncestorIndex(int ancestorIndex) {
      this.ancestorIndex = ancestorIndex;
    }

    public boolean isDoCheckOwner() {
      return doCheckOwner;
    }

    public void setDoCheckOwner(boolean doCheckOwner) {
      this.doCheckOwner = doCheckOwner;
    }

    public FsAction getAncestorAccess() {
      return ancestorAccess;
    }

    public void setAncestorAccess(FsAction ancestorAccess) {
      this.ancestorAccess = ancestorAccess;
    }

    public FsAction getParentAccess() {
      return parentAccess;
    }

    public void setParentAccess(FsAction parentAccess) {
      this.parentAccess = parentAccess;
    }

    public FsAction getAccess() {
      return access;
    }

    public void setAccess(FsAction access) {
      this.access = access;
    }

    public FsAction getSubAccess() {
      return subAccess;
    }

    public void setSubAccess(FsAction subAccess) {
      this.subAccess = subAccess;
    }

    public boolean isIgnoreEmptyDir() {
      return ignoreEmptyDir;
    }

    public void setIgnoreEmptyDir(boolean ignoreEmptyDir) {
      this.ignoreEmptyDir = ignoreEmptyDir;
    }

    public String getOperationName() {
      return operationName;
    }

    public void setOperationName(String operationName) {
      this.operationName = operationName;
    }

    public CallerContext getCallerContext() {
      return callerContext;
    }

    public void setCallerContext(CallerContext callerContext) {
      this.callerContext = callerContext;
    }

    public static class Builder {
      private String fsOwner;
      private String supergroup;
      private UserGroupInformation callerUgi;
      private INodeAttributes[] inodeAttrs;
      private INode[] inodes;
      private byte[][] pathByNameArr;
      private int snapshotId;
      private String path;
      private int ancestorIndex;
      private boolean doCheckOwner;
      private FsAction ancestorAccess;
      private FsAction parentAccess;
      private FsAction access;
      private FsAction subAccess;
      private boolean ignoreEmptyDir;
      private String operationName;
      private CallerContext callerContext;

      public AuthorizationContext build() {
        return new AuthorizationContext(this);
      }

      public Builder fsOwner(String val) {
        this.fsOwner = val;
        return this;
      }

      public Builder supergroup(String val) {
        this.supergroup = val;
        return this;
      }

      public Builder callerUgi(UserGroupInformation val) {
        this.callerUgi = val;
        return this;
      }

      public Builder inodeAttrs(INodeAttributes[] val) {
        this.inodeAttrs = val;
        return this;
      }

      public Builder inodes(INode[] val) {
        this.inodes = val;
        return this;
      }

      public Builder pathByNameArr(byte[][] val) {
        this.pathByNameArr = val;
        return this;
      }

      public Builder snapshotId(int val) {
        this.snapshotId = val;
        return this;
      }

      public Builder path(String val) {
        this.path = val;
        return this;
      }

      public Builder ancestorIndex(int val) {
        this.ancestorIndex = val;
        return this;
      }

      public Builder doCheckOwner(boolean val) {
        this.doCheckOwner = val;
        return this;
      }

      public Builder ancestorAccess(FsAction val) {
        this.ancestorAccess = val;
        return this;
      }

      public Builder parentAccess(FsAction val) {
        this.parentAccess = val;
        return this;
      }

      public Builder access(FsAction val) {
        this.access = val;
        return this;
      }

      public Builder subAccess(FsAction val) {
        this.subAccess = val;
        return this;
      }

      public Builder ignoreEmptyDir(boolean val) {
        this.ignoreEmptyDir = val;
        return this;
      }

      public Builder operationName(String val) {
        this.operationName = val;
        return this;
      }

      public Builder callerContext(CallerContext val) {
        this.callerContext = val;
        return this;
      }
    }

    public AuthorizationContext(Builder builder) {
      this.setFsOwner(builder.fsOwner);
      this.setSupergroup(builder.supergroup);
      this.setCallerUgi(builder.callerUgi);
      this.setInodeAttrs(builder.inodeAttrs);
      this.setInodes(builder.inodes);
      this.setPathByNameArr(builder.pathByNameArr);
      this.setSnapshotId(builder.snapshotId);
      this.setPath(builder.path);
      this.setAncestorIndex(builder.ancestorIndex);
      this.setDoCheckOwner(builder.doCheckOwner);
      this.setAncestorAccess(builder.ancestorAccess);
      this.setParentAccess(builder.parentAccess);
      this.setAccess(builder.access);
      this.setSubAccess(builder.subAccess);
      this.setIgnoreEmptyDir(builder.ignoreEmptyDir);
      this.setOperationName(builder.operationName);
      this.setCallerContext(builder.callerContext);
    }

    @VisibleForTesting
    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      AuthorizationContext other = (AuthorizationContext)obj;
      return getFsOwner().equals(other.getFsOwner()) &&
          getSupergroup().equals(other.getSupergroup()) &&
          getCallerUgi().equals(other.getCallerUgi()) &&
          Arrays.deepEquals(getInodeAttrs(), other.getInodeAttrs()) &&
          Arrays.deepEquals(getInodes(), other.getInodes()) &&
          Arrays.deepEquals(getPathByNameArr(), other.getPathByNameArr()) &&
          getSnapshotId() == other.getSnapshotId() &&
          getPath().equals(other.getPath()) &&
          getAncestorIndex() == other.getAncestorIndex() &&
          isDoCheckOwner() == other.isDoCheckOwner() &&
          getAncestorAccess() == other.getAncestorAccess() &&
          getParentAccess() == other.getParentAccess() &&
          getAccess() == other.getAccess() &&
          getSubAccess() == other.getSubAccess() &&
          isIgnoreEmptyDir() == other.isIgnoreEmptyDir();
    }

    @Override
    public int hashCode() {
      assert false : "hashCode not designed";
      return 42; // any arbitrary constant will do
    }
  }

  /**
   * The AccessControlEnforcer allows implementations to override the
   * default File System permission checking logic enforced on a file system
   * object
   */
  public interface AccessControlEnforcer {

    /**
     * Checks permission on a file system object. Has to throw an Exception
     * if the filesystem object is not accessible by the calling Ugi.
     * @param fsOwner Filesystem owner (The Namenode user)
     * @param supergroup super user geoup
     * @param callerUgi UserGroupInformation of the caller
     * @param inodeAttrs Array of INode attributes for each path element in the
     *                   the path
     * @param inodes Array of INodes for each path element in the path
     * @param pathByNameArr Array of byte arrays of the LocalName
     * @param snapshotId the snapshotId of the requested path
     * @param path Path String
     * @param ancestorIndex Index of ancestor
     * @param doCheckOwner perform ownership check
     * @param ancestorAccess The access required by the ancestor of the path.
     * @param parentAccess The access required by the parent of the path.
     * @param access The access required by the path.
     * @param subAccess If path is a directory, It is the access required of
     *                  the path and all the sub-directories. If path is not a
     *                  directory, there should ideally be no effect.
     * @param ignoreEmptyDir Ignore permission checking for empty directory?
     * @deprecated use{@link #checkPermissionWithContext(AuthorizationContext)}}
     * instead
     * @throws AccessControlException
     */
    public abstract void checkPermission(String fsOwner, String supergroup,
        UserGroupInformation callerUgi, INodeAttributes[] inodeAttrs,
        INode[] inodes, byte[][] pathByNameArr, int snapshotId, String path,
        int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
        FsAction parentAccess, FsAction access, FsAction subAccess,
        boolean ignoreEmptyDir)
            throws AccessControlException;

    /**
     * Checks permission on a file system object. Has to throw an Exception
     * if the filesystem object is not accessessible by the calling Ugi.
     * @param authzContext an {@link AuthorizationContext} object encapsulating
     *                     the various parameters required to authorize an
     *                     operation.
     * @throws AccessControlException
     */
    default void checkPermissionWithContext(AuthorizationContext authzContext)
        throws AccessControlException {
      throw new AccessControlException("The authorization provider does not "
          + "implement the checkPermissionWithContext(AuthorizationContext) "
          + "API.");
    }
  }
  /**
   * Initialize the provider. This method is called at NameNode startup
   * time.
   */
  public abstract void start();

  /**
   * Shutdown the provider. This method is called at NameNode shutdown time.
   */
  public abstract void stop();

  @Deprecated
  String[] getPathElements(String path) {
    path = path.trim();
    if (path.charAt(0) != Path.SEPARATOR_CHAR) {
      throw new IllegalArgumentException("It must be an absolute path: " +
          path);
    }
    int numOfElements = StringUtils.countMatches(path, Path.SEPARATOR);
    if (path.length() > 1 && path.endsWith(Path.SEPARATOR)) {
      numOfElements--;
    }
    String[] pathElements = new String[numOfElements];
    int elementIdx = 0;
    int idx = 0;
    int found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    while (found > -1) {
      if (found > idx) {
        pathElements[elementIdx++] = path.substring(idx, found);
      }
      idx = found + 1;
      found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    }
    if (idx < path.length()) {
      pathElements[elementIdx] = path.substring(idx);
    }
    return pathElements;
  }

  @Deprecated
  public INodeAttributes getAttributes(String fullPath, INodeAttributes inode) {
    return getAttributes(getPathElements(fullPath), inode);
  }

  public abstract INodeAttributes getAttributes(String[] pathElements,
      INodeAttributes inode);

  public INodeAttributes getAttributes(byte[][] components,
      INodeAttributes inode) {
    String[] elements = new String[components.length];
    for (int i = 0; i < elements.length; i++) {
      elements[i] = DFSUtil.bytes2String(components[i]);
    }
    return getAttributes(elements, inode);
  }

  /**
   * Can be over-ridden by implementations to provide a custom Access Control
   * Enforcer that can provide an alternate implementation of the
   * default permission checking logic.
   * @param defaultEnforcer The Default AccessControlEnforcer
   * @return The AccessControlEnforcer to use
   */
  public AccessControlEnforcer getExternalAccessControlEnforcer(
      AccessControlEnforcer defaultEnforcer) {
    return defaultEnforcer;
  }
}
