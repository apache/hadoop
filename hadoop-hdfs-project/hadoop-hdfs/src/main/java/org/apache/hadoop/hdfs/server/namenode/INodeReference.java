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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.visitor.NamespaceVisitor;
import org.apache.hadoop.security.AccessControlException;

/**
 * A reference to an inode.
 * <p>
 * This class and its subclasses are used to support multiple access paths.
 * A file/directory may have multiple access paths when it is stored in some
 * snapshots, and it is renamed/moved to other locations.
 * <p>
 * For example,
 * (1) Suppose we have /abc/foo and the inode is inode(id=1000,name=foo).
 *     Suppose foo is created after snapshot s0,
 *     i.e. foo is not in s0 and inode(id=1000,name=foo)
 *     is in the create-list of /abc for the s0 diff entry.
 * (2) Create snapshot s1, s2 for /abc, i.e. foo is in s1 and s2.
 *     Suppose sDst is the last snapshot /xyz.
 * (3) mv /abc/foo /xyz/bar, i.e. inode(id=1000,name=...) is renamed from "foo"
 *     to "bar" and its parent becomes /xyz.
 * <p>
 * Then, /xyz/bar, /abc/.snapshot/s1/foo and /abc/.snapshot/s2/foo
 * are different access paths to the same inode, inode(id=1000,name=bar).
 * Inside the inode tree, /abc/.snapshot/s1/foo and /abc/.snapshot/s2/foo
 * indeed have the same resolved path,
 * but /xyz/bar has a different resolved path.
 * <p>
 * With references, we have the following
 * - The source /abc/foo inode(id=1000,name=foo) is replaced with
 *   a WithName(name=foo,lastSnapshot=s2) and then it is moved
 *   to the delete-list of /abc for the s2 diff entry.
 *   The replacement also replaces inode(id=1000,name=foo)
 *   in the create-list of /abc for the s0 diff entry with the WithName.
 *   The same as before, /abc/foo is in s1 and s2, but not in s0.
 * - The destination /xyz adds a child DstReference(dstSnapshot=sDst).
 *   DstReference is added to the create-list of /xyz for the sDst diff entry.
 *   /xyz/bar is not in sDst.
 * - Both WithName and DstReference point to another reference WithCount(count=2).
 * - Finally, WithCount(count=2) points to inode(id=1000,name=bar)
 *   Note that the inode name is changed to "bar".
 * <p>
 * Note 1: References other than WithName use the name of the referred inode,
 *         i.e. WithCount and DstReference do not have their own name.
 * Note 2: getParent() always returns the parent in the current state, e.g.
 *         inode(id=1000,name=bar).getParent() returns /xyz but not /abc.
 * Note 3: {@link INodeReference#getId()} returns the id the referred inode,
 *         e.g. all WithName, DstReference and WithCount above return id=1000.
 */
public abstract class INodeReference extends INode {
  /** Assert the relationship this node and the references. */
  abstract void assertReferences();

  @Override
  public String toDetailString() {
    final String s = referred == null? null
        : referred.getFullPathAndObjectString();
    return super.toDetailString() + ", ->" + s;
  }

  /**
   * Try to remove the given reference and then return the reference count.
   * If the given inode is not a reference, return -1;
   */
  public static int tryRemoveReference(INode inode) {
    if (!inode.isReference()) {
      return -1;
    }
    return removeReference(inode.asReference());
  }

  /**
   * Remove the given reference and then return the reference count.
   * If the referred inode is not a WithCount, return -1;
   */
  private static int removeReference(INodeReference ref) {
    final INode referred = ref.getReferredINode();
    if (!(referred instanceof WithCount)) {
      return -1;
    }
    
    WithCount wc = (WithCount) referred;
    wc.removeReference(ref);
    return wc.getReferenceCount();
  }

  /**
   * When destroying a reference node (WithName or DstReference), we call this
   * method to identify the snapshot which is the latest snapshot before the
   * reference node's creation. 
   */
  static int getPriorSnapshot(INodeReference ref) {
    WithCount wc = (WithCount) ref.getReferredINode();
    WithName wn = null;
    if (ref instanceof DstReference) {
      wn = wc.getLastWithName();
    } else if (ref instanceof WithName) {
      wn = wc.getPriorWithName((WithName) ref);
    }
    if (wn != null) {
      INode referred = wc.getReferredINode();
      if (referred.isFile() && referred.asFile().isWithSnapshot()) {
        return referred.asFile().getDiffs().getPrior(wn.lastSnapshotId);
      } else if (referred.isDirectory()) {
        DirectoryWithSnapshotFeature sf = referred.asDirectory()
            .getDirectoryWithSnapshotFeature();
        if (sf != null) {
          return sf.getDiffs().getPrior(wn.lastSnapshotId);
        }
      }
    }
    return Snapshot.NO_SNAPSHOT_ID;
  }
  
  private INode referred;
  
  public INodeReference(INode parent, INode referred) {
    super(parent);
    this.referred = referred;
  }

  public final INode getReferredINode() {
    return referred;
  }

  @Override
  public final boolean isReference() {
    return true;
  }
  
  @Override
  public final INodeReference asReference() {
    return this;
  }

  @Override
  public final boolean isFile() {
    return referred.isFile();
  }
  
  @Override
  public final INodeFile asFile() {
    return referred.asFile();
  }
  
  @Override
  public final boolean isDirectory() {
    return referred.isDirectory();
  }
  
  @Override
  public final INodeDirectory asDirectory() {
    return referred.asDirectory();
  }
  
  @Override
  public final boolean isSymlink() {
    return referred.isSymlink();
  }
  
  @Override
  public final INodeSymlink asSymlink() {
    return referred.asSymlink();
  }

  @Override
  public byte[] getLocalNameBytes() {
    return referred.getLocalNameBytes();
  }

  @Override
  public void setLocalName(byte[] name) {
    referred.setLocalName(name);
  }

  @Override
  public final long getId() {
    return referred.getId();
  }
  
  @Override
  public final PermissionStatus getPermissionStatus(int snapshotId) {
    return referred.getPermissionStatus(snapshotId);
  }
  
  @Override
  public final String getUserName(int snapshotId) {
    return referred.getUserName(snapshotId);
  }
  
  @Override
  final void setUser(String user) {
    referred.setUser(user);
  }
  
  @Override
  public final String getGroupName(int snapshotId) {
    return referred.getGroupName(snapshotId);
  }
  
  @Override
  final void setGroup(String group) {
    referred.setGroup(group);
  }
  
  @Override
  public final FsPermission getFsPermission(int snapshotId) {
    return referred.getFsPermission(snapshotId);
  }

  @Override
  final AclFeature getAclFeature(int snapshotId) {
    return referred.getAclFeature(snapshotId);
  }

  @Override
  final void addAclFeature(AclFeature aclFeature) {
    referred.addAclFeature(aclFeature);
  }

  @Override
  final void removeAclFeature() {
    referred.removeAclFeature();
  }
  
  @Override
  final XAttrFeature getXAttrFeature(int snapshotId) {
    return referred.getXAttrFeature(snapshotId);
  }
  
  @Override
  final void addXAttrFeature(XAttrFeature xAttrFeature) {
    referred.addXAttrFeature(xAttrFeature);
  }
  
  @Override
  final void removeXAttrFeature() {
    referred.removeXAttrFeature();
  }

  @Override
  public final short getFsPermissionShort() {
    return referred.getFsPermissionShort();
  }
  
  @Override
  void setPermission(FsPermission permission) {
    referred.setPermission(permission);
  }

  @Override
  public long getPermissionLong() {
    return referred.getPermissionLong();
  }

  @Override
  public final long getModificationTime(int snapshotId) {
    return referred.getModificationTime(snapshotId);
  }
  
  @Override
  public final INode updateModificationTime(long mtime, int latestSnapshotId) {
    return referred.updateModificationTime(mtime, latestSnapshotId);
  }
  
  @Override
  public final void setModificationTime(long modificationTime) {
    referred.setModificationTime(modificationTime);
  }
  
  @Override
  public final long getAccessTime(int snapshotId) {
    return referred.getAccessTime(snapshotId);
  }
  
  @Override
  public final void setAccessTime(long accessTime) {
    referred.setAccessTime(accessTime);
  }

  @Override
  public final byte getStoragePolicyID() {
    return referred.getStoragePolicyID();
  }

  @Override
  public final byte getLocalStoragePolicyID() {
    return referred.getLocalStoragePolicyID();
  }

  @Override
  final void recordModification(int latestSnapshotId) {
    referred.recordModification(latestSnapshotId);
  }

  @Override // used by WithCount
  public void cleanSubtree(
      ReclaimContext reclaimContext, int snapshot, int prior) {
    referred.cleanSubtree(reclaimContext, snapshot, prior);
  }

  @Override // used by WithCount
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    if (removeReference(this) <= 0) {
      referred.destroyAndCollectBlocks(reclaimContext);
    }
  }

  @Override
  public ContentSummaryComputationContext computeContentSummary(int snapshotId,
      ContentSummaryComputationContext summary) throws AccessControlException {
    return referred.computeContentSummary(snapshotId, summary);
  }

  @Override
  public QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    return referred.computeQuotaUsage(bsps, blockStoragePolicyId, useCache,
        lastSnapshotId);
  }

  @Override
  public final INodeAttributes getSnapshotINode(int snapshotId) {
    return referred.getSnapshotINode(snapshotId);
  }

  @Override
  public QuotaCounts getQuotaCounts() {
    return referred.getQuotaCounts();
  }

  @Override
  public final void clear() {
    super.clear();
    referred = null;
  }

  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final int snapshot) {
    super.dumpTreeRecursively(out, prefix, snapshot);
    if (this instanceof DstReference) {
      out.print(", dstSnapshotId=" + ((DstReference) this).dstSnapshotId);
    }
    if (this instanceof WithCount) {
      out.print(", " + ((WithCount)this).getCountDetails());
    }
    out.println();
    
    final StringBuilder b = new StringBuilder();
    for(int i = 0; i < prefix.length(); i++) {
      b.append(' ');
    }
    b.append("->");
    getReferredINode().dumpTreeRecursively(out, b, snapshot);
  }

  @Override
  public void accept(NamespaceVisitor visitor, int snapshot) {
    visitor.visitReferenceRecursively(this, snapshot);
  }

  public int getDstSnapshotId() {
    return Snapshot.CURRENT_STATE_ID;
  }
  
  /** An anonymous reference with reference count. */
  public static class WithCount extends INodeReference {

    private final List<WithName> withNameList = new ArrayList<>();

    /**
     * Compare snapshot with IDs, where null indicates the current status thus
     * is greater than any non-null snapshot.
     */
    public static final Comparator<WithName> WITHNAME_COMPARATOR
        = new Comparator<WithName>() {
      @Override
      public int compare(WithName left, WithName right) {
        return left.lastSnapshotId - right.lastSnapshotId;
      }
    };
    
    public WithCount(INodeReference parent, INode referred) {
      super(parent, referred);
      Preconditions.checkArgument(!referred.isReference());
      Preconditions.checkArgument(parent == null);
      referred.setParentReference(this);

      INodeReferenceValidation.add(this, WithCount.class);
    }

    public String getCountDetails() {
      final StringBuilder b = new StringBuilder("[");
      if (!withNameList.isEmpty()) {
        final Iterator<WithName> i = withNameList.iterator();
        b.append(i.next().getNameDetails());
        for(; i.hasNext();) {
          b.append(", ").append(i.next().getNameDetails());
        }
      }
      b.append("]");
      return ", count=" + getReferenceCount() + ", names=" + b;
    }

    @Override
    public String toDetailString() {
      return super.toDetailString() + getCountDetails();
    }

    private void assertDstReference(INodeReference parentRef) {
      if (parentRef instanceof DstReference) {
        return;
      }
      throw new IllegalArgumentException("Unexpected non-DstReference:"
          + "\n  parentRef: " + parentRef.toDetailString()
          + "\n  withCount: " + this.toDetailString());
    }

    private void assertReferredINode(INodeReference ref, String name) {
      if (ref.getReferredINode() == this) {
        return;
      }
      throw new IllegalStateException("Inconsistent Reference:"
          + "\n  " + name + ": " + ref.toDetailString()
          + "\n  withCount: " + this.toDetailString());
    }

    @Override
    void assertReferences() {
      for(WithName withName : withNameList) {
        assertReferredINode(withName, " withName");
      }

      final INodeReference parentRef = getParentReference();
      if (parentRef != null) {
        assertDstReference(parentRef);
        assertReferredINode(parentRef, "parentRef");
      }
    }
    
    public int getReferenceCount() {
      int count = withNameList.size();
      if (getParentReference() != null) {
        count++;
      }
      return count;
    }

    /** Increment and then return the reference count. */
    public void addReference(INodeReference ref) {
      if (ref instanceof WithName) {
        WithName refWithName = (WithName) ref;
        int i = Collections.binarySearch(withNameList, refWithName,
            WITHNAME_COMPARATOR);
        Preconditions.checkState(i < 0);
        withNameList.add(-i - 1, refWithName);
      } else if (ref instanceof DstReference) {
        setParentReference(ref);
      }
    }

    private int search(WithName ref) {
      return Collections.binarySearch(withNameList, ref, WITHNAME_COMPARATOR);
    }

    /** Decrement and then return the reference count. */
    public void removeReference(INodeReference ref) {
      if (ref instanceof WithName) {
        final WithName withName = (WithName) ref;
        final int i = search(withName);
        if (i >= 0) {
          withNameList.remove(i);
          INodeReferenceValidation.remove(withName, WithName.class);
        }
      } else if (ref == getParentReference()) {
        setParent(null);
        INodeReferenceValidation.remove((DstReference) ref, DstReference.class);
      }

      if (getReferenceCount() == 0) {
        INodeReferenceValidation.remove(this, WithCount.class);
      }
    }

    /** Return the last WithName reference if there is any, null otherwise. */
    public WithName getLastWithName() {
      return withNameList.size() > 0 ? 
          withNameList.get(withNameList.size() - 1) : null;
    }
    
    WithName getPriorWithName(WithName post) {
      int i = Collections.binarySearch(withNameList, post, WITHNAME_COMPARATOR);
      if (i > 0) {
        return withNameList.get(i - 1);
      } else if (i == 0 || i == -1) {
        return null;
      } else {
        return withNameList.get(-i - 2);
      }
    }

    /**
     * @return the WithName/DstReference node contained in the given snapshot.
     */
    public INodeReference getParentRef(int snapshotId) {
      int start = 0;
      int end = withNameList.size() - 1;
      while (start < end) {
        int mid = start + (end - start) / 2;
        int sid = withNameList.get(mid).lastSnapshotId; 
        if (sid == snapshotId) {
          return withNameList.get(mid);
        } else if (sid < snapshotId) {
          start = mid + 1;
        } else {
          end = mid;
        }
      }
      if (start < withNameList.size() &&
          withNameList.get(start).lastSnapshotId >= snapshotId) {
        return withNameList.get(start);
      } else {
        return this.getParentReference();
      }
    }
  }
  
  /** A reference with a fixed name. */
  public static class WithName extends INodeReference {

    private final byte[] name;

    /**
     * The id of the last snapshot in the src tree when this WithName node was 
     * generated, i.e. this reference is in that snapshot.
     * <p>
     * When calculating the quota usage of the referred node, only
     * the files/dirs existing when this snapshot was taken will be counted for 
     * this WithName node and propagated along its ancestor path.
     */
    private final int lastSnapshotId;
    
    public WithName(INodeDirectory parent, WithCount referred, byte[] name,
        int lastSnapshotId) {
      super(parent, referred);
      this.name = name;
      this.lastSnapshotId = lastSnapshotId;
      referred.addReference(this);

      INodeReferenceValidation.add(this, WithName.class);
    }

    String getNameDetails() {
      return getClass().getSimpleName() + "[" + getLocalName()
          + ", lastSnapshot=" + lastSnapshotId + "]";
    }

    @Override
    void assertReferences() {
      final INode ref= getReferredINode();
      final String err;
      if (ref instanceof WithCount) {
        final WithCount withCount = (WithCount)ref;
        final int i = withCount.search(this);
        if (i >= 0) {
          if (withCount.withNameList.get(i) == this) {
            return;
          } else {
            err = "OBJECT MISMATCH, withNameList.get(" + i + ") != this";
          }
        } else {
          err = "NOT FOUND in withNameList";
        }
      } else {
        err = "UNEXPECTED CLASS, expecting WithCount";
      }

      throw new IllegalStateException(err + ":"
          + "\n  ref: " + (ref == null? null : ref.toDetailString())
          + "\n this: " + this.toDetailString());
    }

    @Override
    public final byte[] getLocalNameBytes() {
      return name;
    }

    @Override
    public final void setLocalName(byte[] name) {
      throw new UnsupportedOperationException("Cannot set name: " + getClass()
          + " is immutable.");
    }
    
    public int getLastSnapshotId() {
      return lastSnapshotId;
    }
    
    @Override
    public final ContentSummaryComputationContext computeContentSummary(
        int snapshotId, ContentSummaryComputationContext summary)
        throws AccessControlException {
      Preconditions.checkState(snapshotId == Snapshot.CURRENT_STATE_ID
          || this.lastSnapshotId >= snapshotId);
      final INode referred =
          this.getReferredINode().asReference().getReferredINode();
      int id = snapshotId != Snapshot.CURRENT_STATE_ID ? snapshotId :
          this.lastSnapshotId;
      return referred.computeContentSummary(id, summary);
    }

    @Override
    public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
        byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
      // if this.lastSnapshotId < lastSnapshotId, the rename of the referred
      // node happened before the rename of its ancestor. This should be
      // impossible since for WithName node we only count its children at the
      // time of the rename.
      Preconditions.checkState(lastSnapshotId == Snapshot.CURRENT_STATE_ID
          || this.lastSnapshotId >= lastSnapshotId);
      final INode referred = this.getReferredINode().asReference()
          .getReferredINode();
      // We will continue the quota usage computation using the same snapshot id
      // as time line (if the given snapshot id is valid). Also, we cannot use 
      // cache for the referred node since its cached quota may have already 
      // been updated by changes in the current tree.
      int id = lastSnapshotId != Snapshot.CURRENT_STATE_ID ? 
          lastSnapshotId : this.lastSnapshotId;
      return referred.computeQuotaUsage(bsps, blockStoragePolicyId, false, id);
    }
    
    @Override
    public void cleanSubtree(ReclaimContext reclaimContext, final int snapshot,
        int prior) {
      // since WithName node resides in deleted list acting as a snapshot copy,
      // the parameter snapshot must be non-null
      Preconditions.checkArgument(snapshot != Snapshot.CURRENT_STATE_ID);
      // if prior is NO_SNAPSHOT_ID, we need to check snapshot belonging to the
      // previous WithName instance
      if (prior == Snapshot.NO_SNAPSHOT_ID) {
        prior = getPriorSnapshot(this);
      }
      
      if (prior != Snapshot.NO_SNAPSHOT_ID
          && Snapshot.ID_INTEGER_COMPARATOR.compare(snapshot, prior) <= 0) {
        return;
      }

      // record the old quota delta
      QuotaCounts old = reclaimContext.quotaDelta().getCountsCopy();
      getReferredINode().cleanSubtree(reclaimContext, snapshot, prior);
      INodeReference ref = getReferredINode().getParentReference();
      if (ref != null) {
        QuotaCounts current = reclaimContext.quotaDelta().getCountsCopy();
        current.subtract(old);
        // we need to update the quota usage along the parent path from ref
        reclaimContext.quotaDelta().addUpdatePath(ref, current);
      }
      
      if (snapshot < lastSnapshotId) {
        // for a WithName node, when we compute its quota usage, we only count
        // in all the nodes existing at the time of the corresponding rename op.
        // Thus if we are deleting a snapshot before/at the snapshot associated 
        // with lastSnapshotId, we do not need to update the quota upwards.
        reclaimContext.quotaDelta().setCounts(old);
      }
    }

    @Override
    public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
      int snapshot = getSelfSnapshot();
      reclaimContext.quotaDelta().add(computeQuotaUsage(reclaimContext.bsps));
      if (removeReference(this) <= 0) {
        getReferredINode().destroyAndCollectBlocks(reclaimContext.getCopy());
      } else {
        int prior = getPriorSnapshot(this);
        INode referred = getReferredINode().asReference().getReferredINode();

        if (snapshot != Snapshot.NO_SNAPSHOT_ID) {
          if (prior != Snapshot.NO_SNAPSHOT_ID && snapshot <= prior) {
            // the snapshot to be deleted has been deleted while traversing 
            // the src tree of the previous rename operation. This usually 
            // happens when rename's src and dst are under the same 
            // snapshottable directory. E.g., the following operation sequence:
            // 1. create snapshot s1 on /test
            // 2. rename /test/foo/bar to /test/foo2/bar
            // 3. create snapshot s2 on /test
            // 4. rename foo2 again
            // 5. delete snapshot s2
            return;
          }
          ReclaimContext newCtx = reclaimContext.getCopy();
          referred.cleanSubtree(newCtx, snapshot, prior);
          INodeReference ref = getReferredINode().getParentReference();
          if (ref != null) {
            // we need to update the quota usage along the parent path from ref
            reclaimContext.quotaDelta().addUpdatePath(ref,
                newCtx.quotaDelta().getCountsCopy());
          }
        }
      }
    }
    
    private int getSelfSnapshot() {
      INode referred = getReferredINode().asReference().getReferredINode();
      int snapshot = Snapshot.NO_SNAPSHOT_ID;
      if (referred.isFile() && referred.asFile().isWithSnapshot()) {
        snapshot = referred.asFile().getDiffs().getPrior(lastSnapshotId);
      } else if (referred.isDirectory()) {
        DirectoryWithSnapshotFeature sf = referred.asDirectory()
            .getDirectoryWithSnapshotFeature();
        if (sf != null) {
          snapshot = sf.getDiffs().getPrior(lastSnapshotId);
        }
      }
      return snapshot;
    }
  }
  
  public static class DstReference extends INodeReference {
    /**
     * Record the latest snapshot of the dst subtree before the rename,
     * i.e. this reference is NOT in that snapshot.  For
     * later operations on the moved/renamed files/directories, if the latest
     * snapshot is after this dstSnapshot, changes will be recorded to the
     * latest snapshot. Otherwise changes will be recorded to the snapshot
     * belonging to the src of the rename.
     * 
     * {@link Snapshot#NO_SNAPSHOT_ID} means no dstSnapshot (e.g., src of the
     * first-time rename).
     */
    private final int dstSnapshotId;
    
    @Override
    public final int getDstSnapshotId() {
      return dstSnapshotId;
    }
    
    public DstReference(INodeDirectory parent, WithCount referred,
        final int dstSnapshotId) {
      super(parent, referred);
      this.dstSnapshotId = dstSnapshotId;
      referred.addReference(this);

      INodeReferenceValidation.add(this, DstReference.class);
    }

    String getDstDetails() {
      return getClass().getSimpleName() + "[" + getLocalName()
          + ", dstSnapshot=" + dstSnapshotId + "]";
    }

    @Override
    void assertReferences() {
      final INode ref = getReferredINode();
      final String err;
      if (ref instanceof WithCount) {
        if (ref.getParentReference() == this) {
          return;
        } else {
          err = "OBJECT MISMATCH, ref.getParentReference() != this";
        }
      } else {
        err = "UNEXPECTED CLASS, expecting WithCount";
      }

      throw new IllegalStateException(err + ":"
          + "\n  ref: " + (ref == null? null : ref.toDetailString())
          + "\n this: " + this.toDetailString());
    }
    
    @Override
    public void cleanSubtree(ReclaimContext reclaimContext, int snapshot,
        int prior) {
      if (snapshot == Snapshot.CURRENT_STATE_ID
          && prior == Snapshot.NO_SNAPSHOT_ID) {
        destroyAndCollectBlocks(reclaimContext);
      } else {
        // if prior is NO_SNAPSHOT_ID, we need to check snapshot belonging to 
        // the previous WithName instance
        if (prior == Snapshot.NO_SNAPSHOT_ID) {
          prior = getPriorSnapshot(this);
        }
        // if prior is not NO_SNAPSHOT_ID, and prior is not before the
        // to-be-deleted snapshot, we can quit here and leave the snapshot
        // deletion work to the src tree of rename
        if (snapshot != Snapshot.CURRENT_STATE_ID
            && prior != Snapshot.NO_SNAPSHOT_ID
            && Snapshot.ID_INTEGER_COMPARATOR.compare(snapshot, prior) <= 0) {
          return;
        }
        getReferredINode().cleanSubtree(reclaimContext, snapshot, prior);
      }
    }

    /**
     * When dstSnapshotId >= snapshotToBeDeleted,
     * this reference is not in snapshotToBeDeleted.
     * This reference should not be destroyed.
     *
     * @param context to {@link ReclaimContext#getSnapshotIdToBeDeleted()}
     */
    private void shouldDestroy(ReclaimContext context) {
      final int snapshotToBeDeleted = context.getSnapshotIdToBeDeleted();
      if (snapshotToBeDeleted == Snapshot.CURRENT_STATE_ID
          || snapshotToBeDeleted > dstSnapshotId) {
        return;
      }
      LOG.warn("Try to destroy a DstReference with dstSnapshotId = {}"
          + " >= snapshotToBeDeleted = {}", dstSnapshotId, snapshotToBeDeleted);
      LOG.warn("    dstRef: {}", toDetailString());
      final INode r = getReferredINode().asReference().getReferredINode();
      LOG.warn("  referred: {}", r.toDetailString());
    }

    /**
     * {@inheritDoc}
     * <br>
     * To destroy a DstReference node, we first remove its link with the 
     * referred node. If the reference number of the referred node is &lt;= 0,
     * we destroy the subtree of the referred node. Otherwise, we clean the
     * referred node's subtree and delete everything created after the last 
     * rename operation, i.e., everything outside of the scope of the prior 
     * WithName nodes.
     * @param reclaimContext
     */
    @Override
    public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
      shouldDestroy(reclaimContext);

      // since we count everything of the subtree for the quota usage of a
      // dst reference node, here we should just simply do a quota computation.
      // then to avoid double counting, we pass a different QuotaDelta to other
      // calls
      reclaimContext.quotaDelta().add(computeQuotaUsage(reclaimContext.bsps));
      ReclaimContext newCtx = reclaimContext.getCopy();

      if (removeReference(this) <= 0) {
        getReferredINode().destroyAndCollectBlocks(newCtx);
      } else {
        // we will clean everything, including files, directories, and 
        // snapshots, that were created after this prior snapshot
        int prior = getPriorSnapshot(this);
        // prior must be non-null, otherwise we do not have any previous 
        // WithName nodes, and the reference number will be 0.
        Preconditions.checkState(prior != Snapshot.NO_SNAPSHOT_ID);
        // identify the snapshot created after prior
        int snapshot = getSelfSnapshot(prior);
        
        INode referred = getReferredINode().asReference().getReferredINode();
        if (referred.isFile()) {
          // if referred is a file, it must be a file with snapshot since we did
          // recordModification before the rename
          INodeFile file = referred.asFile();
          Preconditions.checkState(file.isWithSnapshot());
          // make sure we mark the file as deleted
          file.getFileWithSnapshotFeature().deleteCurrentFile();
          // when calling cleanSubtree of the referred node, since we
          // compute quota usage updates before calling this destroy
          // function, we use true for countDiffChange
          referred.cleanSubtree(newCtx, snapshot, prior);
        } else if (referred.isDirectory()) {
          // similarly, if referred is a directory, it must be an
          // INodeDirectory with snapshot
          INodeDirectory dir = referred.asDirectory();
          Preconditions.checkState(dir.isWithSnapshot());
          DirectoryWithSnapshotFeature.destroyDstSubtree(newCtx, dir,
              snapshot, prior);
        }
      }
    }

    private int getSelfSnapshot(final int prior) {
      WithCount wc = (WithCount) getReferredINode().asReference();
      INode referred = wc.getReferredINode();
      int lastSnapshot = Snapshot.CURRENT_STATE_ID;
      if (referred.isFile() && referred.asFile().isWithSnapshot()) {
        lastSnapshot = referred.asFile().getDiffs().getLastSnapshotId();
      } else if (referred.isDirectory()) {
        DirectoryWithSnapshotFeature sf = referred.asDirectory()
            .getDirectoryWithSnapshotFeature();
        if (sf != null) {
          lastSnapshot = sf.getLastSnapshotId();
        }
      }
      if (lastSnapshot != Snapshot.CURRENT_STATE_ID && lastSnapshot != prior) {
        return lastSnapshot;
      } else {
        return Snapshot.CURRENT_STATE_ID;
      }
    }
  }
}
