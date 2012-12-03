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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

/**
 * Directories where taking snapshots is allowed.
 * 
 * Like other {@link INode} subclasses, this class is synchronized externally
 * by the namesystem and FSDirectory locks.
 */
@InterfaceAudience.Private
public class INodeDirectorySnapshottable extends INodeDirectoryWithQuota {
  static public INodeDirectorySnapshottable newInstance(
      final INodeDirectory dir, final int snapshotQuota) {
    long nsq = -1L;
    long dsq = -1L;

    if (dir instanceof INodeDirectoryWithQuota) {
      final INodeDirectoryWithQuota q = (INodeDirectoryWithQuota)dir;
      nsq = q.getNsQuota();
      dsq = q.getDsQuota();
    }
    return new INodeDirectorySnapshottable(nsq, dsq, dir, snapshotQuota);
  }

  /** Cast INode to INodeDirectorySnapshottable. */
  static public INodeDirectorySnapshottable valueOf(
      INode inode, String src) throws IOException {
    final INodeDirectory dir = INodeDirectory.valueOf(inode, src);
    if (!dir.isSnapshottable()) {
      throw new SnapshotException(
          "Directory is not a snapshottable directory: " + src);
    }
    return (INodeDirectorySnapshottable)dir;
  }
  
  public static Snapshot findLatestSnapshot(INode inode) {
    Snapshot latest = null;
    for(; inode != null; inode = inode.getParent()) {
      if (inode instanceof INodeDirectorySnapshottable) {
        final Snapshot s = ((INodeDirectorySnapshottable)inode).getLastSnapshot();
        if (Snapshot.ID_COMPARATOR.compare(latest, s) < 0) {
          latest = s;
        }
      }
    }
    return latest;
  }

  /** Snapshots of this directory in ascending order of snapshot id. */
  private final List<Snapshot> snapshots = new ArrayList<Snapshot>();
  /** Snapshots of this directory in ascending order of snapshot names. */
  private final List<Snapshot> snapshotsByNames = new ArrayList<Snapshot>();
  
  /**
   * @return {@link #snapshots}
   */
  @VisibleForTesting
  List<Snapshot> getSnapshots() {
    return snapshots;
  }

  /**
   * @return {@link #snapshotsByNames}
   */
  @VisibleForTesting
  List<Snapshot> getSnapshotsByNames() {
    return snapshotsByNames;
  }
  
  /** Number of snapshots allowed. */
  private int snapshotQuota;

  private INodeDirectorySnapshottable(long nsQuota, long dsQuota,
      INodeDirectory dir, final int snapshotQuota) {
    super(nsQuota, dsQuota, dir);
    setSnapshotQuota(snapshotQuota);
  }
  
  public int getNumSnapshots() {
    return snapshots.size();
  }
  
  private int searchSnapshot(byte[] snapshotName) {
    return Collections.binarySearch(snapshotsByNames, snapshotName);
  }

  /** @return the snapshot with the given name. */
  public Snapshot getSnapshot(byte[] snapshotName) {
    final int i = searchSnapshot(snapshotName);
    return i < 0? null: snapshotsByNames.get(i);
  }
  
  /**
   * Rename a snapshot
   * @param path
   *          The directory path where the snapshot was taken. Used for
   *          generating exception message.
   * @param oldName
   *          Old name of the snapshot
   * @param newName
   *          New name the snapshot will be renamed to
   * @throws SnapshotException
   *           Throw SnapshotException when either the snapshot with the old
   *           name does not exist or a snapshot with the new name already
   *           exists
   */
  public void renameSnapshot(String path, String oldName, String newName)
      throws SnapshotException {
    if (newName.equals(oldName)) {
      return;
    }
    final int indexOfOld = searchSnapshot(DFSUtil.string2Bytes(oldName));
    if (indexOfOld < 0) {
      throw new SnapshotException("The snapshot " + oldName
          + " does not exist for directory " + path);
    } else {
      int indexOfNew = searchSnapshot(DFSUtil.string2Bytes(newName));
      if (indexOfNew > 0) {
        throw new SnapshotException("The snapshot " + newName
            + " already exists for directory " + path);
      }
      // remove the one with old name from snapshotsByNames
      Snapshot snapshot = snapshotsByNames.remove(indexOfOld);
      INodeDirectoryWithSnapshot ssRoot = snapshot.getRoot();
      ssRoot.setLocalName(newName);
      indexOfNew = -indexOfNew - 1;
      if (indexOfNew <= indexOfOld) {
        snapshotsByNames.add(indexOfNew, snapshot);
      } else { // indexOfNew > indexOfOld
        snapshotsByNames.add(indexOfNew - 1, snapshot);
      }
    }
  }

  /** @return the last snapshot. */
  public Snapshot getLastSnapshot() {
    final int n = snapshots.size();
    return n == 0? null: snapshots.get(n - 1);
  }

  public int getSnapshotQuota() {
    return snapshotQuota;
  }

  public void setSnapshotQuota(int snapshotQuota) {
    if (snapshotQuota < 0) {
      throw new HadoopIllegalArgumentException(
          "Cannot set snapshot quota to " + snapshotQuota + " < 0");
    }
    this.snapshotQuota = snapshotQuota;
  }

  @Override
  public boolean isSnapshottable() {
    return true;
  }

  /** Add a snapshot. */
  Snapshot addSnapshot(int id, String name) throws SnapshotException {
    //check snapshot quota
    if (snapshots.size() + 1 > snapshotQuota) {
      throw new SnapshotException("Failed to add snapshot: there are already "
          + snapshots.size() + " snapshot(s) and the snapshot quota is "
          + snapshotQuota);
    }
    final Snapshot s = new Snapshot(id, name, this);
    final byte[] nameBytes = s.getRoot().getLocalNameBytes();
    final int i = searchSnapshot(nameBytes);
    if (i >= 0) {
      throw new SnapshotException("Failed to add snapshot: there is already a "
          + "snapshot with the same name \"" + name + "\".");
    }

    snapshots.add(s);
    snapshotsByNames.add(-i - 1, s);

    //set modification time
    final long timestamp = Time.now();
    s.getRoot().updateModificationTime(timestamp);
    updateModificationTime(timestamp);
    return s;
  }
  
  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix) {
    super.dumpTreeRecursively(out, prefix);

    out.print(prefix);
    out.print(snapshots.size());
    out.print(snapshots.size() <= 1 ? " snapshot of " : " snapshots of ");
    out.println(getLocalName());

    dumpTreeRecursively(out, prefix, new Iterable<INodeDirectoryWithSnapshot>() {
      @Override
      public Iterator<INodeDirectoryWithSnapshot> iterator() {
        return new Iterator<INodeDirectoryWithSnapshot>() {
          final Iterator<Snapshot> i = snapshots.iterator();

          @Override
          public boolean hasNext() {
            return i.hasNext();
          }

          @Override
          public INodeDirectoryWithSnapshot next() {
            return i.next().getRoot();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    });
  }
}
