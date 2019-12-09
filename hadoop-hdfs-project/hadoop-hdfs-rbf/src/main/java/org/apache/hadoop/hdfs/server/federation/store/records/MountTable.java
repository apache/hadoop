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
package org.apache.hadoop.hdfs.server.federation.store.records;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.RouterPermissionChecker;
import org.apache.hadoop.hdfs.server.federation.router.RouterQuotaUsage;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Data schema for
 * {@link org.apache.hadoop.hdfs.server.federation.store.
 * MountTableStore FederationMountTableStore} data stored in the
 * {@link org.apache.hadoop.hdfs.server.federation.store.
 * StateStoreService FederationStateStoreService}. Supports string
 * serialization.
 */
public abstract class MountTable extends BaseRecord {

  public static final String ERROR_MSG_NO_SOURCE_PATH =
      "Invalid entry, no source path specified ";
  public static final String ERROR_MSG_MUST_START_WITH_BACK_SLASH =
      "Invalid entry, all mount points must start with / ";
  public static final String ERROR_MSG_NO_DEST_PATH_SPECIFIED =
      "Invalid entry, no destination paths specified ";
  public static final String ERROR_MSG_INVAILD_DEST_NS =
      "Invalid entry, invalid destination nameservice ";
  public static final String ERROR_MSG_INVAILD_DEST_PATH =
      "Invalid entry, invalid destination path ";
  public static final String ERROR_MSG_ALL_DEST_MUST_START_WITH_BACK_SLASH =
      "Invalid entry, all destination must start with / ";

  /** Comparator for paths which considers the /. */
  public static final Comparator<String> PATH_COMPARATOR =
      new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          String s1 = o1.replace('/', ' ');
          String s2 = o2.replace('/', ' ');
          return s1.compareTo(s2);
        }
      };

  /** Comparator based on the mount table source. */
  public static final Comparator<MountTable> SOURCE_COMPARATOR =
      new Comparator<MountTable>() {
        public int compare(MountTable m1, MountTable m2) {
          String src1 = m1.getSourcePath();
          String src2 = m2.getSourcePath();
          return PATH_COMPARATOR.compare(src1, src2);
        }
      };


  /**
   * Default constructor for a mount table entry.
   */
  public MountTable() {
    super();
  }

  public static MountTable newInstance() {
    MountTable record = StateStoreSerializer.newRecord(MountTable.class);
    record.init();
    return record;
  }

  /**
   * Constructor for a mount table entry with a single destinations.
   *
   * @param src Source path in the mount entry.
   * @param destinations Nameservice destination of the mount point.
   * @param dateCreated Created date.
   * @param dateModified Modified date.
   * @throws IOException
   */
  public static MountTable newInstance(final String src,
      final Map<String, String> destinations,
      long dateCreated, long dateModified) throws IOException {

    MountTable record = newInstance(src, destinations);
    record.setDateCreated(dateCreated);
    record.setDateModified(dateModified);
    return record;
  }

  /**
   * Constructor for a mount table entry with multiple destinations.
   *
   * @param src Source path in the mount entry.
   * @param destinations Nameservice destinations of the mount point.
   * @throws IOException
   */
  public static MountTable newInstance(final String src,
      final Map<String, String> destinations) throws IOException {
    MountTable record = newInstance();

    // Normalize the mount path
    record.setSourcePath(normalizeFileSystemPath(src));

    // Build a list of remote locations
    final List<RemoteLocation> locations = new LinkedList<>();
    for (Entry<String, String> entry : destinations.entrySet()) {
      String nsId = entry.getKey();
      String path = normalizeFileSystemPath(entry.getValue());
      RemoteLocation location = new RemoteLocation(nsId, path, src);
      locations.add(location);
    }

    // Set the serialized dest string
    record.setDestinations(locations);

    // Set permission fields
    UserGroupInformation ugi = NameNode.getRemoteUser();
    record.setOwnerName(ugi.getShortUserName());
    String group = ugi.getGroups().isEmpty() ? ugi.getShortUserName()
        : ugi.getPrimaryGroupName();
    record.setGroupName(group);
    record.setMode(new FsPermission(
        RouterPermissionChecker.MOUNT_TABLE_PERMISSION_DEFAULT));

    // Set quota for mount table
    RouterQuotaUsage quota = new RouterQuotaUsage.Builder()
        .fileAndDirectoryCount(RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT)
        .quota(HdfsConstants.QUOTA_RESET)
        .spaceConsumed(RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT)
        .spaceQuota(HdfsConstants.QUOTA_RESET).build();
    record.setQuota(quota);

    // Validate
    record.validate();
    return record;
  }

  /**
   * Get source path in the federated namespace.
   *
   * @return Source path in the federated namespace.
   */
  public abstract String getSourcePath();

  /**
   * Set source path in the federated namespace.
   *
   * @param path Source path in the federated namespace.
   */
  public abstract void setSourcePath(String path);

  /**
   * Get a list of destinations (namespace + path) present for this entry.
   *
   * @return List of RemoteLocation destinations. Null if no destinations.
   */
  public abstract List<RemoteLocation> getDestinations();

  /**
   * Set the destination paths.
   *
   * @param paths Destination paths.
   */
  public abstract void setDestinations(List<RemoteLocation> dests);

  /**
   * Add a new destination to this mount table entry.
   */
  public abstract boolean addDestination(String nsId, String path);

  /**
   * Check if the entry is read only.
   *
   * @return If the entry is read only.
   */
  public abstract boolean isReadOnly();

  /**
   * Set an entry to be read only.
   *
   * @param ro If the entry is read only.
   */
  public abstract void setReadOnly(boolean ro);

  /**
   * Get the order of the destinations for this mount table entry.
   *
   * @return Order of the destinations.
   */
  public abstract DestinationOrder getDestOrder();

  /**
   * Set the order of the destinations for this mount table entry.
   *
   * @param order Order of the destinations.
   */
  public abstract void setDestOrder(DestinationOrder order);

  /**
   * Get owner name of this mount table entry.
   *
   * @return Owner name
   */
  public abstract String getOwnerName();

  /**
   * Set owner name of this mount table entry.
   *
   * @param owner Owner name for mount table entry
   */
  public abstract void setOwnerName(String owner);

  /**
   * Get group name of this mount table entry.
   *
   * @return Group name
   */
  public abstract String getGroupName();

  /**
   * Set group name of this mount table entry.
   *
   * @param group Group name for mount table entry
   */
  public abstract void setGroupName(String group);

  /**
   * Get permission of this mount table entry.
   *
   * @return FsPermission permission mode
   */
  public abstract FsPermission getMode();

  /**
   * Set permission for this mount table entry.
   *
   * @param mode Permission for mount table entry
   */
  public abstract void setMode(FsPermission mode);

  /**
   * Get quota of this mount table entry.
   *
   * @return RouterQuotaUsage quota usage
   */
  public abstract RouterQuotaUsage getQuota();

  /**
   * Set quota for this mount table entry.
   *
   * @param quota QuotaUsage for mount table entry
   */
  public abstract void setQuota(RouterQuotaUsage quota);

  /**
   * Get the default location.
   * @return The default location.
   */
  public RemoteLocation getDefaultLocation() {
    List<RemoteLocation> dests = this.getDestinations();
    if (dests == null || dests.isEmpty()) {
      return null;
    }
    return dests.get(0);
  }

  @Override
  public boolean like(final BaseRecord o) {
    if (o instanceof MountTable) {
      MountTable other = (MountTable)o;
      if (getSourcePath() != null &&
          !getSourcePath().equals(other.getSourcePath())) {
        return false;
      }
      if (getDestinations() != null &&
          !getDestinations().equals(other.getDestinations())) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getSourcePath());
    sb.append("->");
    List<RemoteLocation> destinations = this.getDestinations();
    sb.append(destinations);
    if (destinations != null && destinations.size() > 1) {
      sb.append("[" + this.getDestOrder() + "]");
    }
    if (this.isReadOnly()) {
      sb.append("[RO]");
    }

    if (this.getOwnerName() != null) {
      sb.append("[owner:").append(this.getOwnerName()).append("]");
    }

    if (this.getGroupName() != null) {
      sb.append("[group:").append(this.getGroupName()).append("]");
    }

    if (this.getMode() != null) {
      sb.append("[mode:").append(this.getMode()).append("]");
    }

    if (this.getQuota() != null) {
      sb.append("[quota:").append(this.getQuota()).append("]");
    }

    return sb.toString();
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<>();
    map.put("sourcePath", this.getSourcePath());
    return map;
  }

  @Override
  public void validate() {
    super.validate();
    if (this.getSourcePath() == null || this.getSourcePath().length() == 0) {
      throw new IllegalArgumentException(
          ERROR_MSG_NO_SOURCE_PATH + this);
    }
    if (!this.getSourcePath().startsWith("/")) {
      throw new IllegalArgumentException(
          ERROR_MSG_MUST_START_WITH_BACK_SLASH + this);
    }
    if (this.getDestinations() == null || this.getDestinations().size() == 0) {
      throw new IllegalArgumentException(
          ERROR_MSG_NO_DEST_PATH_SPECIFIED + this);
    }
    for (RemoteLocation loc : getDestinations()) {
      String nsId = loc.getNameserviceId();
      if (nsId == null || nsId.length() == 0) {
        throw new IllegalArgumentException(
            ERROR_MSG_INVAILD_DEST_NS + this);
      }
      if (loc.getDest() == null || loc.getDest().length() == 0) {
        throw new IllegalArgumentException(
            ERROR_MSG_INVAILD_DEST_PATH + this);
      }
      if (!loc.getDest().startsWith("/")) {
        throw new IllegalArgumentException(
            ERROR_MSG_ALL_DEST_MUST_START_WITH_BACK_SLASH + this);
      }
    }
  }

  @Override
  public long getExpirationMs() {
    return 0;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31)
        .append(this.getSourcePath())
        .append(this.getDestinations())
        .append(this.isReadOnly())
        .append(this.getDestOrder())
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MountTable) {
      MountTable other = (MountTable)obj;
      if (!this.getSourcePath().equals(other.getSourcePath())) {
        return false;
      } else if (!this.getDestinations().equals(other.getDestinations())) {
        return false;
      } else if (this.isReadOnly() != other.isReadOnly()) {
        return false;
      } else if (!this.getDestOrder().equals(other.getDestOrder())) {
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Check if a mount table spans all locations.
   * @return If the mount table spreads across all locations.
   */
  public boolean isAll() {
    DestinationOrder order = getDestOrder();
    return order == DestinationOrder.HASH_ALL ||
        order == DestinationOrder.RANDOM ||
        order == DestinationOrder.SPACE;
  }

  /**
   * Normalize a path for that filesystem.
   *
   * @param path Path to normalize.
   * @return Normalized path.
   */
  private static String normalizeFileSystemPath(final String path) {
    Path normalizedPath = new Path(path);
    return normalizedPath.toString();
  }
}