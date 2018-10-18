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
package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.ReencryptAction;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.EnumSet;

/**
 * The public API for performing administrative functions on HDFS. Those writing
 * applications against HDFS should prefer this interface to directly accessing
 * functionality in DistributedFileSystem or DFSClient.
 *
 * Note that this is distinct from the similarly-named DFSAdmin, which
 * is a class that provides the functionality for the CLI `hdfs dfsadmin ...'
 * commands.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HdfsAdmin {

  private DistributedFileSystem dfs;
  private static final FsPermission TRASH_PERMISSION = new FsPermission(
      FsAction.ALL, FsAction.ALL, FsAction.ALL, true);

  /**
   * Create a new HdfsAdmin client.
   *
   * @param uri the unique URI of the HDFS file system to administer
   * @param conf configuration
   * @throws IOException in the event the file system could not be created
   */
  public HdfsAdmin(URI uri, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("'" + uri + "' is not an HDFS URI.");
    } else {
      dfs = (DistributedFileSystem)fs;
    }
  }

  /**
   * Set the namespace quota (count of files, directories, and sym links) for a
   * directory.
   *
   * @param src the path to set the quota for
   * @param quota the value to set for the quota
   * @throws IOException in the event of error
   */
  public void setQuota(Path src, long quota) throws IOException {
    dfs.setQuota(src, quota, HdfsConstants.QUOTA_DONT_SET);
  }

  /**
   * Clear the namespace quota (count of files, directories and sym links) for a
   * directory.
   *
   * @param src the path to clear the quota of
   * @throws IOException in the event of error
   */
  public void clearQuota(Path src) throws IOException {
    dfs.setQuota(src, HdfsConstants.QUOTA_RESET, HdfsConstants.QUOTA_DONT_SET);
  }

  /**
   * Set the storage space quota (size of files) for a directory. Note that
   * directories and sym links do not occupy storage space.
   *
   * @param src the path to set the space quota of
   * @param spaceQuota the value to set for the space quota
   * @throws IOException in the event of error
   */
  public void setSpaceQuota(Path src, long spaceQuota) throws IOException {
    dfs.setQuota(src, HdfsConstants.QUOTA_DONT_SET, spaceQuota);
  }

  /**
   * Clear the storage space quota (size of files) for a directory. Note that
   * directories and sym links do not occupy storage space.
   *
   * @param src the path to clear the space quota of
   * @throws IOException in the event of error
   */
  public void clearSpaceQuota(Path src) throws IOException {
    dfs.setQuota(src, HdfsConstants.QUOTA_DONT_SET, HdfsConstants.QUOTA_RESET);
  }

  /**
   * Set the quota by storage type for a directory. Note that
   * directories and sym links do not occupy storage type quota.
   *
   * @param src the target directory to set the quota by storage type
   * @param type the storage type to set for quota by storage type
   * @param quota the value to set for quota by storage type
   * @throws IOException in the event of error
   */
  public void setQuotaByStorageType(Path src, StorageType type, long quota)
      throws IOException {
    dfs.setQuotaByStorageType(src, type, quota);
  }

  /**
   * Clear the space quota by storage type for a directory. Note that
   * directories and sym links do not occupy storage type quota.
   *
   * @param src the target directory to clear the quota by storage type
   * @param type the storage type to clear for quota by storage type
   * @throws IOException in the event of error
   */
  public void clearQuotaByStorageType(Path src, StorageType type) throws IOException {
    dfs.setQuotaByStorageType(src, type, HdfsConstants.QUOTA_RESET);
  }

  /**
   * Allow snapshot on a directory.
   * @param path The path of the directory where snapshots will be taken.
   */
  public void allowSnapshot(Path path) throws IOException {
    dfs.allowSnapshot(path);
  }

  /**
   * Disallow snapshot on a directory.
   * @param path The path of the snapshottable directory.
   */
  public void disallowSnapshot(Path path) throws IOException {
    dfs.disallowSnapshot(path);
  }

  /**
   * Add a new CacheDirectiveInfo.
   *
   * @param info Information about a directive to add.
   * @param flags {@link CacheFlag}s to use for this operation.
   * @return the ID of the directive that was created.
   * @throws IOException if the directive could not be added
   */
  public long addCacheDirective(CacheDirectiveInfo info,
      EnumSet<CacheFlag> flags) throws IOException {
  return dfs.addCacheDirective(info, flags);
  }

  /**
   * Modify a CacheDirective.
   *
   * @param info Information about the directive to modify. You must set the ID
   *          to indicate which CacheDirective you want to modify.
   * @param flags {@link CacheFlag}s to use for this operation.
   * @throws IOException if the directive could not be modified
   */
  public void modifyCacheDirective(CacheDirectiveInfo info,
      EnumSet<CacheFlag> flags) throws IOException {
    dfs.modifyCacheDirective(info, flags);
  }

  /**
   * Remove a CacheDirective.
   *
   * @param id identifier of the CacheDirectiveInfo to remove
   * @throws IOException if the directive could not be removed
   */
  public void removeCacheDirective(long id)
      throws IOException {
    dfs.removeCacheDirective(id);
  }

  /**
   * List cache directives. Incrementally fetches results from the server.
   *
   * @param filter Filter parameters to use when listing the directives, null to
   *               list all directives visible to us.
   * @return A RemoteIterator which returns CacheDirectiveInfo objects.
   */
  public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(
      CacheDirectiveInfo filter) throws IOException {
    return dfs.listCacheDirectives(filter);
  }

  /**
   * Add a cache pool.
   *
   * @param info
   *          The request to add a cache pool.
   * @throws IOException
   *          If the request could not be completed.
   */
  public void addCachePool(CachePoolInfo info) throws IOException {
    dfs.addCachePool(info);
  }

  /**
   * Modify an existing cache pool.
   *
   * @param info
   *          The request to modify a cache pool.
   * @throws IOException
   *          If the request could not be completed.
   */
  public void modifyCachePool(CachePoolInfo info) throws IOException {
    dfs.modifyCachePool(info);
  }

  /**
   * Remove a cache pool.
   *
   * @param poolName
   *          Name of the cache pool to remove.
   * @throws IOException
   *          if the cache pool did not exist, or could not be removed.
   */
  public void removeCachePool(String poolName) throws IOException {
    dfs.removeCachePool(poolName);
  }

  /**
   * List all cache pools.
   *
   * @return A remote iterator from which you can get CachePoolEntry objects.
   *          Requests will be made as needed.
   * @throws IOException
   *          If there was an error listing cache pools.
   */
  public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
    return dfs.listCachePools();
  }

  /**
   * Get KeyProvider if present.
   *
   * @return the key provider if encryption is enabled on HDFS.
   *         Otherwise, it returns null.
   * @throws IOException on RPC exception to the NN.
   */
  public KeyProvider getKeyProvider() throws IOException {
    return dfs.getClient().getKeyProvider();
  }

  /**
   * Create an encryption zone rooted at an empty existing directory, using the
   * specified encryption key. An encryption zone has an associated encryption
   * key used when reading and writing files within the zone.
   *
   * @param path    The path of the root of the encryption zone. Must refer to
   *                an empty, existing directory.
   * @param keyName Name of key available at the KeyProvider.
   * @throws IOException            if there was a general IO exception
   * @throws AccessControlException if the caller does not have access to path
   * @throws FileNotFoundException  if the path does not exist
   */
  @Deprecated
  public void createEncryptionZone(Path path, String keyName)
      throws IOException, AccessControlException, FileNotFoundException {
    dfs.createEncryptionZone(path, keyName);
  }

  /**
   * Create an encryption zone rooted at an empty existing directory, using the
   * specified encryption key. An encryption zone has an associated encryption
   * key used when reading and writing files within the zone.
   *
   * Additional options, such as provisioning the trash directory, can be
   * specified using {@link CreateEncryptionZoneFlag} flags.
   *
   * @param path    The path of the root of the encryption zone. Must refer to
   *                an empty, existing directory.
   * @param keyName Name of key available at the KeyProvider.
   * @param flags   flags for this operation.
   * @throws IOException            if there was a general IO exception
   * @throws AccessControlException if the caller does not have access to path
   * @throws FileNotFoundException  if the path does not exist
   * @throws HadoopIllegalArgumentException if the flags are invalid
   */
  public void createEncryptionZone(Path path, String keyName,
      EnumSet<CreateEncryptionZoneFlag> flags)
      throws IOException, AccessControlException, FileNotFoundException,
      HadoopIllegalArgumentException{
    dfs.createEncryptionZone(path, keyName);
    if (flags.contains(CreateEncryptionZoneFlag.PROVISION_TRASH)) {
      if (flags.contains(CreateEncryptionZoneFlag.NO_TRASH)) {
        throw new HadoopIllegalArgumentException(
            "can not have both PROVISION_TRASH and NO_TRASH flags");
      }
      dfs.provisionEZTrash(path, TRASH_PERMISSION);
    }
  }

  /**
   * Provision a trash directory for a given encryption zone.

   * @param path the root of the encryption zone
   * @throws IOException if the trash directory can not be created.
   */
  public void provisionEncryptionZoneTrash(Path path) throws IOException {
    dfs.provisionEZTrash(path, TRASH_PERMISSION);
  }

  /**
   * Get the path of the encryption zone for a given file or directory.
   *
   * @param path The path to get the ez for.
   * @return An EncryptionZone, or null if path does not exist or is not in an
   * ez.
   * @throws IOException            if there was a general IO exception
   * @throws AccessControlException if the caller does not have access to path
   */
  public EncryptionZone getEncryptionZoneForPath(Path path)
      throws IOException, AccessControlException {
    return dfs.getEZForPath(path);
  }

  /**
   * Returns a RemoteIterator which can be used to list the encryption zones
   * in HDFS. For large numbers of encryption zones, the iterator will fetch
   * the list of zones in a number of small batches.
   * <p>
   * Since the list is fetched in batches, it does not represent a
   * consistent snapshot of the entire list of encryption zones.
   * <p>
   * This method can only be called by HDFS superusers.
   */
  public RemoteIterator<EncryptionZone> listEncryptionZones()
      throws IOException {
    return dfs.listEncryptionZones();
  }

  /**
   * Performs re-encryption action for a given encryption zone.
   *
   * @param zone the root of the encryption zone
   * @param action the re-encrypt action
   * @throws IOException If any error occurs when handling re-encrypt action.
   */
  public void reencryptEncryptionZone(final Path zone,
      final ReencryptAction action) throws IOException {
    dfs.reencryptEncryptionZone(zone, action);
  }

  /**
   * Returns a RemoteIterator which can be used to list all re-encryption
   * information. For large numbers of re-encryptions, the iterator will fetch
   * the list in a number of small batches.
   * <p>
   * Since the list is fetched in batches, it does not represent a
   * consistent snapshot of the entire list of encryption zones.
   * <p>
   * This method can only be called by HDFS superusers.
   */
  public RemoteIterator<ZoneReencryptionStatus> listReencryptionStatus()
      throws IOException {
    return dfs.listReencryptionStatus();
  }

  /**
   * Returns the FileEncryptionInfo on the HdfsFileStatus for the given path.
   * The return value can be null if the path points to a directory, or a file
   * that is not in an encryption zone.
   *
   * @throws FileNotFoundException if the path does not exist.
   * @throws AccessControlException if no execute permission on parent path.
   */
  public FileEncryptionInfo getFileEncryptionInfo(final Path path)
      throws IOException {
    return dfs.getFileEncryptionInfo(path);
  }

  /**
   * Exposes a stream of namesystem events. Only events occurring after the
   * stream is created are available.
   * See {@link org.apache.hadoop.hdfs.DFSInotifyEventInputStream}
   * for information on stream usage.
   * See {@link org.apache.hadoop.hdfs.inotify.Event}
   * for information on the available events.
   * <p>
   * Inotify users may want to tune the following HDFS parameters to
   * ensure that enough extra HDFS edits are saved to support inotify clients
   * that fall behind the current state of the namespace while reading events.
   * The default parameter values should generally be reasonable. If edits are
   * deleted before their corresponding events can be read, clients will see a
   * {@link org.apache.hadoop.hdfs.inotify.MissingEventsException} on
   * {@link org.apache.hadoop.hdfs.DFSInotifyEventInputStream} method calls.
   *
   * It should generally be sufficient to tune these parameters:
   * dfs.namenode.num.extra.edits.retained
   * dfs.namenode.max.extra.edits.segments.retained
   *
   * Parameters that affect the number of created segments and the number of
   * edits that are considered necessary, i.e. do not count towards the
   * dfs.namenode.num.extra.edits.retained quota):
   * dfs.namenode.checkpoint.period
   * dfs.namenode.checkpoint.txns
   * dfs.namenode.num.checkpoints.retained
   * dfs.ha.log-roll.period
   * <p>
   * It is recommended that local journaling be configured
   * (dfs.namenode.edits.dir) for inotify (in addition to a shared journal)
   * so that edit transfers from the shared journal can be avoided.
   *
   * @throws IOException If there was an error obtaining the stream.
   */
  public DFSInotifyEventInputStream getInotifyEventStream() throws IOException {
    return dfs.getInotifyEventStream();
  }

  /**
   * A version of {@link HdfsAdmin#getInotifyEventStream()} meant for advanced
   * users who are aware of HDFS edits up to lastReadTxid (e.g. because they
   * have access to an FSImage inclusive of lastReadTxid) and only want to read
   * events after this point.
   */
  public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid)
      throws IOException {
    return dfs.getInotifyEventStream(lastReadTxid);
  }

  /**
   * Set the source path to the specified storage policy.
   *
   * @param src The source path referring to either a directory or a file.
   * @param policyName The name of the storage policy.
   */
  public void setStoragePolicy(final Path src, final String policyName)
      throws IOException {
    dfs.setStoragePolicy(src, policyName);
  }

  /**
   * Unset the storage policy set for a given file or directory.
   *
   * @param src file or directory path.
   * @throws IOException
   */
  public void unsetStoragePolicy(final Path src) throws IOException {
    dfs.unsetStoragePolicy(src);
  }

  /**
   * Query the effective storage policy ID for the given file or directory.
   *
   * @param src file or directory path.
   * @return storage policy for the given file or directory.
   * @throws IOException
   */
  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    return dfs.getStoragePolicy(src);
  }

  /**
   * Retrieve all the storage policies supported by HDFS file system.
   *
   * @return all storage policies supported by HDFS file system.
   * @throws IOException
   */
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return dfs.getAllStoragePolicies();
  }

  /**
   * Set the source path to the specified erasure coding policy.
   *
   * @param path The source path referring to a directory.
   * @param ecPolicyName The erasure coding policy name for the directory.
   *
   * @throws IOException
   * @throws HadoopIllegalArgumentException if the specified EC policy is not
   * enabled on the cluster
   */
  public void setErasureCodingPolicy(final Path path,
      final String ecPolicyName) throws IOException {
    dfs.setErasureCodingPolicy(path, ecPolicyName);
  }

  /**
   * Get the erasure coding policy information for the specified path
   *
   * @param path
   * @return Returns the policy information if file or directory on the path is
   *          erasure coded. Null otherwise.
   * @throws IOException
   */
  public ErasureCodingPolicy getErasureCodingPolicy(final Path path)
      throws IOException {
    return dfs.getErasureCodingPolicy(path);
  }

  /**
   * Set the source path to the specified storage policy.
   *
   * @param path The source path referring to either a directory or a file.
   * @throws IOException
   */
  public void satisfyStoragePolicy(final Path path) throws IOException {
    dfs.satisfyStoragePolicy(path);
  }

  /**
   * Get the Erasure coding policies supported.
   *
   * @throws IOException
   */
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    return dfs.getClient().getErasureCodingPolicies();
  }

  /**
   * Unset erasure coding policy from the directory.
   *
   * @param path The source path referring to a directory.
   * @throws IOException
   */
  public void unsetErasureCodingPolicy(final Path path) throws IOException {
    dfs.unsetErasureCodingPolicy(path);
  }

  /**
   * Add Erasure coding policies to HDFS. For each policy input, schema and
   * cellSize are musts, name and id are ignored. They will be automatically
   * created and assigned by Namenode once the policy is successfully added,
   * and will be returned in the response; policy states will be set to
   * DISABLED automatically.
   *
   * @param policies The user defined ec policy list to add.
   * @return Return the response list of adding operations.
   * @throws IOException
   */
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies)  throws IOException {
    return dfs.addErasureCodingPolicies(policies);
  }

  /**
   * Remove erasure coding policy.
   *
   * @param ecPolicyName The name of the policy to be removed.
   * @throws IOException
   */
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    dfs.removeErasureCodingPolicy(ecPolicyName);
  }

  /**
   * Enable erasure coding policy.
   *
   * @param ecPolicyName The name of the policy to be enabled.
   * @throws IOException
   */
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    dfs.enableErasureCodingPolicy(ecPolicyName);
  }

  /**
   * Disable erasure coding policy.
   *
   * @param ecPolicyName The name of the policy to be disabled.
   * @throws IOException
   */
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    dfs.disableErasureCodingPolicy(ecPolicyName);
  }

  /**
   * Returns a RemoteIterator which can be used to list all open files
   * currently managed by the NameNode. For large numbers of open files,
   * iterator will fetch the list in batches of configured size.
   * <p>
   * Since the list is fetched in batches, it does not represent a
   * consistent snapshot of the all open files.
   * <p>
   * This method can only be called by HDFS superusers.
   */
  @Deprecated
  public RemoteIterator<OpenFileEntry> listOpenFiles() throws IOException {
    return dfs.listOpenFiles();
  }

  public RemoteIterator<OpenFileEntry> listOpenFiles(
      EnumSet<OpenFilesType> openFilesTypes, String path) throws IOException {
    return dfs.listOpenFiles(openFilesTypes, path);
  }

}
