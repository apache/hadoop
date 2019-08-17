/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import com.google.common.base.Strings;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.util.RadixNode;
import org.apache.hadoop.ozone.util.RadixTree;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.utils.db.*;
import org.apache.hadoop.utils.db.Table.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneAcl.ZERO_BITSET;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PREFIX_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.PREFIX_LOCK;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.PREFIX;

/**
 * Implementation of PrefixManager.
 */
public class PrefixManagerImpl implements PrefixManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PrefixManagerImpl.class);

  private static final List<OzoneAcl> EMPTY_ACL_LIST = new ArrayList<>();
  private final OMMetadataManager metadataManager;

  // In-memory prefix tree to optimize ACL evaluation
  private RadixTree<OmPrefixInfo> prefixTree;

  // TODO: This isRatisEnabled check will be removed as part of HDDS-1909,
  //  where we integrate both HA and Non-HA code.
  private boolean isRatisEnabled;

  public PrefixManagerImpl(OMMetadataManager metadataManager,
      boolean isRatisEnabled) {
    this.isRatisEnabled = isRatisEnabled;
    this.metadataManager = metadataManager;
    loadPrefixTree();
  }

  private void loadPrefixTree() {
    prefixTree = new RadixTree<>();
    try (TableIterator<String, ? extends
        KeyValue<String, OmPrefixInfo>> iterator =
             getMetadataManager().getPrefixTable().iterator()) {
      iterator.seekToFirst();
      while (iterator.hasNext()) {
        KeyValue<String, OmPrefixInfo> kv = iterator.next();
        prefixTree.insert(kv.getKey(), kv.getValue());
      }
    } catch (IOException ex) {
      LOG.error("Fail to load prefix tree");
    }
  }


  @Override
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl top be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    validateOzoneObj(obj);

    String prefixPath = obj.getPath();
    metadataManager.getLock().acquireLock(PREFIX_LOCK, prefixPath);
    try {
      OmPrefixInfo prefixInfo =
          metadataManager.getPrefixTable().get(prefixPath);

      OMPrefixAclOpResult omPrefixAclOpResult = addAcl(obj, acl, prefixInfo);

      return omPrefixAclOpResult.isOperationsResult();
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Add acl operation failed for prefix path:{} acl:{}",
            prefixPath, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(PREFIX_LOCK, prefixPath);
    }
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    validateOzoneObj(obj);
    String prefixPath = obj.getPath();
    metadataManager.getLock().acquireLock(PREFIX_LOCK, prefixPath);
    try {
      OmPrefixInfo prefixInfo =
          metadataManager.getPrefixTable().get(prefixPath);
      OMPrefixAclOpResult omPrefixAclOpResult = removeAcl(obj, acl, prefixInfo);

      if (!omPrefixAclOpResult.isOperationsResult()) {
        LOG.debug("acl {} does not exist for prefix path {} ", acl, prefixPath);
        return false;
      }

      return omPrefixAclOpResult.isOperationsResult();

    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Remove prefix acl operation failed for prefix path:{}" +
            " acl:{}", prefixPath, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(PREFIX_LOCK, prefixPath);
    }
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    validateOzoneObj(obj);
    String prefixPath = obj.getPath();
    metadataManager.getLock().acquireLock(PREFIX_LOCK, prefixPath);
    try {
      OmPrefixInfo prefixInfo =
          metadataManager.getPrefixTable().get(prefixPath);

      OMPrefixAclOpResult omPrefixAclOpResult = setAcl(obj, acls, prefixInfo);

      return omPrefixAclOpResult.isOperationsResult();
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Set prefix acl operation failed for prefix path:{} acls:{}",
            prefixPath, acls, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(PREFIX_LOCK, prefixPath);
    }
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    validateOzoneObj(obj);
    String prefixPath = obj.getPath();
    metadataManager.getLock().acquireLock(PREFIX_LOCK, prefixPath);
    try {
      String longestPrefix = prefixTree.getLongestPrefix(prefixPath);
      if (prefixPath.equals(longestPrefix)) {
        RadixNode<OmPrefixInfo> lastNode =
            prefixTree.getLastNodeInPrefixPath(prefixPath);
        if (lastNode != null && lastNode.getValue() != null) {
          return lastNode.getValue().getAcls();
        }
      }
    } finally {
      metadataManager.getLock().releaseLock(PREFIX_LOCK, prefixPath);
    }
    return EMPTY_ACL_LIST;
  }

  /**
   * Check access for given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(OzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);

    String prefixPath = ozObject.getPath();
    metadataManager.getLock().acquireLock(PREFIX_LOCK, prefixPath);
    try {
      String longestPrefix = prefixTree.getLongestPrefix(prefixPath);
      if (prefixPath.equals(longestPrefix)) {
        RadixNode<OmPrefixInfo> lastNode =
            prefixTree.getLastNodeInPrefixPath(prefixPath);
        if (lastNode != null && lastNode.getValue() != null) {
          boolean hasAccess = OzoneUtils.checkAclRights(lastNode.getValue().
              getAcls(), context);
          LOG.debug("user:{} has access rights for ozObj:{} ::{} ",
              context.getClientUgi(), ozObject, hasAccess);
          return hasAccess;
        } else {
          return true;
        }
      } else {
        return true;
      }
    } finally {
      metadataManager.getLock().releaseLock(PREFIX_LOCK, prefixPath);
    }
  }

  @Override
  public List<OmPrefixInfo> getLongestPrefixPath(String path) {
    String prefixPath = prefixTree.getLongestPrefix(path);
    metadataManager.getLock().acquireLock(PREFIX_LOCK, prefixPath);
    try {
      return getLongestPrefixPathHelper(prefixPath);
    } finally {
      metadataManager.getLock().releaseLock(PREFIX_LOCK, prefixPath);
    }
  }

  /**
   * Get longest prefix path assuming caller take prefix lock.
   * @param prefixPath
   * @return list of prefix info.
   */
  private List<OmPrefixInfo> getLongestPrefixPathHelper(String prefixPath) {
    return prefixTree.getLongestPrefixPath(prefixPath).stream()
          .map(c -> c.getValue()).collect(Collectors.toList());
  }

  /**
   * Helper method to validate ozone object.
   * @param obj
   * */
  public void validateOzoneObj(OzoneObj obj) throws OMException {
    Objects.requireNonNull(obj);

    if (!obj.getResourceType().equals(PREFIX)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "PrefixManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    String prefixName = obj.getPrefixName();

    if (Strings.isNullOrEmpty(volume)) {
      throw new OMException("Volume name is required.", VOLUME_NOT_FOUND);
    }
    if (Strings.isNullOrEmpty(bucket)) {
      throw new OMException("Bucket name is required.", BUCKET_NOT_FOUND);
    }
    if (Strings.isNullOrEmpty(prefixName)) {
      throw new OMException("Prefix name is required.", PREFIX_NOT_FOUND);
    }
    if (!prefixName.endsWith("/")) {
      throw new OMException("Invalid prefix name: " + prefixName,
          PREFIX_NOT_FOUND);
    }
  }

  public OMPrefixAclOpResult addAcl(OzoneObj ozoneObj, OzoneAcl ozoneAcl,
      OmPrefixInfo prefixInfo) throws IOException {

    List<OzoneAcl> ozoneAclList = null;
    if (prefixInfo != null) {
      ozoneAclList = prefixInfo.getAcls();
    }

    if (ozoneAclList == null) {
      ozoneAclList = new ArrayList<>();
      ozoneAclList.add(ozoneAcl);
    } else {
      boolean addToExistingAcl = false;
      for(OzoneAcl existingAcl: ozoneAclList) {
        if(existingAcl.getName().equals(ozoneAcl.getName()) &&
            existingAcl.getType().equals(ozoneAcl.getType())) {

          BitSet bits = (BitSet) ozoneAcl.getAclBitSet().clone();

          // We need to do "or" before comparision because think of a case like
          // existing acl is 777 and newly added acl is 444, we have already
          // that acl set. In this case if we do direct check they will not
          // be equal, but if we do or and then check, we shall know it
          // has acl's already set or not.
          bits.or(existingAcl.getAclBitSet());

          if (bits.equals(existingAcl.getAclBitSet())) {
            return new OMPrefixAclOpResult(null, false);
          } else {
            existingAcl.getAclBitSet().or(ozoneAcl.getAclBitSet());
            addToExistingAcl = true;
            break;
          }
        }
      }
      if (!addToExistingAcl) {
        ozoneAclList.add(ozoneAcl);
      }
    }
    OmPrefixInfo.Builder upiBuilder = OmPrefixInfo.newBuilder();
    upiBuilder.setName(ozoneObj.getPath()).setAcls(ozoneAclList);
    if (prefixInfo != null && prefixInfo.getMetadata() != null) {
      upiBuilder.addAllMetadata(prefixInfo.getMetadata());
    }
    prefixInfo = upiBuilder.build();

    // update the in-memory prefix tree
    prefixTree.insert(ozoneObj.getPath(), prefixInfo);

    if (!isRatisEnabled) {
      metadataManager.getPrefixTable().put(ozoneObj.getPath(), prefixInfo);
    }
    return new OMPrefixAclOpResult(prefixInfo, true);
  }

  public OMPrefixAclOpResult removeAcl(OzoneObj ozoneObj, OzoneAcl ozoneAcl,
      OmPrefixInfo prefixInfo) throws IOException {
    List<OzoneAcl> list = null;
    if (prefixInfo != null) {
      list = prefixInfo.getAcls();
    }

    if (list == null) {
      return new OMPrefixAclOpResult(null, false);
    }

    boolean removed = false;
    for (OzoneAcl existingAcl: list) {
      if (existingAcl.getName().equals(ozoneAcl.getName())
          && existingAcl.getType() == ozoneAcl.getType()) {
        BitSet bits = (BitSet) ozoneAcl.getAclBitSet().clone();
        bits.and(existingAcl.getAclBitSet());

        // This happens when the acl bitset is not existing for current name
        // and type.
        // Like a case we have 444 permission, 333 is asked to removed.
        if (bits.equals(ZERO_BITSET)) {
          removed = false;
          break;
        }

        // We have some matching. Remove them.
        existingAcl.getAclBitSet().xor(bits);

        // If existing acl has same bitset as passed acl bitset, remove that
        // acl from the list
        if (existingAcl.getAclBitSet().equals(ZERO_BITSET)) {
          list.remove(existingAcl);
        }
        removed = true;
        break;
      }
    }

    // Nothing is matching to remove.
    if (!removed) {
      return new OMPrefixAclOpResult(null, false);
    } else {
      OmPrefixInfo.Builder upiBuilder = OmPrefixInfo.newBuilder();
      upiBuilder.setName(ozoneObj.getPath()).setAcls(list);
      if (prefixInfo != null && prefixInfo.getMetadata() != null) {
        upiBuilder.addAllMetadata(prefixInfo.getMetadata());
      }
      prefixInfo = upiBuilder.build();

      // Update in-memory prefix tree.
      if (list.isEmpty()) {
        prefixTree.removePrefixPath(ozoneObj.getPath());
        if (!isRatisEnabled) {
          metadataManager.getPrefixTable().delete(ozoneObj.getPath());
        }
      } else {
        prefixTree.insert(ozoneObj.getPath(), prefixInfo);
        if (!isRatisEnabled) {
          metadataManager.getPrefixTable().put(ozoneObj.getPath(), prefixInfo);
        }
      }
      return new OMPrefixAclOpResult(prefixInfo, true);
    }
  }

  public OMPrefixAclOpResult setAcl(OzoneObj ozoneObj, List<OzoneAcl> ozoneAcls,
      OmPrefixInfo prefixInfo) throws IOException {
    OmPrefixInfo.Builder upiBuilder = OmPrefixInfo.newBuilder();
    List<OzoneAcl> aclsToBeSet = new ArrayList<>(ozoneAcls.size());
    aclsToBeSet.addAll(ozoneAcls);
    upiBuilder.setName(ozoneObj.getPath());
    if (prefixInfo != null && prefixInfo.getMetadata() != null) {
      upiBuilder.addAllMetadata(prefixInfo.getMetadata());
    }

    // Inherit DEFAULT acls from prefix.
    boolean prefixParentFound = false;
    List<OmPrefixInfo> prefixList = getLongestPrefixPathHelper(
        prefixTree.getLongestPrefix(ozoneObj.getPath()));

    if (prefixList.size() > 0) {
      // Add all acls from direct parent to key.
      OmPrefixInfo parentPrefixInfo = prefixList.get(prefixList.size() - 1);
      if (parentPrefixInfo != null) {
        aclsToBeSet.addAll(OzoneUtils.getDefaultAcls(
            parentPrefixInfo.getAcls()));
        prefixParentFound = true;
      }
    }

    // If no parent prefix is found inherit DEFULT acls from bucket.
    if (!prefixParentFound) {
      String bucketKey = metadataManager.getBucketKey(ozoneObj.getVolumeName(),
          ozoneObj.getBucketName());
      OmBucketInfo bucketInfo = metadataManager.getBucketTable().
          get(bucketKey);
      if (bucketInfo != null) {
        bucketInfo.getAcls().forEach(a -> {
          if (a.getAclScope().equals(OzoneAcl.AclScope.DEFAULT)) {
            aclsToBeSet.add(new OzoneAcl(a.getType(), a.getName(),
                a.getAclBitSet(), OzoneAcl.AclScope.ACCESS));
          }
        });
      }
    }

    prefixInfo = upiBuilder.setAcls(aclsToBeSet).build();

    prefixTree.insert(ozoneObj.getPath(), prefixInfo);
    if (!isRatisEnabled) {
      metadataManager.getPrefixTable().put(ozoneObj.getPath(), prefixInfo);
    }
    return new OMPrefixAclOpResult(prefixInfo, true);
  }

  /**
   * Result of the prefix acl operation.
   */
  public static class OMPrefixAclOpResult {
    private OmPrefixInfo omPrefixInfo;
    private boolean operationsResult;

    public OMPrefixAclOpResult(OmPrefixInfo omPrefixInfo,
        boolean operationsResult) {
      this.omPrefixInfo = omPrefixInfo;
      this.operationsResult = operationsResult;
    }

    public OmPrefixInfo getOmPrefixInfo() {
      return omPrefixInfo;
    }

    public boolean isOperationsResult() {
      return operationsResult;
    }
  }
}
