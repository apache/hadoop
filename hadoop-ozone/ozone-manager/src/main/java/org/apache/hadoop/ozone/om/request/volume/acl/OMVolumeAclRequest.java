package org.apache.hadoop.ozone.om.request.volume.acl;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.scm.storage.CheckedBiFunction;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * Base class for OMVolumeAcl Request.
 */
public abstract class OMVolumeAclRequest extends OMClientRequest {

  private CheckedBiFunction<List<OzoneAcl>, OmVolumeArgs, IOException>
      omVolumeAclOp;

  public OMVolumeAclRequest(OzoneManagerProtocolProtos.OMRequest omRequest,
      CheckedBiFunction<List<OzoneAcl>, OmVolumeArgs, IOException> aclOp) {
    super(omRequest);
    omVolumeAclOp = aclOp;
  }

  /**
   * Get the Acls from the request.
   * @return List of OzoneAcls, for add/remove it is a single element list
   * for set it can be non-single element list.
   */
  protected abstract List<OzoneAcl> getAcls();

  /**
   * Get the volume name from the request.
   * @return volume name
   * This is needed for case where volume does not exist and the omVolumeArgs is
   * null.
   */
  protected abstract String getVolumeName();

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    // protobuf guarantees volume and acls are non-null.
    String volume = getVolumeName();
    List<OzoneAcl> ozoneAcls = getAcls();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeUpdates();
    IOException exception = null;
    OmVolumeArgs omVolumeArgs = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE, volume,
            null, null);
      }
      lockAcquired =
          omMetadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);
      omVolumeArgs = omMetadataManager.getVolumeTable().get(dbVolumeKey);
      if (omVolumeArgs == null) {
        throw new OMException(OMException.ResultCodes.VOLUME_NOT_FOUND);
      }

      omVolumeAclOp.apply(ozoneAcls, omVolumeArgs);

      // update cache.
      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(dbVolumeKey),
          new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));
    } catch (IOException ex) {
      exception = ex;
    } finally {
      if (lockAcquired) {
        omMetadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
      }
    }

    return handleResult(omVolumeArgs, omMetrics, exception);
  }

  abstract protected OMClientResponse handleResult(OmVolumeArgs omVolumeArgs,
      OMMetrics omMetrics, IOException ex);

}
