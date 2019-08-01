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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
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

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    // protobuf guarantees volume and acls are non-null.
    String volume = getVolumeName();
    List<OzoneAcl> ozoneAcls = getAcls();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeUpdates();
    OmVolumeArgs omVolumeArgs = null;

    OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    IOException exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, null, null);
      }
      lockAcquired =
          omMetadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);
      omVolumeArgs = omMetadataManager.getVolumeTable().get(dbVolumeKey);
      if (omVolumeArgs == null) {
        throw new OMException(OMException.ResultCodes.VOLUME_NOT_FOUND);
      }

      // result is false upon add existing acl or remove non-existing acl
      boolean result = true;
      try {
        omVolumeAclOp.apply(ozoneAcls, omVolumeArgs);
      } catch (OMException ex) {
        result = false;
      }

      if (result) {
        // update cache.
        omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(dbVolumeKey),
            new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));
      }

      omClientResponse = onSuccess(omResponse, omVolumeArgs, result);
    } catch (IOException ex) {
      exception = ex;
      omMetrics.incNumVolumeUpdateFails();
      omClientResponse = onFailure(omResponse, ex);
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (lockAcquired) {
        omMetadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
      }
    }

    onComplete(exception);

    return omClientResponse;
  }

  /**
   * Get the Acls from the request.
   * @return List of OzoneAcls, for add/remove it is a single element list
   * for set it can be non-single element list.
   */
  abstract List<OzoneAcl> getAcls();

  /**
   * Get the volume name from the request.
   * @return volume name
   * This is needed for case where volume does not exist and the omVolumeArgs is
   * null.
   */
  abstract String getVolumeName();

  // TODO: Finer grain metrics can be moved to these callbacks. They can also
  // be abstracted into separate interfaces in future.
  /**
   * Get the initial om response builder with lock.
   * @return om response builder.
   */
  abstract OMResponse.Builder onInit();

  /**
   * Get the om client response on success case with lock.
   * @param omResponse
   * @param omVolumeArgs
   * @param result
   * @return OMClientResponse
   */
  abstract OMClientResponse onSuccess(
      OMResponse.Builder omResponse, OmVolumeArgs omVolumeArgs, boolean result);

  /**
   * Get the om client response on failure case with lock.
   * @param omResponse
   * @param ex
   * @return OMClientResponse
   */
  abstract OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException ex);

  /**
   * Completion hook for final processing before return without lock.
   * Usually used for logging without lock.
   * @param ex
   */
  abstract void onComplete(IOException ex);
}
