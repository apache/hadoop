package org.apache.hadoop.ozone.om.request.bucket.acl;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.util.BooleanBiFunction;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.acl.OMBucketAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;


/**
 * Handle removeAcl request for bucket.
 */
public class OMBucketRemoveAclRequest extends OMBucketAclRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketAddAclRequest.class);

  private static BooleanBiFunction<List<OzoneAcl>, OmBucketInfo> bucketAddAclOp;
  private String path;
  private List<OzoneAcl> ozoneAcls;

  static {
    bucketAddAclOp = (ozoneAcls, omBucketInfo) -> {
      return omBucketInfo.removeAcl(ozoneAcls.get(0));
    };
  }

  public OMBucketRemoveAclRequest(OMRequest omRequest) {
    super(omRequest, bucketAddAclOp);
    OzoneManagerProtocolProtos.RemoveAclRequest removeAclRequest =
        getOmRequest().getRemoveAclRequest();
    path = removeAclRequest.getObj().getPath();
    ozoneAcls = Lists.newArrayList(
        OzoneAcl.fromProtobuf(removeAclRequest.getAcl()));
  }

  @Override
  List<OzoneAcl> getAcls() {
    return ozoneAcls;
  }

  @Override
  String getPath() {
    return path;
  }

  @Override
  OMResponse.Builder onInit() {
    return OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.RemoveAcl).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

  }

  @Override
  OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmBucketInfo omBucketInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setRemoveAclResponse(RemoveAclResponse.newBuilder()
        .setResponse(operationResult));
    return new OMBucketAclResponse(omBucketInfo,
        omResponse.build());
  }

  @Override
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException exception) {
    return new OMBucketAclResponse(null,
        createErrorOMResponse(omResponse, exception));
  }

  @Override
  void onComplete(boolean operationResult, IOException exception,
      OMMetrics omMetrics) {
    if (operationResult) {
      LOG.debug("Remove acl: {} for path: {} success!", getAcls(), getPath());
    } else {
      omMetrics.incNumBucketUpdateFails();
      if (exception == null) {
        LOG.error("Remove acl {} for path {} failed, because acl does not " +
                "exist",
            getAcls(), getPath());
      } else {
        LOG.error("Remove acl {} for path {} failed!", getAcls(), getPath(),
            exception);
      }
    }
  }
}

