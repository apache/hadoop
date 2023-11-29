package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

@Public
@Unstable
public abstract class GetSubClustersResponse {
  @Public
  @Stable
  public static GetSubClustersResponse newInstance() {
    GetSubClustersResponse response = Records.newRecord(GetSubClustersResponse.class);
    return response;
  }
}
