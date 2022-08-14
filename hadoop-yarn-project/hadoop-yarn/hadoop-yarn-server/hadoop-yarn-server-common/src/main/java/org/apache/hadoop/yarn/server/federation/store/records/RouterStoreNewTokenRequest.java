package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class RouterStoreNewTokenRequest {

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static RouterStoreNewTokenRequest newInstance(RouterStoreToken routerStoreToken) {
    RouterStoreNewTokenRequest request = Records.newRecord(RouterStoreNewTokenRequest.class);
    request.setRouterStoreToken(routerStoreToken);
    return request;
  }

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract RouterStoreToken getRouterStoreToken();

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract void setRouterStoreToken(RouterStoreToken routerStoreToken);

}
