package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class RouterRemoveStoredTokenResponse {

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static RouterRemoveStoredTokenResponse newInstance(RouterStoreToken routerStoreToken) {
    RouterRemoveStoredTokenResponse request = Records.newRecord(RouterRemoveStoredTokenResponse.class);
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
