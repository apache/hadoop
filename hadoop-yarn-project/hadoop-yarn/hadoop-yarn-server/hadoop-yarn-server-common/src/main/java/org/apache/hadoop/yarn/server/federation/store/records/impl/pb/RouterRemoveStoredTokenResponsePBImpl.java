package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRemoveStoredTokenResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRemoveStoredTokenResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProto;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreNewTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;

@Private
@Unstable
public class RouterRemoveStoredTokenResponsePBImpl extends RouterStoreNewTokenResponse {

  private RouterRemoveStoredTokenResponseProto proto =
      RouterRemoveStoredTokenResponseProto.getDefaultInstance();
  private RouterRemoveStoredTokenResponseProto.Builder builder = null;
  private boolean viaProto = false;
  private RouterStoreToken routerStoreToken = null;

  public RouterRemoveStoredTokenResponsePBImpl() {
    builder = RouterRemoveStoredTokenResponseProto.newBuilder();
  }

  public RouterRemoveStoredTokenResponsePBImpl(RouterRemoveStoredTokenResponseProto requestProto) {
    this.proto = requestProto;
    viaProto = true;
  }

  public RouterRemoveStoredTokenResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterRemoveStoredTokenResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.routerStoreToken != null &&
            !((RouterStoreTokenPBImpl) this.routerStoreToken).getProto().equals(builder.getRouterStoreToken())) {
      builder.setRouterStoreToken(convertToProtoFormat(this.routerStoreToken));
    }
  }

  @Override
  public RouterStoreToken getRouterStoreToken() {
    RouterRemoveStoredTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.routerStoreToken != null) {
      return this.routerStoreToken;
    }
    if (!p.hasRouterStoreToken()) {
      return null;
    }
    this.routerStoreToken = convertFromProtoFormat(p.getRouterStoreToken());
    return this.routerStoreToken;
  }

  @Override
  public void setRouterStoreToken(RouterStoreToken storeToken) {
    maybeInitBuilder();
    if (storeToken == null) {
      builder.clearRouterStoreToken();
    }
    this.routerStoreToken = storeToken;
  }

  private RouterStoreTokenProto convertToProtoFormat(RouterStoreToken storeToken) {
    return ((RouterStoreTokenPBImpl) storeToken).getProto();
  }

  private RouterStoreToken convertFromProtoFormat(RouterStoreTokenProto storeTokenProto) {
    return new RouterStoreTokenPBImpl(storeTokenProto);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
