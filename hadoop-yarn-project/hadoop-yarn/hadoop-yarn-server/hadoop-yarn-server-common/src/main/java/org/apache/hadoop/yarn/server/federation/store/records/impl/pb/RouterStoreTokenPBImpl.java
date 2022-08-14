package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProtoOrBuilder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RouterStoreTokenPBImpl extends RouterStoreToken {

  private RouterStoreTokenProto proto = RouterStoreTokenProto.getDefaultInstance();

  private RouterStoreTokenProto.Builder builder = null;

  private boolean viaProto = false;

  public RouterStoreTokenPBImpl() {
    builder = RouterStoreTokenProto.newBuilder();
  }

  public RouterStoreTokenPBImpl(RouterStoreTokenProto storeTokenProto) {
    this.proto = storeTokenProto;
    viaProto = true;
  }

  public RouterStoreTokenProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterStoreTokenProto.newBuilder(proto);
    }
    viaProto = false;
  }

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

  @Override
  public RMDelegationTokenIdentifier getTokenIdentifier() throws IOException {
    RouterStoreTokenProtoOrBuilder p = viaProto ? proto : builder;
    YARNDelegationTokenIdentifierProto identifierProto = p.getTokenIdentifier();
    ByteArrayInputStream in = new ByteArrayInputStream(identifierProto.toByteArray());
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier();
    identifier.readFields(new DataInputStream(in));
    return identifier;
  }

  @Override
  public Long getRenewDate() {
    RouterStoreTokenProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRenewDate();
  }

  @Override
  public void setIdentifier(YARNDelegationTokenIdentifier identifier) {
    maybeInitBuilder();
    if(identifier == null) {
      builder.clearTokenIdentifier();
    }
    builder.setTokenIdentifier(identifier.getProto());
  }

  @Override
  public void setRenewDate(Long renewDate) {
    maybeInitBuilder();
    if(renewDate == null) {
      builder.clearRenewDate();
    }
    builder.setRenewDate(renewDate);
  }
}
