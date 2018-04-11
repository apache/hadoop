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
package org.apache.hadoop.hdfs.server.federation.store.records.impl.pb;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.MountTableRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.MountTableRecordProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.MountTableRecordProto.DestOrder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.MountTableRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoteLocationProto;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.QuotaUsageProto;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.RouterAdminServer;
import org.apache.hadoop.hdfs.server.federation.router.RouterPermissionChecker;
import org.apache.hadoop.hdfs.server.federation.router.RouterQuotaUsage;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;

import com.google.protobuf.Message;

/**
 * Protobuf implementation of the MountTable record.
 */
public class MountTablePBImpl extends MountTable implements PBRecord {

  private FederationProtocolPBTranslator<MountTableRecordProto, Builder,
      MountTableRecordProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<MountTableRecordProto, Builder,
              MountTableRecordProtoOrBuilder>(MountTableRecordProto.class);

  public MountTablePBImpl() {
  }

  public MountTablePBImpl(MountTableRecordProto proto) {
    this.setProto(proto);
  }

  @Override
  public MountTableRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Message proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    this.translator.readInstance(base64String);
  }

  @Override
  public String getSourcePath() {
    MountTableRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();
    if (!proto.hasSrcPath()) {
      return null;
    }
    return proto.getSrcPath();
  }

  @Override
  public void setSourcePath(String path) {
    Builder builder = this.translator.getBuilder();
    if (path == null) {
      builder.clearSrcPath();
    } else {
      builder.setSrcPath(path);
    }
  }

  @Override
  public List<RemoteLocation> getDestinations() {
    MountTableRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();
    if (proto.getDestinationsCount() == 0) {
      return null;
    }

    final List<RemoteLocation> ret = new LinkedList<>();
    final List<RemoteLocationProto> destList = proto.getDestinationsList();
    for (RemoteLocationProto dest : destList) {
      String nsId = dest.getNameserviceId();
      String path = dest.getPath();
      RemoteLocation loc = new RemoteLocation(nsId, path, getSourcePath());
      ret.add(loc);
    }
    return ret;
  }

  @Override
  public void setDestinations(final List<RemoteLocation> dests) {
    Builder builder = this.translator.getBuilder();
    builder.clearDestinations();
    for (RemoteLocation dest : dests) {
      RemoteLocationProto.Builder itemBuilder =
          RemoteLocationProto.newBuilder();
      String nsId = dest.getNameserviceId();
      String path = dest.getDest();
      itemBuilder.setNameserviceId(nsId);
      itemBuilder.setPath(path);
      RemoteLocationProto item = itemBuilder.build();
      builder.addDestinations(item);
    }
  }

  @Override
  public boolean addDestination(String nsId, String path) {
    // Check if the location is already there
    List<RemoteLocation> dests = getDestinations();
    for (RemoteLocation dest : dests) {
      if (dest.getNameserviceId().equals(nsId) && dest.getDest().equals(path)) {
        return false;
      }
    }

    // Add it to the existing list
    Builder builder = this.translator.getBuilder();
    RemoteLocationProto.Builder itemBuilder =
        RemoteLocationProto.newBuilder();
    itemBuilder.setNameserviceId(nsId);
    itemBuilder.setPath(path);
    RemoteLocationProto item = itemBuilder.build();
    builder.addDestinations(item);
    return true;
  }

  @Override
  public void setDateModified(long time) {
    this.translator.getBuilder().setDateModified(time);
  }

  @Override
  public long getDateModified() {
    return this.translator.getProtoOrBuilder().getDateModified();
  }

  @Override
  public void setDateCreated(long time) {
    this.translator.getBuilder().setDateCreated(time);
  }

  @Override
  public long getDateCreated() {
    return this.translator.getProtoOrBuilder().getDateCreated();
  }

  @Override
  public boolean isReadOnly() {
    MountTableRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();
    if (!proto.hasReadOnly()) {
      return false;
    }
    return proto.getReadOnly();
  }

  @Override
  public void setReadOnly(boolean ro) {
    this.translator.getBuilder().setReadOnly(ro);
  }

  @Override
  public DestinationOrder getDestOrder() {
    MountTableRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();
    return convert(proto.getDestOrder());
  }

  @Override
  public void setDestOrder(DestinationOrder order) {
    Builder builder = this.translator.getBuilder();
    if (order == null) {
      builder.clearDestOrder();
    } else {
      builder.setDestOrder(convert(order));
    }
  }

  @Override
  public String getOwnerName() {
    MountTableRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();
    if (!proto.hasOwnerName()) {
      return RouterAdminServer.getSuperUser();
    }
    return proto.getOwnerName();
  }

  @Override
  public void setOwnerName(String owner) {
    Builder builder = this.translator.getBuilder();
    if (owner == null) {
      builder.clearOwnerName();
    } else {
      builder.setOwnerName(owner);
    }
  }

  @Override
  public String getGroupName() {
    MountTableRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();
    if (!proto.hasGroupName()) {
      return RouterAdminServer.getSuperGroup();
    }
    return proto.getGroupName();
  }

  @Override
  public void setGroupName(String group) {
    Builder builder = this.translator.getBuilder();
    if (group == null) {
      builder.clearGroupName();
    } else {
      builder.setGroupName(group);
    }
  }

  @Override
  public FsPermission getMode() {
    MountTableRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();
    short mode = RouterPermissionChecker.MOUNT_TABLE_PERMISSION_DEFAULT;
    if (proto.hasMode()) {
      mode = (short) proto.getMode();
    }
    return new FsPermission(mode);
  }

  @Override
  public void setMode(FsPermission mode) {
    Builder builder = this.translator.getBuilder();
    if (mode == null) {
      builder.clearMode();
    } else {
      builder.setMode(mode.toShort());
    }
  }

  @Override
  public RouterQuotaUsage getQuota() {
    MountTableRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();

    long nsQuota = HdfsConstants.QUOTA_DONT_SET;
    long nsCount = RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT;
    long ssQuota = HdfsConstants.QUOTA_DONT_SET;
    long ssCount = RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT;
    if (proto.hasQuota()) {
      QuotaUsageProto quotaProto = proto.getQuota();
      nsQuota = quotaProto.getQuota();
      nsCount = quotaProto.getFileAndDirectoryCount();
      ssQuota = quotaProto.getSpaceQuota();
      ssCount = quotaProto.getSpaceConsumed();
    }

    RouterQuotaUsage.Builder builder = new RouterQuotaUsage.Builder()
        .quota(nsQuota).fileAndDirectoryCount(nsCount).spaceQuota(ssQuota)
        .spaceConsumed(ssCount);
    return builder.build();
  }

  @Override
  public void setQuota(RouterQuotaUsage quota) {
    Builder builder = this.translator.getBuilder();
    if (quota == null) {
      builder.clearQuota();
    } else {
      QuotaUsageProto quotaUsage = QuotaUsageProto.newBuilder()
          .setFileAndDirectoryCount(quota.getFileAndDirectoryCount())
          .setQuota(quota.getQuota()).setSpaceConsumed(quota.getSpaceConsumed())
          .setSpaceQuota(quota.getSpaceQuota()).build();
      builder.setQuota(quotaUsage);
    }
  }

  private DestinationOrder convert(DestOrder order) {
    switch (order) {
    case LOCAL:
      return DestinationOrder.LOCAL;
    case RANDOM:
      return DestinationOrder.RANDOM;
    case HASH_ALL:
      return DestinationOrder.HASH_ALL;
    case SPACE:
      return DestinationOrder.SPACE;
    default:
      return DestinationOrder.HASH;
    }
  }

  private DestOrder convert(DestinationOrder order) {
    switch (order) {
    case LOCAL:
      return DestOrder.LOCAL;
    case RANDOM:
      return DestOrder.RANDOM;
    case HASH_ALL:
      return DestOrder.HASH_ALL;
    case SPACE:
      return DestOrder.SPACE;
    default:
      return DestOrder.HASH;
    }
  }
}