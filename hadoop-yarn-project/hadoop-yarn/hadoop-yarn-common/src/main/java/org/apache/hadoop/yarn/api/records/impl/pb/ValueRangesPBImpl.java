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
package org.apache.hadoop.yarn.api.records.impl.pb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.BitSet;

import com.google.protobuf.ByteString;
import org.apache.hadoop.yarn.api.records.ValueRange;
import org.apache.hadoop.yarn.api.records.ValueRanges;
import org.apache.hadoop.yarn.proto.YarnProtos.ValueRangeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ValueRangesProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ValueRangesProtoOrBuilder;

public class ValueRangesPBImpl extends ValueRanges {

  ValueRangesProto proto = ValueRangesProto.getDefaultInstance();
  ValueRangesProto.Builder builder = null;
  boolean viaProto = false;
  List<ValueRange> ranges = null;
  List<ValueRange> unmodifiableRanges = null;

  private boolean isCoalesced = false;

  private BitSet bitSetStore = null;

  private boolean byteStoreEnable = false;

  /**
   * TODO: we have a plan to compress the bitset if currently still allocate too
   * much memory, like gzip to compress. But seems currenly we get the ideal
   * result, so will re-consider the plan after roll-out to prod bed
   */
  private int byte_store_encode = 0;

  public ValueRangesPBImpl(ValueRangesProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ValueRangesPBImpl() {
    builder = ValueRangesProto.newBuilder();
  }

  public ValueRangesProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  public synchronized void setByteStoreEnable(boolean enable) {
    byteStoreEnable = enable;
  }

  public synchronized boolean isByteStoreEnable() {
    if (ranges != null || bitSetStore != null) {
      return byteStoreEnable;
    }

    ValueRangesProtoOrBuilder p = viaProto ? proto : builder;
    if (p.getByteStoreEnable() || p.hasRangesByteStore()) {
      byteStoreEnable = true;
    }
    return byteStoreEnable;
  }

  public boolean isCoalesced() {
    return isCoalesced;
  }

  public synchronized void setCoalesced(boolean flag) {
    isCoalesced = flag;
  }

  public synchronized BitSet getBitSetStore() {
    initLocalRangesStore();
    if (bitSetStore != null) {
      return (BitSet) bitSetStore.clone();
    }
    return null;
  }

  public synchronized void setBitSetStore(BitSet bitSetStore) {
    this.bitSetStore = (BitSet) bitSetStore.clone();
    byteStoreEnable = true;
  }

  @Override
  public synchronized ByteBuffer getBytesStore() {
    ValueRangesProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasRangesByteStore()) {
      return null;
    }
    ByteBuffer rangesByteBuffer =
        convertFromProtoFormat(p.getRangesByteStore());
    return rangesByteBuffer;
  }

  private void initLocalRangesStore() {
    if (this.ranges != null || this.bitSetStore != null) {
      return;
    }
    isByteStoreEnable();
    if (byteStoreEnable) {
      initLocalBitSetStore();
    } else {
      initLocalRanges();
    }
  }

  private void initLocalBitSetStore() {
    if (this.bitSetStore != null) {
      return;
    }

    ValueRangesProtoOrBuilder p = viaProto ? proto : builder;
    bitSetStore = new BitSet();
    if (!p.hasRangesByteStore()) {
      return;
    }
    ByteBuffer rangesByteBuffer =
        convertFromProtoFormat(p.getRangesByteStore());
    if (rangesByteBuffer != null) {
      bitSetStore = BitSet.valueOf(rangesByteBuffer);
    }
  }

  private void initLocalRanges() {
    if (this.ranges != null) {
      return;
    }
    ValueRangesProtoOrBuilder p = viaProto ? proto : builder;
    List<ValueRangeProto> list = p.getRangesList();
    List<ValueRange> tempRanges = new ArrayList<ValueRange>();
    for (ValueRangeProto a : list) {
      tempRanges.add(convertFromProtoFormat(a));
    }
    assignRanges(tempRanges);
  }

  @Override
  public synchronized int getRangesCount() {
    int result = 0;
    initLocalRangesStore();
    if (bitSetStore != null) {
      List<ValueRange> list = convertFromBitSetToRanges(bitSetStore);
      if (list != null) {
        result = list.size();
      }
    } else {
      result = getRangesList().size();
    }
    return result;
  }

  private void assignRanges(List<ValueRange> value) {
    List<ValueRange> newList = new ArrayList<ValueRange>();
    for (ValueRange range : value) {
      newList.add(range.clone());
    }
    ranges = newList;
    unmodifiableRanges = Collections.unmodifiableList(value);
  }

  @Override
  public synchronized List<ValueRange> getSortedRangesList() {
    initLocalRangesStore();
    List<ValueRange> newList = cloneList(this.getRangesList());
    Collections.sort(newList);
    return newList;
  }

  @Override
  public synchronized List<ValueRange> getRangesList() {
    initLocalRangesStore();
    return unmodifiableRanges;
  }

  @Override
  public synchronized void setRangesList(List<ValueRange> rangesList) {
    if (rangesList == null) {
      maybeInitBuilder();
      builder.clearRanges();
    }
    assignRanges(rangesList);
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ValueRangesProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.ranges != null) {
      addRangesToProto();
    }
    if (byteStoreEnable) {
      addByteStoreEnableToProto();
      addByteStoreToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void addRangesToProto() {
    maybeInitBuilder();
    if (ranges == null || ranges.isEmpty()) {
      builder.clearRanges();
      return;
    }
    List<ValueRangeProto> list = new LinkedList<>();
    for (ValueRange range : ranges) {
      list.add(convertToProtoFormat(range));
    }
    builder.clearRanges();
    builder.addAllRanges(list);
  }

  private void addByteStoreEnableToProto() {
    maybeInitBuilder();
    builder.setByteStoreEnable(byteStoreEnable);
  }

  private void addByteStoreToProto() {
    if (this.bitSetStore != null) {
      byte[] result = bitSetStore.toByteArray();
      builder.setRangesByteStore(convertToProtoFormat(ByteBuffer.wrap(result)));
    }
  }

  protected final ByteBuffer convertFromProtoFormat(ByteString byteString) {
    return ProtoUtils.convertFromProtoFormat(byteString);
  }

  protected final ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
    return ProtoUtils.convertToProtoFormat(byteBuffer);
  }

  private static ValueRangePBImpl convertFromProtoFormat(ValueRangeProto a) {
    return new ValueRangePBImpl(a);
  }

  private static ValueRangeProto convertToProtoFormat(ValueRange t) {
    return ((ValueRangePBImpl) t).getProto();
  }

}
