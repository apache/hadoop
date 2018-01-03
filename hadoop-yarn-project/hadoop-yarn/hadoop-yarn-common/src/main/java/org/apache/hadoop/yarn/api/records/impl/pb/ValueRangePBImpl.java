package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ValueRange;
import org.apache.hadoop.yarn.proto.YarnProtos.ValueRangeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ValueRangeProtoOrBuilder;

public class ValueRangePBImpl extends ValueRange {

  ValueRangeProto proto = ValueRangeProto.getDefaultInstance();
  ValueRangeProto.Builder builder = null;
  boolean viaProto = false;
  int begin, end = -1;

  public ValueRangePBImpl(ValueRangeProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ValueRangePBImpl() {
  }

  public ValueRangeProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int getBegin() {
    initLocalRange();
    return begin;
  }

  @Override
  public int getEnd() {
    initLocalRange();
    return end;
  }

  @Override
  public void setBegin(int value) {
    begin = value;
  }

  @Override
  public void setEnd(int value) {
    end = value;
  }

  @Override
  public boolean isLessOrEqual(ValueRange other) {
    if (this.getBegin() >= other.getBegin() && this.getEnd() <= other.getEnd()) {
      return true;
    }
    return false;
  }

  private void maybeInitBuilder() {
    if (viaProto) {
      builder = ValueRangeProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (begin != -1 && end != -1) {
      addRangeToProto();
    }
  }

  private void addRangeToProto() {
    maybeInitBuilder();
    if (begin == -1 && end == -1)
      return;
    if (builder == null) {
      builder = ValueRangeProto.newBuilder();
    }
    builder.setBegin(begin);
    builder.setEnd(end);
  }

  private void initLocalRange() {
    if (begin != -1 && end != -1) {
      return;
    }
    if (!viaProto && builder == null) {
      builder = ValueRangeProto.newBuilder();
    }
    ValueRangeProtoOrBuilder p = viaProto ? proto : builder;
    begin = p.getBegin();
    end = p.getEnd();
  }

}
