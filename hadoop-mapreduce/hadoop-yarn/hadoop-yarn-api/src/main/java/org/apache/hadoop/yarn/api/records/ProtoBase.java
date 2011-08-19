package org.apache.hadoop.yarn.api.records;

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.util.ProtoUtils;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

public abstract class ProtoBase <T extends Message> {
  
  public abstract T getProto();

  //TODO Force a comparator?
  
  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }
  
  @Override
  public String toString() {
    return getProto().toString().replaceAll("\\n", ", ").replaceAll("\\s+", " ");
  }
  
  protected final ByteBuffer convertFromProtoFormat(ByteString byteString) {
    return ProtoUtils.convertFromProtoFormat(byteString);
  }

  protected final ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
    return ProtoUtils.convertToProtoFormat(byteBuffer);
  }
}
