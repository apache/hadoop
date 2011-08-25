package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.proto.YarnProtos.URLProto;
import org.apache.hadoop.yarn.proto.YarnProtos.URLProtoOrBuilder;


    
public class URLPBImpl extends ProtoBase<URLProto> implements URL {
  URLProto proto = URLProto.getDefaultInstance();
  URLProto.Builder builder = null;
  boolean viaProto = false;
  
  public URLPBImpl() {
    builder = URLProto.newBuilder();
  }

  public URLPBImpl(URLProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public URLProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = URLProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getFile() {
    URLProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFile()) {
      return null;
    }
    return (p.getFile());
  }

  @Override
  public void setFile(String file) {
    maybeInitBuilder();
    if (file == null) { 
      builder.clearFile();
      return;
    }
    builder.setFile((file));
  }
  @Override
  public String getScheme() {
    URLProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasScheme()) {
      return null;
    }
    return (p.getScheme());
  }

  @Override
  public void setScheme(String scheme) {
    maybeInitBuilder();
    if (scheme == null) { 
      builder.clearScheme();
      return;
    }
    builder.setScheme((scheme));
  }
  @Override
  public String getHost() {
    URLProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHost()) {
      return null;
    }
    return (p.getHost());
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    if (host == null) { 
      builder.clearHost();
      return;
    }
    builder.setHost((host));
  }
  @Override
  public int getPort() {
    URLProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPort());
  }

  @Override
  public void setPort(int port) {
    maybeInitBuilder();
    builder.setPort((port));
  }



}  
