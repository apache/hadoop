package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class GetDiagnosticsResponsePBImpl extends ProtoBase<GetDiagnosticsResponseProto> implements GetDiagnosticsResponse {
  GetDiagnosticsResponseProto proto = GetDiagnosticsResponseProto.getDefaultInstance();
  GetDiagnosticsResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private List<String> diagnostics = null;
  
  
  public GetDiagnosticsResponsePBImpl() {
    builder = GetDiagnosticsResponseProto.newBuilder();
  }

  public GetDiagnosticsResponsePBImpl(GetDiagnosticsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetDiagnosticsResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.diagnostics != null) {
      addDiagnosticsToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetDiagnosticsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public List<String> getDiagnosticsList() {
    initDiagnostics();
    return this.diagnostics;
  }
  @Override
  public String getDiagnostics(int index) {
    initDiagnostics();
    return this.diagnostics.get(index);
  }
  @Override
  public int getDiagnosticsCount() {
    initDiagnostics();
    return this.diagnostics.size();
  }
  
  private void initDiagnostics() {
    if (this.diagnostics != null) {
      return;
    }
    GetDiagnosticsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<String> list = p.getDiagnosticsList();
    this.diagnostics = new ArrayList<String>();

    for (String c : list) {
      this.diagnostics.add(c);
    }
  }
  
  @Override
  public void addAllDiagnostics(final List<String> diagnostics) {
    if (diagnostics == null)
      return;
    initDiagnostics();
    this.diagnostics.addAll(diagnostics);
  }
  
  private void addDiagnosticsToProto() {
    maybeInitBuilder();
    builder.clearDiagnostics();
    if (diagnostics == null) 
      return;
    builder.addAllDiagnostics(diagnostics);
  }
  @Override
  public void addDiagnostics(String diagnostics) {
    initDiagnostics();
    this.diagnostics.add(diagnostics);
  }
  @Override
  public void removeDiagnostics(int index) {
    initDiagnostics();
    this.diagnostics.remove(index);
  }
  @Override
  public void clearDiagnostics() {
    initDiagnostics();
    this.diagnostics.clear();
  }

}  
