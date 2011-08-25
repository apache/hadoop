package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import java.util.List;

public interface GetDiagnosticsResponse {
  public abstract List<String> getDiagnosticsList();
  public abstract String getDiagnostics(int index);
  public abstract int getDiagnosticsCount();
  
  public abstract void addAllDiagnostics(List<String> diagnostics);
  public abstract void addDiagnostics(String diagnostic);
  public abstract void removeDiagnostics(int index);
  public abstract void clearDiagnostics();

}
