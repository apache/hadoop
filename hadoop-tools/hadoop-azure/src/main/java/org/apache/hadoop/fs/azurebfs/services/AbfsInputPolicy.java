package org.apache.hadoop.fs.azurebfs.services;

import java.util.Locale;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_AVRO;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_COLUMNAR;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_CSV;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_HBASE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_JSON;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ORC;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_PARQUET;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_RANDOM;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_VECTOR;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;

public enum AbfsInputPolicy {

  SEQUENTIAL(FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL),
  RANDOM(FS_OPTION_OPENFILE_READ_POLICY_RANDOM),
  VECTORED(FS_OPTION_OPENFILE_READ_POLICY_VECTOR),
  LAYOUT("layout"),
  ADAPTIVE(FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE);

  private final String policy;

  AbfsInputPolicy(String policy) {
    this.policy = policy;
  }

  @Override
  public String toString() {
    return policy;
  }

  String getPolicy() {
    return policy;
  }

  public static AbfsInputPolicy getPolicy(String name, boolean isLayoutPresent) {
    String trimmed = name.trim().toLowerCase(Locale.ENGLISH);
    if (isLayoutPresent) {
      return LAYOUT;
    }
    switch (trimmed) {
    // all these options currently map to random IO.
    case FS_OPTION_OPENFILE_READ_POLICY_HBASE:
    case FS_OPTION_OPENFILE_READ_POLICY_RANDOM:
    case FS_OPTION_OPENFILE_READ_POLICY_COLUMNAR:
    case FS_OPTION_OPENFILE_READ_POLICY_ORC:
    case FS_OPTION_OPENFILE_READ_POLICY_PARQUET:
      return RANDOM;

    // handle the sequential formats.
    case FS_OPTION_OPENFILE_READ_POLICY_AVRO:
    case FS_OPTION_OPENFILE_READ_POLICY_CSV:
    case FS_OPTION_OPENFILE_READ_POLICY_JSON:
    case FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL:
    case FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE:
      return SEQUENTIAL;
    default:
      return ADAPTIVE;
    }
  }
}
