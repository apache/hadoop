package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class FixedSASTokenProvider implements SASTokenProvider {
  private String fixedSASToken;

  public FixedSASTokenProvider(final String fixedSASToken) {
    this.fixedSASToken = fixedSASToken;
  }

  @Override
  public void initialize(final Configuration configuration,
      final String accountName)
      throws IOException {
  }

  @Override
  public String getSASToken(final String account,
      final String fileSystem,
      final String path,
      final String operation) throws IOException {
    return fixedSASToken;
  }
}
