package org.apache.hadoop.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;

public class FakeRenewer extends TokenRenewer {
  static Token<?> lastRenewed = null;
  static Token<?> lastCanceled = null;
  static final Text KIND = new Text("TESTING-TOKEN-KIND");

  @Override
  public boolean handleKind(Text kind) {
    return FakeRenewer.KIND.equals(kind);
  }

  @Override
  public boolean isManaged(Token<?> token) throws IOException {
    return true;
  }

  @Override
  public long renew(Token<?> token, Configuration conf) {
    lastRenewed = token;
    return 0;
  }

  @Override
  public void cancel(Token<?> token, Configuration conf) {
    lastCanceled = token;
  }

  public static void reset() {
    lastRenewed = null;
    lastCanceled = null;
  }
}