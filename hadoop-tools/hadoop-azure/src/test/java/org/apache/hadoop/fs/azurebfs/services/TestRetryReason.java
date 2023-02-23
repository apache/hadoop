package org.apache.hadoop.fs.azurebfs.services;

import java.net.SocketException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestRetryReason {

  @Test
  public void test4xxStatusRetryReason() {
    Assertions.assertThat(RetryReason.getAbbreviation(null, 403, null))
        .describedAs("Abbreviation for 4xx should be equal to 4xx")
        .isEqualTo("403");
  }

  @Test
  public void testConnectionResetRetryReason() {
    SocketException connReset = new SocketException("connection reset by peer");
    Assertions.assertThat(RetryReason.getAbbreviation(connReset, null, null)).describedAs("");
  }
}
