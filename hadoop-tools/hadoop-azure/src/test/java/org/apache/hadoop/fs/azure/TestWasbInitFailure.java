package org.apache.hadoop.fs.azure;

import java.net.URI;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test to verify WASB initialization fails as expected.
 */
public class TestWasbInitFailure {

  /**
   * Test that initialization of Non-secure WASB FileSystem fails as expected.
   * @throws Exception on any failure
   */
  @Test
  public void testWasbInitFails() throws Exception {
    URI wasbUri = URI.create("wasb://container@account.blob.core.windows.net");
    assertFailure(wasbUri);
  }

  /**
   * Test that initialization of Secure WASB FileSystem fails as expected.
   * @throws Exception on any failure
   */
  @Test
  public void testSecureWasbInitFails() throws Exception {
    URI wasbUri = URI.create("wasbs://container@account.blob.core.windows.net");
    assertFailure(wasbUri);
  }

  private void assertFailure(URI uri) throws Exception {
    Configuration conf = new Configuration();
    IllegalArgumentException ex = intercept(IllegalArgumentException.class, () -> {
      FileSystem.newInstance(uri, conf).close();
    });
    Assertions.assertThat(ex.getMessage())
        .contains("WASB Driver using wasb(s) schema is No longer Supported.");

    ex = intercept(IllegalArgumentException.class, () -> {
      FileSystem.get(uri, conf).close();
    });
    Assertions.assertThat(ex.getMessage())
        .contains("WASB Driver using wasb(s) schema is No longer Supported.");
  }
}
