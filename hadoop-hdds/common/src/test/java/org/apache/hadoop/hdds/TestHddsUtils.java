package org.apache.hadoop.hdds;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

public class TestHddsUtils {

  @Test
  public void testGetHostName() {
    Assert.assertEquals(Optional.of("localhost"),
        HddsUtils.getHostName("localhost:1234"));

    Assert.assertEquals(Optional.of("localhost"),
        HddsUtils.getHostName("localhost"));

    Assert.assertEquals(Optional.empty(),
        HddsUtils.getHostName(":1234"));
  }

}