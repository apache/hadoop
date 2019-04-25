package org.apache.hadoop.hdds.conf;

import java.io.StringWriter;

import org.junit.Assert;
import org.junit.Test;

public class TestConfigFileAppender {

  @Test
  public void testInit() {
    ConfigFileAppender appender = new ConfigFileAppender();

    appender.init();

    appender.addConfig("hadoop.scm.enabled", "true", "desc",
        new ConfigTag[] {ConfigTag.OZONE, ConfigTag.SECURITY});

    StringWriter builder = new StringWriter();
    appender.write(builder);

    Assert.assertTrue("Generated config should contain property key entry",
        builder.toString().contains("<name>hadoop.scm.enabled</name>"));

    Assert.assertTrue("Generated config should contain tags",
        builder.toString().contains("<tag>OZONE, SECURITY</tag>"));
  }
}