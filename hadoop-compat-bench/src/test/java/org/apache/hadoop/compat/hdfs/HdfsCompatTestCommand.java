package org.apache.hadoop.compat.hdfs;


import org.apache.hadoop.compat.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Field;

public class HdfsCompatTestCommand extends HdfsCompatCommand {
  public HdfsCompatTestCommand(String uri, String suiteName, Configuration conf) {
    super(uri, suiteName, conf);
  }

  @Override
  public void initialize() throws IOException, ReflectiveOperationException {
    super.initialize();
    Field shellField = HdfsCompatCommand.class.getDeclaredField("shell");
    shellField.setAccessible(true);
    HdfsCompatShellScope shell = (HdfsCompatShellScope) shellField.get(this);
    if (shell != null) {
      Field envField = shell.getClass().getDeclaredField("env");
      envField.setAccessible(true);
      HdfsCompatEnvironment env = (HdfsCompatEnvironment) envField.get(shell);
      Field suiteField = HdfsCompatCommand.class.getDeclaredField("suite");
      suiteField.setAccessible(true);
      HdfsCompatSuite suite = (HdfsCompatSuite) suiteField.get(this);
      shellField.set(this, getShellScope(env, suite));
    }
  }

  protected HdfsCompatShellScope getShellScope(HdfsCompatEnvironment env, HdfsCompatSuite suite) {
    return new HdfsCompatTestShellScope(env, suite);
  }
}