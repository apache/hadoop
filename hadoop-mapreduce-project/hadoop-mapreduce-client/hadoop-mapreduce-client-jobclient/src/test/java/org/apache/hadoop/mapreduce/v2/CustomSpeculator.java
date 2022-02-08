package org.apache.hadoop.mapreduce.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.app.speculate.DefaultSpeculator;
import org.apache.hadoop.util.ApplicationClassLoader;

public class CustomSpeculator extends DefaultSpeculator {
  public CustomSpeculator(Configuration conf, AppContext context) {
    super(conf, context);
    verifyClassLoader(getClass());
  }

  /**
   * Verifies that the class was loaded by the job classloader if it is in the
   * context of the MRAppMaster, and if not throws an exception to fail the
   * job.
   */
  private void verifyClassLoader(Class<?> cls) {
    // to detect that it is instantiated in the context of the MRAppMaster, we
    // inspect the stack trace and determine a caller is MRAppMaster
    for (StackTraceElement e : new Throwable().getStackTrace()) {
      if (e.getClassName().equals(MRAppMaster.class.getName()) &&
          !(cls.getClassLoader() instanceof ApplicationClassLoader)) {
        throw new ExceptionInInitializerError("incorrect classloader used");
      }
    }
  }
}
