package org.apache.hadoop.fs.s3a.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESS_GRANTS_FALLBACK_TO_IAM_ENABLED;

public class S3AccessGrantsUtil {

  protected static final Logger LOG =
      LoggerFactory.getLogger(S3AccessGrantsUtil.class);

  private static final LogExactlyOnce LOG_EXACTLY_ONCE = new LogExactlyOnce(LOG);
  private static final String S3AG_PLUGIN_CLASSNAME =
      "software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin";

  /**
   * S3 Access Grants plugin availability.
   */
  private static final boolean S3AG_PLUGIN_FOUND = checkForS3AGPlugin();

  private static boolean checkForS3AGPlugin() {
    try {
      ClassLoader cl = DefaultS3ClientFactory.class.getClassLoader();
      cl.loadClass(S3AG_PLUGIN_CLASSNAME);
      LOG.debug("S3AG plugin class {} found", S3AG_PLUGIN_CLASSNAME);
      return true;
    } catch (Exception e) {
      LOG.debug("S3AG plugin class {} not found", S3AG_PLUGIN_CLASSNAME, e);
      return false;
    }
  }

  /**
   * Is the S3AG plugin available?
   * @return true if it was found in the classloader
   */
  private static synchronized boolean isS3AGPluginAvailable() {
    return S3AG_PLUGIN_FOUND;
  }

  public static <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> void
      applyS3AccessGrantsConfigurations(BuilderT builder, Configuration conf) {
    if (isS3AGPluginAvailable()) {
      boolean s3agFallbackEnabled = conf.getBoolean(
          AWS_S3_ACCESS_GRANTS_FALLBACK_TO_IAM_ENABLED, false);
      S3AccessGrantsPlugin accessGrantsPlugin =
          S3AccessGrantsPlugin.builder().enableFallback(s3agFallbackEnabled).build();
      builder.addPlugin(accessGrantsPlugin);
      LOG_EXACTLY_ONCE.info("s3ag plugin is added to s3 client with fallback: {}", s3agFallbackEnabled);
    } else {
      LOG_EXACTLY_ONCE.warn("s3ag plugin is not available.");
    }
  }
}
