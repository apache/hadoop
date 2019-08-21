package org.apache.hadoop.ozone.insight;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.insight.Component.Type;

import picocli.CommandLine;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * Subcommand to show configuration values/documentation.
 */
@CommandLine.Command(
    name = "config",
    description = "Show configuration for a specific subcomponents",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ConfigurationSubCommand extends BaseInsightSubcommand
    implements Callable<Void> {

  @CommandLine.Parameters(description = "Name of the insight point (use list "
      + "to check the available options)")
  private String insightName;

  @Override
  public Void call() throws Exception {
    InsightPoint insight =
        getInsight(getInsightCommand().createOzoneConfiguration(), insightName);
    System.out.println(
        "Configuration for `" + insightName + "` (" + insight.getDescription()
            + ")");
    System.out.println();
    for (Class clazz : insight.getConfigurationClasses()) {
      showConfig(clazz);

    }
    return null;
  }

  private void showConfig(Class clazz) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.addResource(getHost(conf, new Component(Type.SCM)) + "/conf");
    ConfigGroup configGroup =
        (ConfigGroup) clazz.getAnnotation(ConfigGroup.class);
    if (configGroup == null) {
      return;
    }

    String prefix = configGroup.prefix();

    for (Method method : clazz.getMethods()) {
      if (method.isAnnotationPresent(Config.class)) {
        Config config = method.getAnnotation(Config.class);
        String key = prefix + "." + config.key();
        System.out.println(">>> " + key);
        System.out.println("       default: " + config.defaultValue());
        System.out.println("       current: " + conf.get(key));
        System.out.println();
        System.out.println(config.description());
        System.out.println();
        System.out.println();

      }
    }

  }

}
