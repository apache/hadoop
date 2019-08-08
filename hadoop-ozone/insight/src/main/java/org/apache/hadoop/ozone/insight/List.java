package org.apache.hadoop.ozone.insight;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import picocli.CommandLine;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Subcommand to list of the available insight points.
 */
@CommandLine.Command(
    name = "list",
    description = "Show available insight points.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class List extends BaseInsightSubcommand implements Callable<Void> {

  @CommandLine.Parameters(defaultValue = "")
  private String selection;

  @Override
  public Void call() throws Exception {

    System.out.println("Available insight points:\n\n");

    Map<String, InsightPoint> insightPoints =
        createInsightPoints(new OzoneConfiguration());
    for (String key : insightPoints.keySet()) {
      System.out.println(String.format("  %-33s    %s", key,
          insightPoints.get(key).getDescription()));
    }
    return null;
  }

}
