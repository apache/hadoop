package org.apache.hadoop.ozone.insight;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Command line interface to show metrics for a specific component.
 */
@CommandLine.Command(
    name = "metrics",
    aliases = "metric",
    description = "Show available metrics.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class MetricsSubCommand extends BaseInsightSubcommand
    implements Callable<Void> {

  @CommandLine.Parameters(defaultValue = "")
  private String insightName;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf =
        getInsightCommand().createOzoneConfiguration();
    InsightPoint insight =
        getInsight(conf, insightName);
    Set<Component> sources =
        insight.getMetrics().stream().map(MetricGroupDisplay::getComponent)
            .collect(Collectors.toSet());
    Map<Component, List<String>> metrics = getMetrics(conf, sources);
    System.out.println(
        "Metrics for `" + insightName + "` (" + insight.getDescription() + ")");
    System.out.println();
    for (MetricGroupDisplay group : insight.getMetrics()) {
      System.out.println(group.getDescription());
      System.out.println();
      for (MetricDisplay display : group.getMetrics()) {
        System.out.println("  " + display.getDescription() + ": " + selectValue(
            metrics.get(group.getComponent()), display));
      }
      System.out.println();
      System.out.println();

    }
    return null;
  }

  private Map<Component, List<String>> getMetrics(OzoneConfiguration conf,
      Collection<Component> sources) {
    Map<Component, List<String>> result = new HashMap<>();
    for (Component source : sources) {
      result.put(source, getMetrics(conf, source));
    }
    return result;
  }

  private String selectValue(List<String> metrics,
      MetricDisplay metricDisplay) {
    for (String line : metrics) {
      if (line.startsWith(metricDisplay.getId())) {
        boolean filtered = false;
        for (Entry<String, String> filter : metricDisplay.getFilter()
            .entrySet()) {
          if (!line
              .contains(filter.getKey() + "=\"" + filter.getValue() + "\"")) {
            filtered = true;
          }
        }
        if (!filtered) {
          return line.split(" ")[1];
        }
      }
    }
    return "???";
  }

  private List<String> getMetrics(OzoneConfiguration conf,
      Component component) {
    HttpClient client = HttpClientBuilder.create().build();
    HttpGet get = new HttpGet(getHost(conf, component) + "/prom");
    try {
      HttpResponse execute = client.execute(get);
      if (execute.getStatusLine().getStatusCode() != 200) {
        throw new RuntimeException(
            "Can't read prometheus metrics endpoint" + execute.getStatusLine()
                .getStatusCode());
      }
      try (BufferedReader bufferedReader = new BufferedReader(
          new InputStreamReader(execute.getEntity().getContent(),
              StandardCharsets.UTF_8))) {
        return bufferedReader.lines().collect(Collectors.toList());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
