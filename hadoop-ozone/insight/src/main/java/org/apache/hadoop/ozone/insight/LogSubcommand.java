/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.insight.LoggerSource.Level;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import picocli.CommandLine;

/**
 * Subcommand to display log.
 */
@CommandLine.Command(
    name = "log",
    aliases = "logs",
    description = "Show log4j events related to the insight point",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class LogSubcommand extends BaseInsightSubCommand
    implements Callable<Void> {

  @CommandLine.Parameters(description = "Name of the insight point (use list "
      + "to check the available options)")
  private String insightName;

  @CommandLine.Option(names = "-v", description = "Enable verbose mode to "
      + "show more information / detailed message")
  private boolean verbose;

  @CommandLine.Option(names = "-f", description = "Define filters to scope "
      + "the output (eg. -f datanode=_1234_datanode_id)")
  private Map<String, String> filters;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf =
        getInsightCommand().createOzoneConfiguration();
    InsightPoint insight =
        getInsight(conf, insightName);

    List<LoggerSource> loggers = insight.getRelatedLoggers(verbose, filters);

    for (LoggerSource logger : loggers) {
      setLogLevel(conf, logger.getLoggerName(), logger.getComponent(),
          logger.getLevel());
    }

    Set<Component> sources = loggers.stream().map(LoggerSource::getComponent)
        .collect(Collectors.toSet());
    try {
      streamLog(conf, sources, loggers,
          (logLine) -> insight.filterLog(filters, logLine));
    } finally {
      for (LoggerSource logger : loggers) {
        setLogLevel(conf, logger.getLoggerName(), logger.getComponent(),
            Level.INFO);
      }
    }
    return null;
  }

  /**
   * Stream log from multiple endpoint.
   *
   * @param conf           Configuration (to find the log endpoints)
   * @param sources        Components to connect to (like scm, om...)
   * @param relatedLoggers loggers to display
   * @param filter         any additional filter
   */
  private void streamLog(OzoneConfiguration conf, Set<Component> sources,
      List<LoggerSource> relatedLoggers, Predicate<String> filter) {
    List<Thread> loggers = new ArrayList<>();
    for (Component sourceComponent : sources) {
      loggers.add(new Thread(
          () -> streamLog(conf, sourceComponent, relatedLoggers, filter)));
    }
    for (Thread thread : loggers) {
      thread.start();
    }
    for (Thread thread : loggers) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void streamLog(OzoneConfiguration conf, Component logComponent,
      List<LoggerSource> loggers, Predicate<String> filter) {
    HttpClient client = HttpClientBuilder.create().build();

    HttpGet get = new HttpGet(getHost(conf, logComponent) + "/logstream");
    try {
      HttpResponse execute = client.execute(get);
      try (BufferedReader bufferedReader = new BufferedReader(
          new InputStreamReader(execute.getEntity().getContent(),
              StandardCharsets.UTF_8))) {
        bufferedReader.lines()
            .filter(line -> {
              for (LoggerSource logger : loggers) {
                if (line.contains(logger.getLoggerName()) && filter
                    .test(line)) {
                  return true;
                }
              }
              return false;
            })
            .map(this::processLogLine)
            .map(l -> "[" + logComponent.prefix() + "] " + l)
            .forEach(System.out::println);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String processLogLine(String line) {
    Pattern p = Pattern.compile("<json>(.*)</json>");
    Matcher m = p.matcher(line);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, "\n" + m.group(1).replaceAll("\\\\n", "\n"));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  private void setLogLevel(OzoneConfiguration conf, String name,
      Component component, LoggerSource.Level level) {
    HttpClient client = HttpClientBuilder.create().build();

    String request = String
        .format("/logLevel?log=%s&level=%s", name,
            level);
    String hostName = getHost(conf, component);
    HttpGet get = new HttpGet(hostName + request);
    try {
      HttpResponse execute = client.execute(get);
      if (execute.getStatusLine().getStatusCode() != 200) {
        throw new RuntimeException(
            "Can't set the log level: " + hostName + " -> HTTP " + execute
                .getStatusLine().getStatusCode());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
