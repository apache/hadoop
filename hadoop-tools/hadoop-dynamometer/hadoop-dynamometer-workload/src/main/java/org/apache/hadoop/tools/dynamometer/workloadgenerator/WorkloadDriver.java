/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tools.dynamometer.workloadgenerator;

import org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditReplayMapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the driver for generating generic workloads against a NameNode under
 * test. It launches a map-only job with a mapper class specified by the
 * {@value MAPPER_CLASS_NAME} argument. See the specific mappers (currently
 * {@link AuditReplayMapper} and {@link CreateFileMapper}) for information on
 * their specific behavior and parameters.
 */
public class WorkloadDriver extends Configured implements Tool {

  private static final Logger LOG =
      LoggerFactory.getLogger(WorkloadDriver.class);

  public static final String START_TIMESTAMP_MS = "start_timestamp_ms";
  public static final String START_TIME_OFFSET = "start_time_offset";
  public static final String START_TIME_OFFSET_DEFAULT = "1m";
  public static final String NN_URI = "nn_uri";
  public static final String MAPPER_CLASS_NAME = "mapper_class_name";

  public int run(String[] args) throws Exception {
    Option helpOption = new Option("h", "help", false,
        "Shows this message. Additionally specify the " + MAPPER_CLASS_NAME
            + " argument to show help for a specific mapper class.");
    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(OptionBuilder.withArgName("NN URI").hasArg()
        .withDescription("URI of the NameNode under test").isRequired()
        .create(NN_URI));
    OptionGroup startTimeOptions = new OptionGroup();
    startTimeOptions.addOption(OptionBuilder.withArgName("Start Timestamp")
        .hasArg().withDescription("Mapper start UTC timestamp in ms")
        .create(START_TIMESTAMP_MS));
    startTimeOptions
        .addOption(OptionBuilder.withArgName("Start Time Offset").hasArg()
            .withDescription("Mapper start time as an offset from current "
                + "time. Human-readable formats accepted, e.g. 10m (default "
                + START_TIME_OFFSET_DEFAULT + ").")
            .create(START_TIME_OFFSET));
    options.addOptionGroup(startTimeOptions);
    Option mapperClassOption = OptionBuilder.withArgName("Mapper ClassName")
        .hasArg()
        .withDescription("Class name of the mapper; must be a WorkloadMapper "
            + "subclass. Mappers supported currently: \n"
            + "1. AuditReplayMapper \n"
            + "2. CreateFileMapper \n"
            + "Fully specified class names are also supported.")
        .isRequired().create(MAPPER_CLASS_NAME);
    options.addOption(mapperClassOption);

    Options helpOptions = new Options();
    helpOptions.addOption(helpOption);
    Option mapperClassNotRequiredOption = (Option) mapperClassOption.clone();
    mapperClassNotRequiredOption.setRequired(false);
    helpOptions.addOption(mapperClassNotRequiredOption);

    CommandLineParser parser = new PosixParser();
    CommandLine cli = parser.parse(helpOptions, args, true);
    if (cli.hasOption("h")) {
      String footer = null;
      if (cli.hasOption(MAPPER_CLASS_NAME)) {
        footer = getMapperUsageInfo(cli.getOptionValue(MAPPER_CLASS_NAME));
      }

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(200, "./start-workload [options]", null, options,
          footer);
      return 1;
    }

    cli = parser.parse(options, args);
    String nnURI = cli.getOptionValue(NN_URI);
    long startTimestampMs;
    if (cli.hasOption(START_TIMESTAMP_MS)) {
      startTimestampMs = Long.parseLong(cli.getOptionValue(START_TIMESTAMP_MS));
    } else {
      // Leverage the human-readable time parsing capabilities of Configuration
      String tmpConfKey = "___temp_config_property___";
      Configuration tmpConf = new Configuration();
      tmpConf.set(tmpConfKey,
          cli.getOptionValue(START_TIME_OFFSET, START_TIME_OFFSET_DEFAULT));
      startTimestampMs = tmpConf.getTimeDuration(tmpConfKey, 0,
          TimeUnit.MILLISECONDS) + System.currentTimeMillis();
    }
    Class<? extends WorkloadMapper<?, ?, ?, ?>> mapperClass = getMapperClass(
        cli.getOptionValue(MAPPER_CLASS_NAME));
    if (!mapperClass.newInstance().verifyConfigurations(getConf())) {
      System.err
          .println(getMapperUsageInfo(cli.getOptionValue(MAPPER_CLASS_NAME)));
      return 1;
    }

    Job job = getJobForSubmission(getConf(), nnURI, startTimestampMs,
        mapperClass);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static Job getJobForSubmission(Configuration baseConf, String nnURI,
      long startTimestampMs, Class<? extends WorkloadMapper<?, ?, ?, ?>>
      mapperClass) throws IOException, InstantiationException,
      IllegalAccessException {
    Configuration conf = new Configuration(baseConf);
    conf.set(NN_URI, nnURI);
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);

    String startTimeString = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss z")
        .format(new Date(startTimestampMs));
    LOG.info("The workload will start at " + startTimestampMs + " ms ("
        + startTimeString + ")");
    conf.setLong(START_TIMESTAMP_MS, startTimestampMs);

    Job job = Job.getInstance(conf, "Dynamometer Workload Driver");
    job.setJarByClass(mapperClass);
    job.setMapperClass(mapperClass);
    mapperClass.newInstance().configureJob(job);

    return job;
  }

  public static void main(String[] args) throws Exception {
    WorkloadDriver driver = new WorkloadDriver();
    System.exit(ToolRunner.run(driver, args));
  }

  // The cast is actually checked via isAssignableFrom but the compiler doesn't
  // recognize this
  @SuppressWarnings("unchecked")
  private Class<? extends WorkloadMapper<?, ?, ?, ?>> getMapperClass(
      String className) {
    String[] potentialQualifiedClassNames = {
        WorkloadDriver.class.getPackage().getName() + "." + className,
        AuditReplayMapper.class.getPackage().getName() + "." + className,
        className
    };
    for (String qualifiedClassName : potentialQualifiedClassNames) {
      Class<?> mapperClass;
      try {
        mapperClass = getConf().getClassByName(qualifiedClassName);
      } catch (ClassNotFoundException cnfe) {
        continue;
      }
      if (!WorkloadMapper.class.isAssignableFrom(mapperClass)) {
        throw new IllegalArgumentException(className + " is not a subclass of "
            + WorkloadMapper.class.getCanonicalName());
      }
      return (Class<? extends WorkloadMapper<?, ?, ?, ?>>) mapperClass;
    }
    throw new IllegalArgumentException("Unable to find workload mapper class: "
        + className);
  }

  private String getMapperUsageInfo(String mapperClassName)
      throws ClassNotFoundException, InstantiationException,
      IllegalAccessException {
    WorkloadMapper<?, ?, ?, ?> mapper = getMapperClass(mapperClassName)
        .newInstance();
    StringBuilder builder = new StringBuilder("Usage for ");
    builder.append(mapper.getClass().getSimpleName());
    builder.append(":\n");
    builder.append(mapper.getDescription());
    for (String configDescription : mapper.getConfigDescriptions()) {
      builder.append("\n    ");
      builder.append(configDescription);
    }
    builder.append("\nConfiguration parameters can be set at the ");
    builder.append("_start_ of the argument list like:\n");
    builder.append("  -Dconfiguration.key=configurationValue");

    return builder.toString();
  }

}
