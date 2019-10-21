/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Parses arguments passed to the FS-&gt;CS converter.
 * If the arguments are valid, it calls the converter itself.
 *
 */
public class FSConfigToCSConfigArgumentHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSConfigToCSConfigArgumentHandler.class);
  private final FSConfigToCSConfigConverter converter;

  public FSConfigToCSConfigArgumentHandler(FSConfigToCSConfigConverter
      converter) {
    this.converter = converter;
  }

  /**
   * Represents options for the converter CLI.
   *
   */
  public enum CliOption {
    YARN_SITE("yarn-site.xml", "y", "yarnsiteconfig",
        "Path to a valid yarn-site.xml config file", true, true),

    // fair-scheduler.xml is not mandatory
    // if FairSchedulerConfiguration.ALLOCATION_FILE is defined in yarn-site.xml
    FAIR_SCHEDULER("fair-scheduler.xml", "f", "fsconfig",
        "Path to a valid fair-scheduler.xml config file", false, true),
    CONVERSION_RULES("conversion rules config file", "r", "rulesconfig",
        "Optional parameter. If given, should specify a valid path to the " +
            "conversion rules file (property format).", false, true),
    CONSOLE_MODE("console mode", "p", "print",
        "If defined, the converted configuration will " +
            "only be emitted to the console.", false, false),
    CLUSTER_RESOURCE("cluster resource", "c", "cluster-resource",
        "Needs to be given if maxResources is defined as percentages " +
            "for any queue, otherwise this parameter can be omitted.",
            false, true),
    OUTPUT_DIR("output directory", "o", "output-directory",
        "Output directory for yarn-site.xml and" +
            " capacity-scheduler.xml files." +
            "Must have write permission for user who is running this script.",
            true, true);

    private final String name;
    private final String shortSwitch;
    private final String longSwitch;
    private final String description;
    private final boolean required;
    private final boolean hasArg;

    CliOption(String name, String shortSwitch, String longSwitch,
        String description, boolean required, boolean hasArg) {
      this.name = name;
      this.shortSwitch = shortSwitch;
      this.longSwitch = longSwitch;
      this.description = description;
      this.required = required;
      this.hasArg = hasArg;
    }

    public Option createCommonsCliOption() {
      Option option = new Option(shortSwitch, longSwitch, hasArg, description);
      option.setRequired(required);
      return option;
    }

    public String getAsArgumentString() {
      return shortSwitch + "|" + longSwitch + ": " + description;
    }
  }

  public int parseAndConvert(String[] args) throws Exception {
    Options opts = createOptions();

    try {
      CommandLine cliParser = new GnuParser().parse(opts, args);
      checkOptionPresent(cliParser, CliOption.YARN_SITE);
      checkOptionPresent(cliParser, CliOption.OUTPUT_DIR);

      FSConfigToCSConfigConverterParams params = validateInputFiles(cliParser);
      converter.convert(params);
    } catch (MissingArgumentException e) {
      String msg = "Missing argument for options" + e.getMessage();
      logAndStdErr(e, msg);
      return -1;
    } catch (PreconditionException e) {
      String msg = "Cannot start FS config conversion due to the following"
          + " precondition error: " + e.getMessage();
      logAndStdErr(e, msg);
      return -1;
    } catch (UnsupportedPropertyException e) {
      String msg = "Unsupported property/setting encountered during FS config "
          + "conversion: " + e.getMessage();
      logAndStdErr(e, msg);
      return -1;
    } catch (ConversionException | IllegalArgumentException e) {
      String msg = "Fatal error during FS config conversion: " + e.getMessage();
      logAndStdErr(e, msg);
      return -1;
    }

    return 0;
  }

  private void logAndStdErr(Exception e, String msg) {
    LOG.error(msg, e);
    System.err.println(msg);
  }

  private Options createOptions() {
    Options opts = new Options();

    for (CliOption cliOption : CliOption.values()) {
      opts.addOption(cliOption.createCommonsCliOption());
    }

    return opts;
  }

  private FSConfigToCSConfigConverterParams validateInputFiles(
      CommandLine cliParser) {
    String yarnSiteXmlFile =
        cliParser.getOptionValue(CliOption.YARN_SITE.shortSwitch);
    String fairSchedulerXmlFile =
        cliParser.getOptionValue(CliOption.FAIR_SCHEDULER.shortSwitch);
    String conversionRulesFile =
        cliParser.getOptionValue(CliOption.CONVERSION_RULES.shortSwitch);
    String outputDir =
        cliParser.getOptionValue(CliOption.OUTPUT_DIR.shortSwitch);

    checkFile(CliOption.YARN_SITE, yarnSiteXmlFile);
    checkFile(CliOption.FAIR_SCHEDULER, fairSchedulerXmlFile);
    checkFile(CliOption.CONVERSION_RULES, conversionRulesFile);
    checkDirectory(CliOption.OUTPUT_DIR, outputDir);

    return FSConfigToCSConfigConverterParams.Builder.create()
        .withYarnSiteXmlConfig(yarnSiteXmlFile)
        .withFairSchedulerXmlConfig(fairSchedulerXmlFile)
        .withConversionRulesConfig(conversionRulesFile)
        .withClusterResource(
            cliParser.getOptionValue(CliOption.CLUSTER_RESOURCE.shortSwitch))
        .withConsole(cliParser.hasOption(CliOption.CONSOLE_MODE.shortSwitch))
        .withOutputDirectory(outputDir)
        .build();
  }

  private static void checkOptionPresent(CommandLine cliParser,
      CliOption cliOption) {
    if (!cliParser.hasOption(cliOption.shortSwitch)) {
      throw new PreconditionException(
          String.format("Missing %s parameter " + "(switch: %s|%s).",
              cliOption.name, cliOption.shortSwitch, cliOption.longSwitch));
    }
  }

  private static void checkFile(CliOption cliOption, String filePath) {
    checkFileInternal(cliOption, filePath, true);
  }

  private static void checkDirectory(CliOption cliOption, String dirPath) {
    checkFileInternal(cliOption, dirPath, false);
  }

  private static void checkFileInternal(CliOption cliOption, String filePath,
      boolean isFile) {
    //We can safely ignore null here as files / dirs were checked before
    if (filePath == null) {
      return;
    }

    File file = new File(filePath);
    if (isFile && file.isDirectory()) {
      throw new PreconditionException(
          String.format("Specified path %s is a directory but should be " +
           " a file (As value of parameter %s)", filePath, cliOption.name));
    } else if (!isFile && !file.isDirectory()) {
      throw new PreconditionException(
          String.format("Specified path %s is not a directory " +
          "(As value of parameter %s)", filePath, cliOption.name));
    } else if (!file.exists()) {
      throw new PreconditionException(
          String.format("Specified path %s does not exist " +
          "(As value of parameter %s)", filePath, cliOption.name));
    }
  }
}
