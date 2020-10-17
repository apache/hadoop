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

import java.io.File;
import java.util.function.Supplier;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Parses arguments passed to the FS-&gt;CS converter.
 * If the arguments are valid, it calls the converter itself.
 *
 */
public class FSConfigToCSConfigArgumentHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSConfigToCSConfigArgumentHandler.class);

  private static final String ALREADY_CONTAINS_EXCEPTION_MSG =
      "The %s (provided with %s|%s arguments) contains " +
          "the %s provided with the %s|%s options.";
  private static final String ALREADY_CONTAINS_FILE_EXCEPTION_MSG =
      "The %s %s (provided with %s|%s arguments) already contains a file " +
          "or directory named %s which will be the output of the conversion!";

  private FSConfigToCSConfigRuleHandler ruleHandler;
  private FSConfigToCSConfigConverterParams converterParams;
  private ConversionOptions conversionOptions;
  private ConvertedConfigValidator validator;

  private Supplier<FSConfigToCSConfigConverter>
      converterFunc = this::getConverter;

  public FSConfigToCSConfigArgumentHandler() {
    this.conversionOptions = new ConversionOptions(new DryRunResultHolder(),
        false);
    this.validator = new ConvertedConfigValidator();
  }

  @VisibleForTesting
  FSConfigToCSConfigArgumentHandler(ConversionOptions conversionOptions,
      ConvertedConfigValidator validator) {
    this.conversionOptions = conversionOptions;
    this.validator = validator;
  }

  /**
   * Represents options for the converter CLI.
   *
   */
  public enum CliOption {
    YARN_SITE("yarn-site.xml", "y", "yarnsiteconfig",
        "Path to a valid yarn-site.xml config file", true),

    // fair-scheduler.xml is not mandatory
    // if FairSchedulerConfiguration.ALLOCATION_FILE is defined in yarn-site.xml
    FAIR_SCHEDULER("fair-scheduler.xml", "f", "fsconfig",
        "Path to a valid fair-scheduler.xml config file", true),
    CONVERSION_RULES("conversion rules config file", "r", "rulesconfig",
        "Optional parameter. If given, should specify a valid path to the " +
            "conversion rules file (property format).", true),
    CONSOLE_MODE("console mode", "p", "print",
        "If defined, the converted configuration will " +
            "only be emitted to the console.", false),
    CLUSTER_RESOURCE("cluster resource", "c", "cluster-resource",
        "Needs to be given if maxResources is defined as percentages " +
            "for any queue, otherwise this parameter can be omitted.",
              true),
    OUTPUT_DIR("output directory", "o", "output-directory",
        "Output directory for yarn-site.xml and" +
            " capacity-scheduler.xml files." +
            "Must have write permission for user who is running this script.",
            true),
    DRY_RUN("dry run", "d", "dry-run", "Performs a dry-run of the conversion." +
            "Outputs whether the conversion is possible or not.", false),
    NO_TERMINAL_RULE_CHECK("no terminal rule check", "t",
        "no-terminal-rule-check",
        "Disables checking whether a placement rule is terminal to maintain" +
        " backward compatibility with configs that were made before YARN-8967.",
        false),
    CONVERT_PLACEMENT_RULES("convert placement rules",
        "m", "convert-placement-rules",
        "Convert Fair Scheduler placement rules to Capacity" +
        " Scheduler mapping rules", false),
    SKIP_VERIFICATION("skip verification", "s",
        "skip-verification",
        "Skips the verification of the converted configuration", false),
    ENABLE_ASYNC_SCHEDULER("enable asynchronous scheduler", "a", "enable-async-scheduler",
      "Enables the Asynchronous scheduler which decouples the CapacityScheduler" +
        " scheduling from Node Heartbeats.", false),
    HELP("help", "h", "help", "Displays the list of options", false);

    private final String name;
    private final String shortSwitch;
    private final String longSwitch;
    private final String description;
    private final boolean hasArg;

    CliOption(String name, String shortSwitch, String longSwitch,
        String description, boolean hasArg) {
      this.name = name;
      this.shortSwitch = shortSwitch;
      this.longSwitch = longSwitch;
      this.description = description;
      this.hasArg = hasArg;
    }

    public Option createCommonsCliOption() {
      Option option = new Option(shortSwitch, longSwitch, hasArg, description);
      return option;
    }
  }

  int parseAndConvert(String[] args) throws Exception {
    Options opts = createOptions();
    int retVal = 0;

    try {
      if (args.length == 0) {
        LOG.info("Missing command line arguments");
        printHelp(opts);
        return 0;
      }

      CommandLine cliParser = new GnuParser().parse(opts, args);

      if (cliParser.hasOption(CliOption.HELP.shortSwitch)) {
        printHelp(opts);
        return 0;
      }

      FSConfigToCSConfigConverter converter =
          prepareAndGetConverter(cliParser);

      converter.convert(converterParams);

      String outputDir = converterParams.getOutputDirectory();
      boolean skipVerification =
          cliParser.hasOption(CliOption.SKIP_VERIFICATION.shortSwitch);
      if (outputDir != null && !skipVerification) {
        validator.validateConvertedConfig(
            converterParams.getOutputDirectory());
      }
    } catch (ParseException e) {
      String msg = "Options parsing failed: " + e.getMessage();
      logAndStdErr(e, msg);
      printHelp(opts);
      retVal = -1;
    } catch (PreconditionException e) {
      String msg = "Cannot start FS config conversion due to the following"
          + " precondition error: " + e.getMessage();
      handleException(e, msg);
      retVal = -1;
    } catch (UnsupportedPropertyException e) {
      String msg = "Unsupported property/setting encountered during FS config "
          + "conversion: " + e.getMessage();
      handleException(e, msg);
      retVal = -1;
    } catch (ConversionException | IllegalArgumentException e) {
      String msg = "Fatal error during FS config conversion: " + e.getMessage();
      handleException(e, msg);
      retVal = -1;
    } catch (VerificationException e) {
      Throwable cause = e.getCause();
      String msg = "Verification failed: " + e.getCause().getMessage();
      conversionOptions.handleVerificationFailure(cause, msg);
      retVal = -1;
    }

    conversionOptions.handleParsingFinished();

    return retVal;
  }

  private void handleException(Exception e, String msg) {
    conversionOptions.handleGenericException(e, msg);
  }

  static void logAndStdErr(Throwable t, String msg) {
    LOG.debug("Stack trace", t);
    LOG.error(msg);
    System.err.println(msg);
  }

  private Options createOptions() {
    Options opts = new Options();

    for (CliOption cliOption : CliOption.values()) {
      opts.addOption(cliOption.createCommonsCliOption());
    }

    return opts;
  }

  private FSConfigToCSConfigConverter prepareAndGetConverter(
      CommandLine cliParser) {
    boolean dryRun =
        cliParser.hasOption(CliOption.DRY_RUN.shortSwitch);
    conversionOptions.setDryRun(dryRun);
    conversionOptions.setNoTerminalRuleCheck(
        cliParser.hasOption(CliOption.NO_TERMINAL_RULE_CHECK.shortSwitch));
    conversionOptions.setEnableAsyncScheduler(
      cliParser.hasOption(CliOption.ENABLE_ASYNC_SCHEDULER.shortSwitch));

    checkOptionPresent(cliParser, CliOption.YARN_SITE);
    checkOutputDefined(cliParser, dryRun);

    converterParams = validateInputFiles(cliParser);
    ruleHandler = new FSConfigToCSConfigRuleHandler(conversionOptions);

    return converterFunc.get();
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
    boolean convertPlacementRules =
        cliParser.hasOption(CliOption.CONVERT_PLACEMENT_RULES.shortSwitch);

    checkFile(CliOption.YARN_SITE, yarnSiteXmlFile);
    checkFile(CliOption.FAIR_SCHEDULER, fairSchedulerXmlFile);
    checkFile(CliOption.CONVERSION_RULES, conversionRulesFile);
    checkDirectory(CliOption.OUTPUT_DIR, outputDir);
    checkOutputDirDoesNotContainXmls(yarnSiteXmlFile, outputDir);

    return FSConfigToCSConfigConverterParams.Builder.create()
        .withYarnSiteXmlConfig(yarnSiteXmlFile)
        .withFairSchedulerXmlConfig(fairSchedulerXmlFile)
        .withConversionRulesConfig(conversionRulesFile)
        .withClusterResource(
            cliParser.getOptionValue(CliOption.CLUSTER_RESOURCE.shortSwitch))
        .withConsole(cliParser.hasOption(CliOption.CONSOLE_MODE.shortSwitch))
        .withOutputDirectory(outputDir)
        .withConvertPlacementRules(convertPlacementRules)
        .build();
  }

  private static void checkOutputDirDoesNotContainXmls(String yarnSiteXmlFile,
      String outputDir) {
    if (yarnSiteXmlFile == null || outputDir == null) {
      return;
    }

    // check whether yarn-site.xml is not in the output folder
    File xmlFile = new File(yarnSiteXmlFile);
    File xmlParentFolder = xmlFile.getParentFile();
    File output = new File(outputDir);
    if (output.equals(xmlParentFolder)) {
      throw new IllegalArgumentException(
          String.format(ALREADY_CONTAINS_EXCEPTION_MSG,
              CliOption.OUTPUT_DIR.name, CliOption.OUTPUT_DIR.shortSwitch,
              CliOption.OUTPUT_DIR.longSwitch, CliOption.YARN_SITE.name,
              CliOption.YARN_SITE.shortSwitch,
              CliOption.YARN_SITE.longSwitch));
    }

    // check whether the output folder does not contain nor yarn-site.xml
    // neither capacity-scheduler.xml
    checkFileNotInOutputDir(output,
        YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    checkFileNotInOutputDir(output,
        YarnConfiguration.CS_CONFIGURATION_FILE);
  }

  private static void checkFileNotInOutputDir(File output, String fileName) {
    File file = new File(output, fileName);
    if (file.exists()) {
      throw new IllegalArgumentException(
          String.format(ALREADY_CONTAINS_FILE_EXCEPTION_MSG,
              CliOption.OUTPUT_DIR.name, output,
              CliOption.OUTPUT_DIR.shortSwitch,
              CliOption.OUTPUT_DIR.longSwitch,
              fileName));
    }
  }

  private void printHelp(Options opts) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("General options are: ", opts);
  }

  private static void checkOptionPresent(CommandLine cliParser,
      CliOption cliOption) {
    if (!cliParser.hasOption(cliOption.shortSwitch)) {
      throw new PreconditionException(
          String.format("Missing %s parameter " + "(switch: %s|%s).",
              cliOption.name, cliOption.shortSwitch, cliOption.longSwitch));
    }
  }

  private static void checkOutputDefined(CommandLine cliParser,
      boolean dryRun) {
    boolean hasOutputDir =
        cliParser.hasOption(CliOption.OUTPUT_DIR.shortSwitch);

    boolean console =
        cliParser.hasOption(CliOption.CONSOLE_MODE.shortSwitch);

    if (!console && !hasOutputDir && !dryRun) {
      throw new PreconditionException(
         "Output directory or console mode was not defined. Please" +
          " use -h or --help to see command line switches");
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

  private FSConfigToCSConfigConverter getConverter() {
    return new FSConfigToCSConfigConverter(ruleHandler, conversionOptions);
  }

  @VisibleForTesting
  void setConverterSupplier(Supplier<FSConfigToCSConfigConverter>
      supplier) {
    this.converterFunc = supplier;
  }
}
