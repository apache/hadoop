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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsLoader.OfflineEditsLoaderFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * This class implements an offline edits viewer, tool that
 * can be used to view edit logs.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OfflineEditsViewer extends Configured implements Tool {
  private static final String HELP_OPT = "-h";
  private static final String HELP_LONGOPT = "--help";
  private final static String defaultProcessor = "xml";

  /**
   * Print help.
   */  
  private void printHelp() {
    String summary =
      "Usage: bin/hdfs oev [OPTIONS] -i INPUT_FILE -o OUTPUT_FILE\n" +
      "Offline edits viewer\n" +
      "Parse a Hadoop edits log file INPUT_FILE and save results\n" +
      "in OUTPUT_FILE.\n" +
      "Required command line arguments:\n" +
      "-i,--inputFile <arg>   edits file to process, xml (case\n" +
      "                       insensitive) extension means XML format,\n" +
      "                       any other filename means binary format.\n" +
      "                       XML/Binary format input file is not allowed\n" +
      "                       to be processed by the same type processor.\n" +
      "-o,--outputFile <arg>  Name of output file. If the specified\n" +
      "                       file exists, it will be overwritten,\n" +
      "                       format of the file is determined\n" +
      "                       by -p option\n" +
      "\n" + 
      "Optional command line arguments:\n" +
      "-p,--processor <arg>   Select which type of processor to apply\n" +
      "                       against image file, currently supported\n" +
      "                       processors are: binary (native binary format\n" +
      "                       that Hadoop uses), xml (default, XML\n" +
      "                       format), stats (prints statistics about\n" +
      "                       edits file)\n" +
      "-h,--help              Display usage information and exit\n" +
      "-f,--fix-txids         Renumber the transaction IDs in the input,\n" +
      "                       so that there are no gaps or invalid\n" +
      "                       transaction IDs.\n" +
      "-r,--recover           When reading binary edit logs, use recovery \n" +
      "                       mode.  This will give you the chance to skip \n" +
      "                       corrupt parts of the edit log.\n" +
      "-v,--verbose           More verbose output, prints the input and\n" +
      "                       output filenames, for processors that write\n" +
      "                       to a file, also output to screen. On large\n" +
      "                       image files this will dramatically increase\n" +
      "                       processing time (default is false).\n";


    System.out.println(summary);
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
  }

  /**
   * Build command-line options and descriptions
   *
   * @return command line options
   */
  public static Options buildOptions() {
    Options options = new Options();

    // Build in/output file arguments, which are required, but there is no 
    // addOption method that can specify this
    OptionBuilder.isRequired();
    OptionBuilder.hasArgs();
    OptionBuilder.withLongOpt("outputFilename");
    options.addOption(OptionBuilder.create("o"));
    
    OptionBuilder.isRequired();
    OptionBuilder.hasArgs();
    OptionBuilder.withLongOpt("inputFilename");
    options.addOption(OptionBuilder.create("i"));
    
    options.addOption("p", "processor", true, "");
    options.addOption("v", "verbose", false, "");
    options.addOption("f", "fix-txids", false, "");
    options.addOption("r", "recover", false, "");
    options.addOption("h", "help", false, "");

    return options;
  }

  /** Process an edit log using the chosen processor or visitor.
   * 
   * @param inputFilename   The file to process
   * @param outputFilename  The output file name
   * @param processor       If visitor is null, the processor to use
   * @param visitor         If non-null, the visitor to use.
   * 
   * @return                0 on success; error code otherwise
   */
  public int go(String inputFileName, String outputFileName, String processor,
      Flags flags, OfflineEditsVisitor visitor)
  {
    if (flags.getPrintToScreen()) {
      System.out.println("input  [" + inputFileName  + "]");
      System.out.println("output [" + outputFileName + "]");
    }

    boolean xmlInput = StringUtils.toLowerCase(inputFileName).endsWith(".xml");
    if (xmlInput && StringUtils.equalsIgnoreCase("xml", processor)) {
      System.err.println("XML format input file is not allowed"
          + " to be processed by XML processor.");
      return -1;
    } else if(!xmlInput && StringUtils.equalsIgnoreCase("binary", processor)) {
      System.err.println("Binary format input file is not allowed"
          + " to be processed by Binary processor.");
      return -1;
    }

    try {
      if (visitor == null) {
        visitor = OfflineEditsVisitorFactory.getEditsVisitor(
            outputFileName, processor, flags.getPrintToScreen());
      }

      OfflineEditsLoader loader = OfflineEditsLoaderFactory.
          createLoader(visitor, inputFileName, xmlInput, flags);
      loader.loadEdits();
    } catch(Exception e) {
      System.err.println("Encountered exception. Exiting: " + e.getMessage());
      e.printStackTrace(System.err);
      return -1;
    }
    return 0;
  }

  public static class Flags {
    private boolean printToScreen = false;
    private boolean fixTxIds = false;
    private boolean recoveryMode = false;
    
    public Flags() {
    }
    
    public boolean getPrintToScreen() {
      return printToScreen;
    }
    
    public void setPrintToScreen() {
      printToScreen = true;
    }
    
    public boolean getFixTxIds() {
      return fixTxIds;
    }
    
    public void setFixTxIds() {
      fixTxIds = true;
    }
    
    public boolean getRecoveryMode() {
      return recoveryMode;
    }
    
    public void setRecoveryMode() {
      recoveryMode = true;
    }
  }
  
  /**
   * Main entry point for ToolRunner (see ToolRunner docs)
   *
   * @param argv The parameters passed to this program.
   * @return 0 on success, non zero on error.
   */
  @Override
  public int run(String[] argv) throws Exception {
    Options options = buildOptions();
    if(argv.length == 0) {
      printHelp();
      return 0;
    }
    // print help and exit with zero exit code
    if (argv.length == 1 && isHelpOption(argv[0])) {
      printHelp();
      return 0;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      System.out.println(
        "Error parsing command-line options: " + e.getMessage());
      printHelp();
      return -1;
    }
    
    if (cmd.hasOption("h")) {
      // print help and exit with non zero exit code since
      // it is not expected to give help and other options together.
      printHelp();
      return -1;
    }
    String inputFileName = cmd.getOptionValue("i");
    String outputFileName = cmd.getOptionValue("o");
    String processor = cmd.getOptionValue("p");
    if(processor == null) {
      processor = defaultProcessor;
    }
    Flags flags = new Flags();
    if (cmd.hasOption("r")) {
      flags.setRecoveryMode();
    }
    if (cmd.hasOption("f")) {
      flags.setFixTxIds();
    }
    if (cmd.hasOption("v")) {
      flags.setPrintToScreen();
    }
    return go(inputFileName, outputFileName, processor, flags, null);
  }

  /**
   * main() runs the offline edits viewer using ToolRunner
   *
   * @param argv Command line parameters.
   */
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new OfflineEditsViewer(), argv);
    System.exit(res);
  }

  private static boolean isHelpOption(String arg) {
    return arg.equalsIgnoreCase(HELP_OPT) ||
        arg.equalsIgnoreCase(HELP_LONGOPT);
  }
}
