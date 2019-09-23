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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;

/**
 * OfflineImageViewerPB to dump the contents of an Hadoop image file to XML or
 * the console. Main entry point into utility, either via the command line or
 * programmatically.
 */
@InterfaceAudience.Private
public class OfflineImageViewerPB {
  private static final String HELP_OPT = "-h";
  private static final String HELP_LONGOPT = "--help";
  public static final Logger LOG =
      LoggerFactory.getLogger(OfflineImageViewerPB.class);

  private final static String usage = "Usage: bin/hdfs oiv [OPTIONS] -i INPUTFILE -o OUTPUTFILE\n"
      + "Offline Image Viewer\n"
      + "View a Hadoop fsimage INPUTFILE using the specified PROCESSOR,\n"
      + "saving the results in OUTPUTFILE.\n"
      + "\n"
      + "The oiv utility will attempt to parse correctly formed image files\n"
      + "and will abort fail with mal-formed image files.\n"
      + "\n"
      + "The tool works offline and does not require a running cluster in\n"
      + "order to process an image file.\n"
      + "\n"
      + "The following image processors are available:\n"
      + "  * XML: This processor creates an XML document with all elements of\n"
      + "    the fsimage enumerated, suitable for further analysis by XML\n"
      + "    tools.\n"
      + "  * ReverseXML: This processor takes an XML file and creates a\n"
      + "    binary fsimage containing the same elements.\n"
      + "  * FileDistribution: This processor analyzes the file size\n"
      + "    distribution in the image.\n"
      + "    -maxSize specifies the range [0, maxSize] of file sizes to be\n"
      + "     analyzed (128GB by default).\n"
      + "    -step defines the granularity of the distribution. (2MB by default)\n"
      + "    -format formats the output result in a human-readable fashion\n"
      + "     rather than a number of bytes. (false by default)\n"
      + "  * Web: Run a viewer to expose read-only WebHDFS API.\n"
      + "    -addr specifies the address to listen. (localhost:5978 by default)\n"
      + "    It does not support secure mode nor HTTPS.\n"
      + "  * Delimited (experimental): Generate a text file with all of the elements common\n"
      + "    to both inodes and inodes-under-construction, separated by a\n"
      + "    delimiter. The default delimiter is \\t, though this may be\n"
      + "    changed via the -delimiter argument.\n"
      + "    -sp print storage policy, used by delimiter only.\n"
      + "  * DetectCorruption: Detect potential corruption of the image by\n"
      + "    selectively loading parts of it and actively searching for\n"
      + "    inconsistencies. Outputs a summary of the found corruptions\n"
      + "    in a delimited format.\n"
      + "    Note that the check is not exhaustive, and only catches\n"
      + "    missing nodes during the namespace reconstruction.\n"
      + "\n"
      + "Required command line arguments:\n"
      + "-i,--inputFile <arg>   FSImage or XML file to process.\n"
      + "\n"
      + "Optional command line arguments:\n"
      + "-o,--outputFile <arg>  Name of output file. If the specified\n"
      + "                       file exists, it will be overwritten.\n"
      + "                       (output to stdout by default)\n"
      + "                       If the input file was an XML file, we\n"
      + "                       will also create an <outputFile>.md5 file.\n"
      + "-p,--processor <arg>   Select which type of processor to apply\n"
      + "                       against image file. (XML|FileDistribution|\n"
      + "                       ReverseXML|Web|Delimited|DetectCorruption)\n"
      + "                       The default is Web.\n"
      + "-delimiter <arg>       Delimiting string to use with Delimited or \n"
      + "                       DetectCorruption processor. \n"
      + "-t,--temp <arg>        Use temporary dir to cache intermediate\n"
      + "                       result to generate DetectCorruption or\n"
      + "                       Delimited outputs. If not set, the processor\n"
      + "                       constructs the namespace in memory \n"
      + "                       before outputting text.\n"
      + "-h,--help              Display usage information and exit\n";

  /**
   * Build command-line options and descriptions
   */
  private static Options buildOptions() {
    Options options = new Options();

    // Build in/output file arguments, which are required, but there is no
    // addOption method that can specify this
    OptionBuilder.isRequired();
    OptionBuilder.hasArgs();
    OptionBuilder.withLongOpt("inputFile");
    options.addOption(OptionBuilder.create("i"));

    options.addOption("o", "outputFile", true, "");
    options.addOption("p", "processor", true, "");
    options.addOption("h", "help", false, "");
    options.addOption("maxSize", true, "");
    options.addOption("step", true, "");
    options.addOption("format", false, "");
    options.addOption("addr", true, "");
    options.addOption("delimiter", true, "");
    options.addOption("sp", false, "");
    options.addOption("t", "temp", true, "");

    return options;
  }

  /**
   * Entry point to command-line-driven operation. User may specify options and
   * start fsimage viewer from the command line. Program will process image file
   * and exit cleanly or, if an error is encountered, inform user and exit.
   * 
   * @param args
   *          Command line options
   * @throws IOException
   */
  public static void main(String[] args) throws Exception {
    int status = run(args);
    ExitUtil.terminate(status);
  }

  public static int run(String[] args) throws Exception {
    Options options = buildOptions();
    if (args.length == 0) {
      printUsage();
      return 0;
    }
    // print help and exit with zero exit code
    if (args.length == 1 && isHelpOption(args[0])) {
      printUsage();
      return 0;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println("Error parsing command-line options: ");
      printUsage();
      return -1;
    }

    if (cmd.hasOption("h")) {
      // print help and exit with non zero exit code since
      // it is not expected to give help and other options together.
      printUsage();
      return -1;
    }

    String inputFile = cmd.getOptionValue("i");
    String processor = cmd.getOptionValue("p", "Web");
    String outputFile = cmd.getOptionValue("o", "-");
    String delimiter = cmd.getOptionValue("delimiter",
        PBImageTextWriter.DEFAULT_DELIMITER);
    String tempPath = cmd.getOptionValue("t", "");

    Configuration conf = new Configuration();
    PrintStream out = null;
    try {
      out = outputFile.equals("-") || "REVERSEXML".equalsIgnoreCase(processor) ?
        System.out : new PrintStream(outputFile, "UTF-8");
      switch (StringUtils.toUpperCase(processor)) {
      case "FILEDISTRIBUTION":
        long maxSize = Long.parseLong(cmd.getOptionValue("maxSize", "0"));
        int step = Integer.parseInt(cmd.getOptionValue("step", "0"));
        boolean formatOutput = cmd.hasOption("format");
        try (RandomAccessFile r = new RandomAccessFile(inputFile, "r")) {
          new FileDistributionCalculator(conf, maxSize, step, formatOutput, out)
            .visit(r);
        }
        break;
      case "XML":
        try (RandomAccessFile r = new RandomAccessFile(inputFile, "r")) {
          new PBImageXmlWriter(conf, out).visit(r);
        }
        break;
      case "REVERSEXML":
        try {
          OfflineImageReconstructor.run(inputFile, outputFile);
        } catch (Exception e) {
          System.err.println("OfflineImageReconstructor failed: "
              + e.getMessage());
          e.printStackTrace(System.err);
          ExitUtil.terminate(1);
        }
        break;
      case "WEB":
        String addr = cmd.getOptionValue("addr", "localhost:5978");
        try (WebImageViewer viewer =
            new WebImageViewer(NetUtils.createSocketAddr(addr), conf)) {
          viewer.start(inputFile);
        }
        break;
      case "DELIMITED":
        boolean printStoragePolicy = cmd.hasOption("sp");
        try (PBImageDelimitedTextWriter writer =
            new PBImageDelimitedTextWriter(out, delimiter,
                tempPath, printStoragePolicy);
            RandomAccessFile r = new RandomAccessFile(inputFile, "r")) {
          writer.visit(r);
        }
        break;
      case "DETECTCORRUPTION":
        try (PBImageCorruptionDetector detector =
            new PBImageCorruptionDetector(out, delimiter, tempPath)) {
          detector.visit(new RandomAccessFile(inputFile, "r"));
        }
        break;
      default:
        System.err.println("Invalid processor specified : " + processor);
        printUsage();
        return -1;
      }
      return 0;
    } catch (EOFException e) {
      System.err.println("Input file ended unexpectedly. Exiting");
    } catch (IOException e) {
      System.err.println("Encountered exception.  Exiting: " + e.getMessage());
      e.printStackTrace(System.err);
    } finally {
      if (out != null && out != System.out) {
        out.close();
      }
    }
    return -1;
  }

  /**
   * Print application usage instructions.
   */
  private static void printUsage() {
    System.out.println(usage);
  }

  private static boolean isHelpOption(String arg) {
    return arg.equalsIgnoreCase(HELP_OPT) ||
        arg.equalsIgnoreCase(HELP_LONGOPT);
  }
}
