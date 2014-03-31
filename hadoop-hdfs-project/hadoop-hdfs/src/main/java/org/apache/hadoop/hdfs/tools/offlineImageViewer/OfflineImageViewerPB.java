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
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;

/**
 * OfflineImageViewerPB to dump the contents of an Hadoop image file to XML or
 * the console. Main entry point into utility, either via the command line or
 * programatically.
 */
@InterfaceAudience.Private
public class OfflineImageViewerPB {
  public static final Log LOG = LogFactory.getLog(OfflineImageViewerPB.class);

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
      + "  * FileDistribution: This processor analyzes the file size\n"
      + "    distribution in the image.\n"
      + "    -maxSize specifies the range [0, maxSize] of file sizes to be\n"
      + "     analyzed (128GB by default).\n"
      + "    -step defines the granularity of the distribution. (2MB by default)\n"
      + "  * Web: Run a viewer to expose read-only WebHDFS API.\n"
      + "    -addr specifies the address to listen. (localhost:5978 by default)\n"
      + "\n"
      + "Required command line arguments:\n"
      + "-i,--inputFile <arg>   FSImage file to process.\n"
      + "\n"
      + "Optional command line arguments:\n"
      + "-o,--outputFile <arg>  Name of output file. If the specified\n"
      + "                       file exists, it will be overwritten.\n"
      + "                       (output to stdout by default)\n"
      + "-p,--processor <arg>   Select which type of processor to apply\n"
      + "                       against image file. (XML|FileDistribution|Web)\n"
      + "                       (Web by default)\n"
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
    options.addOption("addr", true, "");

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
  public static void main(String[] args) throws IOException {
    int status = run(args);
    System.exit(status);
  }

  public static int run(String[] args) throws IOException {
    Options options = buildOptions();
    if (args.length == 0) {
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

    if (cmd.hasOption("h")) { // print help and exit
      printUsage();
      return 0;
    }

    String inputFile = cmd.getOptionValue("i");
    String processor = cmd.getOptionValue("p", "Web");
    String outputFile = cmd.getOptionValue("o", "-");

    PrintWriter out = outputFile.equals("-") ?
        new PrintWriter(System.out) : new PrintWriter(new File(outputFile));

    Configuration conf = new Configuration();
    try {
      if (processor.equals("FileDistribution")) {
        long maxSize = Long.parseLong(cmd.getOptionValue("maxSize", "0"));
        int step = Integer.parseInt(cmd.getOptionValue("step", "0"));
        new FileDistributionCalculator(conf, maxSize, step, out)
            .visit(new RandomAccessFile(inputFile, "r"));
      } else if (processor.equals("XML")) {
        new PBImageXmlWriter(conf, out).visit(new RandomAccessFile(inputFile,
            "r"));
      } else if (processor.equals("Web")) {
        String addr = cmd.getOptionValue("addr", "localhost:5978");
        new WebImageViewer(NetUtils.createSocketAddr(addr))
            .initServerAndWait(inputFile);
      }
      return 0;
    } catch (EOFException e) {
      System.err.println("Input file ended unexpectedly. Exiting");
    } catch (IOException e) {
      System.err.println("Encountered exception.  Exiting: " + e.getMessage());
    } finally {
      IOUtils.cleanup(null, out);
    }
    return -1;
  }

  /**
   * Print application usage instructions.
   */
  private static void printUsage() {
    System.out.println(usage);
  }
}
