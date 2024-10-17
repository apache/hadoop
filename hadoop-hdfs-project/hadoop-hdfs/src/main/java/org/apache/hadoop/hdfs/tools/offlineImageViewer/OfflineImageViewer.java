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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.PositionTrackingInputStream;

/**
 * OfflineImageViewer to dump the contents of an Hadoop image file to XML
 * or the console.  Main entry point into utility, either via the
 * command line or programmatically.
 */
@InterfaceAudience.Private
public class OfflineImageViewer {
  public static final Logger LOG =
      LoggerFactory.getLogger(OfflineImageViewer.class);
  
  private final static String usage =
      "Usage: bin/hdfs oiv_legacy [OPTIONS] -i INPUTFILE -o OUTPUTFILE\n"
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
          + "  * Ls: The default image processor generates an lsr-style listing\n"
          + "    of the files in the namespace, with the same fields in the same\n"
          + "    order.  Note that in order to correctly determine file sizes,\n"
          + "    this formatter cannot skip blocks and will override the\n"
          + "    -skipBlocks option.\n"
          + "  * Indented: This processor enumerates over all of the elements in\n"
          + "    the fsimage file, using levels of indentation to delineate\n"
          + "    sections within the file.\n"
          + "  * Delimited: Generate a text file with all of the elements common\n"
          + "    to both inodes and inodes-under-construction, separated by a\n"
          + "    delimiter. The default delimiter is \u0001, though this may be\n"
          + "    changed via the -delimiter argument. This processor also overrides\n"
          + "    the -skipBlocks option for the same reason as the Ls processor\n"
          + "  * XML: This processor creates an XML document with all elements of\n"
          + "    the fsimage enumerated, suitable for further analysis by XML\n"
          + "    tools.\n"
          + "  * FileDistribution: This processor analyzes the file size\n"
          + "    distribution in the image.\n"
          + "    -maxSize specifies the range [0, maxSize] of file sizes to be\n"
          + "     analyzed (128GB by default).\n"
          + "    -step defines the granularity of the distribution. (2MB by default)\n"
          + "    -format formats the output result in a human-readable fashion\n"
          + "     rather than a number of bytes. (false by default)\n"
          + "  * NameDistribution: This processor analyzes the file names\n"
          + "    in the image and prints total number of file names and how frequently\n"
          + "    file names are reused.\n"
          + "\n"
          + "Required command line arguments:\n"
          + "-i,--inputFile <arg>   FSImage file to process.\n"
          + "-o,--outputFile <arg>  Name of output file. If the specified\n"
          + "                       file exists, it will be overwritten.\n"
          + "\n"
          + "Optional command line arguments:\n"
          + "-p,--processor <arg>   Select which type of processor to apply\n"
          + "                       against image file."
          + " (Ls|XML|Delimited|Indented|FileDistribution|NameDistribution).\n"
          + "-h,--help              Display usage information and exit\n"
          + "-printToScreen         For processors that write to a file, also\n"
          + "                       output to screen. On large image files this\n"
          + "                       will dramatically increase processing time.\n"
          + "-skipBlocks            Skip inodes' blocks information. May\n"
          + "                       significantly decrease output.\n"
          + "                       (default = false).\n"
          + "-delimiter <arg>       Delimiting string to use with Delimited processor\n";

  private final boolean skipBlocks;
  private final String inputFile;
  private final ImageVisitor processor;
  
  public OfflineImageViewer(String inputFile, ImageVisitor processor, 
             boolean skipBlocks) {
    this.inputFile = inputFile;
    this.processor = processor;
    this.skipBlocks = skipBlocks;
  }

  /**
   * Process image file.
   */
  public void go() throws IOException  {
    DataInputStream in = null;
    PositionTrackingInputStream tracker = null;
    ImageLoader fsip = null;
    boolean done = false;
    try {
      tracker = new PositionTrackingInputStream(new BufferedInputStream(
          Files.newInputStream(Paths.get(inputFile))));
      in = new DataInputStream(tracker);

      int imageVersionFile = findImageVersion(in);

      fsip = ImageLoader.LoaderFactory.getLoader(imageVersionFile);

      if(fsip == null) 
        throw new IOException("No image processor to read version " +
            imageVersionFile + " is available.");
      fsip.loadImage(in, processor, skipBlocks);
      done = true;
    } finally {
      if (!done) {
        if (tracker != null) {
          LOG.error("image loading failed at offset " + tracker.getPos());
        } else {
          LOG.error("Failed to load image file.");
        }
      }
      IOUtils.cleanupWithLogger(LOG, in, tracker);
    }
  }

  /**
   * Check an fsimage datainputstream's version number.
   *
   * The datainput stream is returned at the same point as it was passed in;
   * this method has no effect on the datainputstream's read pointer.
   *
   * @param in Datainputstream of fsimage
   * @return Filesystem layout version of fsimage represented by stream
   * @throws IOException If problem reading from in
   */
  private int findImageVersion(DataInputStream in) throws IOException {
    in.mark(42); // arbitrary amount, resetting immediately

    int version = in.readInt();
    in.reset();

    return version;
  }
  
  /**
   * Build command-line options and descriptions
   */
  public static Options buildOptions() {
    Options options = new Options();

    // Build in/output file arguments, which are required, but there is no 
    // addOption method that can specify this
    options.addOption(Option.builder("o").required().hasArgs().longOpt("outputFile").build());

    options.addOption(Option.builder("i").required().hasArgs().longOpt("inputFile").build());

    options.addOption("p", "processor", true, "");
    options.addOption("h", "help", false, "");
    options.addOption("maxSize", true, "");
    options.addOption("step", true, "");
    options.addOption("format", false, "");
    options.addOption("skipBlocks", false, "");
    options.addOption("printToScreen", false, "");
    options.addOption("delimiter", true, "");

    return options;
  }
  
  /**
   * Entry point to command-line-driven operation.  User may specify
   * options and start fsimage viewer from the command line.  Program
   * will process image file and exit cleanly or, if an error is
   * encountered, inform user and exit.
   *
   * @param args Command line options
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    Options options = buildOptions();
    if(args.length == 0) {
      printUsage();
      return;
    }
    
    CommandLineParser parser = new PosixParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println("Error parsing command-line options: ");
      printUsage();
      return;
    }

    if(cmd.hasOption("h")) { // print help and exit
      printUsage();
      return;
    }

    boolean skipBlocks = cmd.hasOption("skipBlocks");
    boolean printToScreen = cmd.hasOption("printToScreen");
    String inputFile = cmd.getOptionValue("i");
    String processor = cmd.getOptionValue("p", "Ls");
    String outputFile = cmd.getOptionValue("o");
    String delimiter = cmd.getOptionValue("delimiter");
    
    if( !(delimiter == null || processor.equals("Delimited")) ) {
      System.out.println("Can only specify -delimiter with Delimited processor");
      printUsage();
      return;
    }
    
    ImageVisitor v;
    if(processor.equals("Indented")) {
      v = new IndentedImageVisitor(outputFile, printToScreen);
    } else if (processor.equals("XML")) {
      v = new XmlImageVisitor(outputFile, printToScreen);
    } else if (processor.equals("Delimited")) {
      v = delimiter == null ?  
                 new DelimitedImageVisitor(outputFile, printToScreen) :
                 new DelimitedImageVisitor(outputFile, printToScreen, delimiter);
      skipBlocks = false;
    } else if (processor.equals("FileDistribution")) {
      long maxSize = Long.parseLong(cmd.getOptionValue("maxSize", "0"));
      int step = Integer.parseInt(cmd.getOptionValue("step", "0"));
      boolean formatOutput = cmd.hasOption("format");
      v = new FileDistributionVisitor(outputFile, maxSize, step, formatOutput);
    } else if (processor.equals("NameDistribution")) {
      v = new NameDistributionVisitor(outputFile, printToScreen);
    } else {
      v = new LsImageVisitor(outputFile, printToScreen);
      skipBlocks = false;
    }
    
    try {
      OfflineImageViewer d = new OfflineImageViewer(inputFile, v, skipBlocks);
      d.go();
    } catch (EOFException e) {
      System.err.println("Input file ended unexpectedly.  Exiting");
    } catch(IOException e) {
      System.err.println("Encountered exception.  Exiting: " + e.getMessage());
    }
  }

  /**
   * Print application usage instructions.
   */
  private static void printUsage() {
    System.out.println(usage);
  }
}
