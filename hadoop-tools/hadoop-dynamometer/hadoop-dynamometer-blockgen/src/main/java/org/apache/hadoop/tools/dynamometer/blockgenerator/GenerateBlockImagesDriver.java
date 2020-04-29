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
package org.apache.hadoop.tools.dynamometer.blockgenerator;

import java.net.URI;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is the main driver class. It takes in the following arguments: -
 * Required: input path of the fsImage from the HDFS cluster to be simulated -
 * Required: output path for generated block image files for each Dynamometer
 * DataNode - Required: Number of DataNodes to generate blocks for - Optional:
 * Number of reducers to use for the job (defaults to number of DataNodes)
 */

public class GenerateBlockImagesDriver extends Configured implements Tool {

  public static final String FSIMAGE_INPUT_PATH_ARG = "fsimage_input_path";
  public static final String BLOCK_IMAGE_OUTPUT_ARG = "block_image_output_dir";
  public static final String NUM_REDUCERS_ARG = "num_reducers";
  public static final String NUM_DATANODES_ARG = "num_datanodes";

  public static final String NUM_DATANODES_KEY = "dyno.blockgen.num.datanodes";

  public GenerateBlockImagesDriver(Configuration conf) {
    setConf(conf);
  }

  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("h", "help", false, "Shows this message");
    options.addOption(OptionBuilder.withArgName("Input path of the XML fsImage")
        .hasArg().isRequired(true)
        .withDescription("Input path to the Hadoop fsImage XML file (required)")
        .create(FSIMAGE_INPUT_PATH_ARG));
    options.addOption(OptionBuilder.withArgName("BlockImage output directory")
        .hasArg().isRequired(true)
        .withDescription("Directory where the generated files containing the "
            + "block listing for each DataNode should be stored (required)")
        .create(BLOCK_IMAGE_OUTPUT_ARG));
    options.addOption(OptionBuilder.withArgName("Number of reducers").hasArg()
        .isRequired(false)
        .withDescription(
            "Number of reducers for this job (defaults to number of datanodes)")
        .create(NUM_REDUCERS_ARG));
    options.addOption(OptionBuilder.withArgName("Number of datanodes").hasArg()
        .isRequired(true)
        .withDescription("Number of DataNodes to create blocks for (required)")
        .create(NUM_DATANODES_ARG));

    CommandLineParser parser = new PosixParser();
    CommandLine cli = parser.parse(options, args);
    if (cli.hasOption("h")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(200, "hadoop jar dynamometer-*.jar "
              + "org.apache.hadoop.tools.dynamometer.blockgenerator."
              + "GenerateBlockImagesDriver [options]",
          null, options, null);
      return 0;
    }

    String fsImageInputPath = cli.getOptionValue(FSIMAGE_INPUT_PATH_ARG);
    String blockImageOutputDir = cli.getOptionValue(BLOCK_IMAGE_OUTPUT_ARG);
    int numDataNodes = Integer.parseInt(cli.getOptionValue(NUM_DATANODES_ARG));
    int numReducers = Integer.parseInt(
        cli.getOptionValue(NUM_REDUCERS_ARG, String.valueOf(numDataNodes)));

    FileSystem fs = FileSystem.get(new URI(blockImageOutputDir), getConf());
    Job job = Job.getInstance(getConf(), "Create blocksImages for Dynamometer");
    FileInputFormat.setInputPaths(job, new Path(fsImageInputPath));
    Path blockImagesDir = new Path(blockImageOutputDir);
    fs.delete(blockImagesDir, true);
    FileOutputFormat.setOutputPath(job, blockImagesDir);
    job.getConfiguration().setInt(NUM_DATANODES_KEY, numDataNodes);

    job.setJarByClass(GenerateBlockImagesDriver.class);
    job.setInputFormatClass(NoSplitTextInputFormat.class);
    job.setNumReduceTasks(numReducers);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    job.setMapperClass(XMLParserMapper.class);
    job.setReducerClass(GenerateDNBlockInfosReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(BlockInfo.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    GenerateBlockImagesDriver driver = new GenerateBlockImagesDriver(
        new Configuration());
    System.exit(ToolRunner.run(driver, args));
  }

  /** A simple text input format that doesn't allow splitting of files. */
  public static class NoSplitTextInputFormat extends TextInputFormat {
    @Override
    public boolean isSplitable(JobContext context, Path file) {
      return false;
    }
  }
}
