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
package org.apache.hadoop.mapred.gridmix;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapred.gridmix.GenerateData.GenDataFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * This is a utility class for all the compression related modules.
 */
class CompressionEmulationUtil {
  static final Log LOG = LogFactory.getLog(CompressionEmulationUtil.class);
  
  /**
   * Enable compression usage in GridMix runs.
   */
  private static final String COMPRESSION_EMULATION_ENABLE = 
    "gridmix.compression-emulation.enable";
  
  /**
   * Enable input data decompression.
   */
  private static final String INPUT_DECOMPRESSION_EMULATION_ENABLE = 
    "gridmix.compression-emulation.input-decompression.enable";
  
  /**
   * This is a {@link Mapper} implementation for generating random text data.
   * It uses {@link RandomTextDataGenerator} for generating text data and the
   * output files are compressed.
   */
  public static class RandomTextDataMapper
  extends Mapper<NullWritable, LongWritable, Text, Text> {
    private RandomTextDataGenerator rtg;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int listSize = 
        RandomTextDataGenerator.getRandomTextDataGeneratorListSize(conf);
      int wordSize = 
        RandomTextDataGenerator.getRandomTextDataGeneratorWordSize(conf);
      rtg = new RandomTextDataGenerator(listSize, wordSize);
    }
    
    /**
     * Emits random words sequence of desired size. Note that the desired output
     * size is passed as the value parameter to this map.
     */
    @Override
    public void map(NullWritable key, LongWritable value, Context context)
    throws IOException, InterruptedException {
      //TODO Control the extra data written ..
      //TODO Should the key\tvalue\n be considered for measuring size?
      //     Can counters like BYTES_WRITTEN be used? What will be the value of
      //     such counters in LocalJobRunner?
      for (long bytes = value.get(); bytes > 0;) {
        String randomKey = rtg.getRandomWord();
        String randomValue = rtg.getRandomWord();
        context.write(new Text(randomKey), new Text(randomValue));
        bytes -= (randomValue.getBytes().length + randomKey.getBytes().length);
      }
    }
  }
  
  /**
   * Configure the {@link Job} for enabling compression emulation.
   */
  static void configure(final Job job) throws IOException, InterruptedException,
                                              ClassNotFoundException {
    LOG.info("Gridmix is configured to generate compressed input data.");
    // set the random text mapper
    job.setMapperClass(RandomTextDataMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(GenDataFormat.class);
    job.setJarByClass(GenerateData.class);

    // set the output compression true
    FileOutputFormat.setCompressOutput(job, true);
    try {
      FileInputFormat.addInputPath(job, new Path("ignored"));
    } catch (IOException e) {
      LOG.error("Error while adding input path ", e);
    }
  }
  
  /** Publishes compression related data statistics. Following statistics are
   * published
   * <ul>
   *   <li>Total compressed input data size</li>
   *   <li>Number of compressed input data files</li>
   *   <li>Compression Ratio</li>
   *   <li>Text data dictionary size</li>
   *   <li>Random text word size</li>
   * </ul>
   */
  static void publishCompressedDataStatistics(Path inputDir, Configuration conf,
                                              long uncompressedDataSize) 
  throws IOException {
    LOG.info("Generation of compressed data successful.");
    FileSystem fs = inputDir.getFileSystem(conf);
    CompressionCodecFactory compressionCodecs = 
      new CompressionCodecFactory(conf);

    // iterate over compressed files and sum up the compressed file sizes
    long compressedDataSize = 0;
    int numCompressedFiles = 0;
    // obtain input data file statuses
    FileStatus[] outFileStatuses = 
      fs.listStatus(inputDir, new Utils.OutputFileUtils.OutputFilesFilter());
    for (FileStatus status : outFileStatuses) {
      // check if the input file is compressed
      if (compressionCodecs != null) {
        CompressionCodec codec = compressionCodecs.getCodec(status.getPath());
        if (codec != null) {
          ++numCompressedFiles;
          compressedDataSize += status.getLen();
        }
      }
    }

    // publish the input data size
    LOG.info("Total size of compressed input data (bytes) : " 
             + StringUtils.humanReadableInt(compressedDataSize));
    LOG.info("Total number of compressed input data files : " 
             + numCompressedFiles);

    // compute the compression ratio
    double ratio = ((double)compressedDataSize) / uncompressedDataSize;

    // publish the compression ratio
    LOG.info("Input Data Compression Ratio : " + ratio);

    // publish the random text data generator configuration parameters
    LOG.info("Compressed data generator list size : " 
        + RandomTextDataGenerator.getRandomTextDataGeneratorListSize(conf));
    LOG.info("Compressed data generator word size : " 
        + RandomTextDataGenerator.getRandomTextDataGeneratorWordSize(conf));
  }
  
  /**
   * Enables/Disables compression emulation.
   * @param conf Target configuration where the parameter 
   * {@value #COMPRESSION_EMULATION_ENABLE} will be set. 
   * @param val The value to be set.
   */
  static void setCompressionEmulationEnabled(Configuration conf, boolean val) {
    conf.setBoolean(COMPRESSION_EMULATION_ENABLE, val);
  }
  
  /**
   * Checks if compression emulation is enabled or not. Default is {@code true}.
   */
  static boolean isCompressionEmulationEnabled(Configuration conf) {
    return conf.getBoolean(COMPRESSION_EMULATION_ENABLE, true);
  }
  
  /**
   * Enables/Disables input decompression emulation.
   * @param conf Target configuration where the parameter 
   * {@value #INPUT_DECOMPRESSION_EMULATION_ENABLE} will be set. 
   * @param val The value to be set.
   */
  static void setInputCompressionEmulationEnabled(Configuration conf, 
                                                  boolean val) {
    conf.setBoolean(INPUT_DECOMPRESSION_EMULATION_ENABLE, val);
  }
  
  /**
   * Check if input decompression emulation is enabled or not. 
   * Default is {@code false}.
   */
  static boolean isInputCompressionEmulationEnabled(Configuration conf) {
    return conf.getBoolean(INPUT_DECOMPRESSION_EMULATION_ENABLE, false);
  }
  
  /**
   * Returns a {@link InputStream} for a file that might be compressed.
   */
  static InputStream getPossiblyDecompressedInputStream(Path file, 
                                                        Configuration conf,
                                                        long offset)
  throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    if (isCompressionEmulationEnabled(conf)
        && isInputCompressionEmulationEnabled(conf)) {
      CompressionCodecFactory compressionCodecs = 
        new CompressionCodecFactory(conf);
      CompressionCodec codec = compressionCodecs.getCodec(file);
      if (codec != null) {
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          CompressionInputStream in = 
            codec.createInputStream(fs.open(file), decompressor);
          //TODO Seek doesnt work with compressed input stream. 
          //     Use SplittableCompressionCodec?
          return (InputStream)in;
        }
      }
    }
    FSDataInputStream in = fs.open(file);
    in.seek(offset);
    return (InputStream)in;
  }
  
  /**
   * Returns a {@link OutputStream} for a file that might need 
   * compression.
   */
  static OutputStream getPossiblyCompressedOutputStream(Path file, 
                                                        Configuration conf)
  throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    JobConf jConf = new JobConf(conf);
    if (org.apache.hadoop.mapred.FileOutputFormat.getCompressOutput(jConf)) {
      // get the codec class
      Class<? extends CompressionCodec> codecClass =
        org.apache.hadoop.mapred.FileOutputFormat
                                .getOutputCompressorClass(jConf, 
                                                          GzipCodec.class);
      // get the codec implementation
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);

      // add the appropriate extension
      file = file.suffix(codec.getDefaultExtension());

      if (isCompressionEmulationEnabled(conf)) {
        FSDataOutputStream fileOut = fs.create(file, false);
        return new DataOutputStream(codec.createOutputStream(fileOut));
      }
    }
    return fs.create(file, false);
  }
  
  /**
   * Extracts compression/decompression related configuration parameters from 
   * the source configuration to the target configuration.
   */
  static void configureCompressionEmulation(Configuration source, 
                                            Configuration target) {
    // enable output compression
    target.setBoolean(FileOutputFormat.COMPRESS, 
        source.getBoolean(FileOutputFormat.COMPRESS, false));

    // set the job output compression codec
    String jobOutputCompressionCodec = 
      source.get(FileOutputFormat.COMPRESS_CODEC);
    if (jobOutputCompressionCodec != null) {
      target.set(FileOutputFormat.COMPRESS_CODEC, jobOutputCompressionCodec);
    }

    // set the job output compression type
    String jobOutputCompressionType = 
      source.get(FileOutputFormat.COMPRESS_TYPE);
    if (jobOutputCompressionType != null) {
      target.set(FileOutputFormat.COMPRESS_TYPE, jobOutputCompressionType);
    }

    // enable map output compression
    target.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS,
        source.getBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, false));

    // set the map output compression codecs
    String mapOutputCompressionCodec = 
      source.get(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC);
    if (mapOutputCompressionCodec != null) {
      target.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, 
                 mapOutputCompressionCodec);
    }

    // enable input decompression
    //TODO replace with mapInputBytes and hdfsBytesRead
    Path[] inputs = 
      org.apache.hadoop.mapred.FileInputFormat
         .getInputPaths(new JobConf(source));
    boolean needsCompressedInput = false;
    CompressionCodecFactory compressionCodecs = 
      new CompressionCodecFactory(source);
    for (Path input : inputs) {
      CompressionCodec codec = compressionCodecs.getCodec(input);
      if (codec != null) {
        needsCompressedInput = true;
      }
    }
    setInputCompressionEmulationEnabled(target, needsCompressedInput);
  }
}
