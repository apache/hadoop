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
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.hadoop.mapred.gridmix.GenerateData.DataStatistics;
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
   * Configuration property for setting the compression ratio for map input 
   * data.
   */
  private static final String GRIDMIX_MAP_INPUT_COMPRESSION_RATIO = 
    "gridmix.compression-emulation.map-input.decompression-ratio";
  
  /**
   * Configuration property for setting the compression ratio of map output.
   */
  private static final String GRIDMIX_MAP_OUTPUT_COMPRESSION_RATIO = 
    "gridmix.compression-emulation.map-output.compression-ratio";
  
  /**
   * Configuration property for setting the compression ratio of job output.
   */
  private static final String GRIDMIX_JOB_OUTPUT_COMPRESSION_RATIO = 
    "gridmix.compression-emulation.job-output.compression-ratio";
  
  /**
   * Default compression ratio.
   */
  static final float DEFAULT_COMPRESSION_RATIO = 0.5F;
  
  private static final CompressionRatioLookupTable COMPRESSION_LOOKUP_TABLE = 
    new CompressionRatioLookupTable();

  private static final Charset charsetUTF8 = Charset.forName("UTF-8");

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
        bytes -= (randomValue.getBytes(charsetUTF8).length +
            randomKey.getBytes(charsetUTF8).length);
      }
    }
  }
  
  /**
   * Configure the {@link Job} for enabling compression emulation.
   */
  static void configure(final Job job) throws IOException, InterruptedException,
                                              ClassNotFoundException {
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

  /**
   * This is the lookup table for mapping compression ratio to the size of the 
   * word in the {@link RandomTextDataGenerator}'s dictionary. 
   * 
   * Note that this table is computed (empirically) using a dictionary of 
   * default length i.e {@value RandomTextDataGenerator#DEFAULT_LIST_SIZE}.
   */
  private static class CompressionRatioLookupTable {
    private static Map<Float, Integer> map = new HashMap<Float, Integer>(60);
    private static final float MIN_RATIO = 0.07F;
    private static final float MAX_RATIO = 0.68F;
    
    // add the empirically obtained data points in the lookup table
    CompressionRatioLookupTable() {
      map.put(.07F,30);
      map.put(.08F,25);
      map.put(.09F,60);
      map.put(.10F,20);
      map.put(.11F,70);
      map.put(.12F,15);
      map.put(.13F,80);
      map.put(.14F,85);
      map.put(.15F,90);
      map.put(.16F,95);
      map.put(.17F,100);
      map.put(.18F,105);
      map.put(.19F,110);
      map.put(.20F,115);
      map.put(.21F,120);
      map.put(.22F,125);
      map.put(.23F,130);
      map.put(.24F,140);
      map.put(.25F,145);
      map.put(.26F,150);
      map.put(.27F,155);
      map.put(.28F,160);
      map.put(.29F,170);
      map.put(.30F,175);
      map.put(.31F,180);
      map.put(.32F,190);
      map.put(.33F,195);
      map.put(.34F,205);
      map.put(.35F,215);
      map.put(.36F,225);
      map.put(.37F,230);
      map.put(.38F,240);
      map.put(.39F,250);
      map.put(.40F,260);
      map.put(.41F,270);
      map.put(.42F,280);
      map.put(.43F,295);
      map.put(.44F,310);
      map.put(.45F,325);
      map.put(.46F,335);
      map.put(.47F,355);
      map.put(.48F,375);
      map.put(.49F,395);
      map.put(.50F,420);
      map.put(.51F,440);
      map.put(.52F,465);
      map.put(.53F,500);
      map.put(.54F,525);
      map.put(.55F,550);
      map.put(.56F,600);
      map.put(.57F,640);
      map.put(.58F,680);
      map.put(.59F,734);
      map.put(.60F,813);
      map.put(.61F,905);
      map.put(.62F,1000);
      map.put(.63F,1055);
      map.put(.64F,1160);
      map.put(.65F,1355);
      map.put(.66F,1510);
      map.put(.67F,1805);
      map.put(.68F,2170);
    }
    
    /**
     * Returns the size of the word in {@link RandomTextDataGenerator}'s 
     * dictionary that can generate text with the desired compression ratio.
     * 
     * @throws RuntimeException If ratio is less than {@value #MIN_RATIO} or 
     *                          greater than {@value #MAX_RATIO}.
     */
    int getWordSizeForRatio(float ratio) {
      ratio = standardizeCompressionRatio(ratio);
      if (ratio >= MIN_RATIO && ratio <= MAX_RATIO) {
        return map.get(ratio);
      } else {
        throw new RuntimeException("Compression ratio should be in the range [" 
          + MIN_RATIO + "," + MAX_RATIO + "]. Configured compression ratio is " 
          + ratio + ".");
      }
    }
  }
  
  /**
   * Setup the data generator's configuration to generate compressible random 
   * text data with the desired compression ratio.
   * Note that the compression ratio, if configured, will set the 
   * {@link RandomTextDataGenerator}'s list-size and word-size based on 
   * empirical values using the compression ratio set in the configuration. 
   * 
   * Hence to achieve the desired compression ratio, 
   * {@link RandomTextDataGenerator}'s list-size will be set to the default 
   * value i.e {@value RandomTextDataGenerator#DEFAULT_LIST_SIZE}.
   */
  static void setupDataGeneratorConfig(Configuration conf) {
    boolean compress = isCompressionEmulationEnabled(conf);
    if (compress) {
      float ratio = getMapInputCompressionEmulationRatio(conf);
      LOG.info("GridMix is configured to generate compressed input data with "
               + " a compression ratio of " + ratio);
      int wordSize = COMPRESSION_LOOKUP_TABLE.getWordSizeForRatio(ratio);
      RandomTextDataGenerator.setRandomTextDataGeneratorWordSize(conf, 
                                                                 wordSize);

      // since the compression ratios are computed using the default value of 
      // list size
      RandomTextDataGenerator.setRandomTextDataGeneratorListSize(conf, 
          RandomTextDataGenerator.DEFAULT_LIST_SIZE);
    }
  }
  
  /**
   * Returns a {@link RandomTextDataGenerator} that generates random 
   * compressible text with the desired compression ratio.
   */
  static RandomTextDataGenerator getRandomTextDataGenerator(float ratio, 
                                                            long seed) {
    int wordSize = COMPRESSION_LOOKUP_TABLE.getWordSizeForRatio(ratio);
    RandomTextDataGenerator rtg = 
      new RandomTextDataGenerator(RandomTextDataGenerator.DEFAULT_LIST_SIZE, 
            seed, wordSize);
    return rtg;
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
  static DataStatistics publishCompressedDataStatistics(Path inputDir, 
                          Configuration conf, long uncompressedDataSize) 
  throws IOException {
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

    LOG.info("Gridmix is configured to use compressed input data.");
    // publish the input data size
    LOG.info("Total size of compressed input data : " 
             + StringUtils.humanReadableInt(compressedDataSize));
    LOG.info("Total number of compressed input data files : " 
             + numCompressedFiles);

    if (numCompressedFiles == 0) {
      throw new RuntimeException("No compressed file found in the input" 
          + " directory : " + inputDir.toString() + ". To enable compression"
          + " emulation, run Gridmix either with "
          + " an input directory containing compressed input file(s) or" 
          + " use the -generate option to (re)generate it. If compression"
          + " emulation is not desired, disable it by setting '" 
          + COMPRESSION_EMULATION_ENABLE + "' to 'false'.");
    }
    
    // publish compression ratio only if its generated in this gridmix run
    if (uncompressedDataSize > 0) {
      // compute the compression ratio
      double ratio = ((double)compressedDataSize) / uncompressedDataSize;

      // publish the compression ratio
      LOG.info("Input Data Compression Ratio : " + ratio);
    }
    
    return new DataStatistics(compressedDataSize, numCompressedFiles, true);
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
   * Set the map input data compression ratio in the given conf.
   */
  static void setMapInputCompressionEmulationRatio(Configuration conf, 
                                                   float ratio) {
    conf.setFloat(GRIDMIX_MAP_INPUT_COMPRESSION_RATIO, ratio);
  }
  
  /**
   * Get the map input data compression ratio using the given configuration.
   * If the compression ratio is not set in the configuration then use the 
   * default value i.e {@value #DEFAULT_COMPRESSION_RATIO}.
   */
  static float getMapInputCompressionEmulationRatio(Configuration conf) {
    return conf.getFloat(GRIDMIX_MAP_INPUT_COMPRESSION_RATIO, 
                         DEFAULT_COMPRESSION_RATIO);
  }
  
  /**
   * Set the map output data compression ratio in the given configuration.
   */
  static void setMapOutputCompressionEmulationRatio(Configuration conf, 
                                                    float ratio) {
    conf.setFloat(GRIDMIX_MAP_OUTPUT_COMPRESSION_RATIO, ratio);
  }
  
  /**
   * Get the map output data compression ratio using the given configuration.
   * If the compression ratio is not set in the configuration then use the 
   * default value i.e {@value #DEFAULT_COMPRESSION_RATIO}.
   */
  static float getMapOutputCompressionEmulationRatio(Configuration conf) {
    return conf.getFloat(GRIDMIX_MAP_OUTPUT_COMPRESSION_RATIO, 
                         DEFAULT_COMPRESSION_RATIO);
  }
  
  /**
   * Set the job output data compression ratio in the given configuration.
   */
  static void setJobOutputCompressionEmulationRatio(Configuration conf, 
                                                    float ratio) {
    conf.setFloat(GRIDMIX_JOB_OUTPUT_COMPRESSION_RATIO, ratio);
  }
  
  /**
   * Get the job output data compression ratio using the given configuration.
   * If the compression ratio is not set in the configuration then use the 
   * default value i.e {@value #DEFAULT_COMPRESSION_RATIO}.
   */
  static float getJobOutputCompressionEmulationRatio(Configuration conf) {
    return conf.getFloat(GRIDMIX_JOB_OUTPUT_COMPRESSION_RATIO, 
                         DEFAULT_COMPRESSION_RATIO);
  }
  
  /**
   * Standardize the compression ratio i.e round off the compression ratio to
   * only 2 significant digits.
   */
  static float standardizeCompressionRatio(float ratio) {
    // round off to 2 significant digits
    int significant = (int)Math.round(ratio * 100);
    return ((float)significant)/100;
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

  /**
   * Get the uncompressed input bytes count from the given possibly compressed
   * input bytes count.
   * @param possiblyCompressedInputBytes input bytes count. This is compressed
   *        input size if compression emulation is on.
   * @param conf configuration of the Gridmix simulated job
   * @return uncompressed input bytes count. Compute this in case if compressed
   *         input was used
   */
  static long getUncompressedInputBytes(long possiblyCompressedInputBytes,
                                        Configuration conf) {
    long uncompressedInputBytes = possiblyCompressedInputBytes;

    if (CompressionEmulationUtil.isInputCompressionEmulationEnabled(conf)) {
      float inputCompressionRatio =
          CompressionEmulationUtil.getMapInputCompressionEmulationRatio(conf);
      uncompressedInputBytes /= inputCompressionRatio;
    }
    return uncompressedInputBytes;
  }
}
