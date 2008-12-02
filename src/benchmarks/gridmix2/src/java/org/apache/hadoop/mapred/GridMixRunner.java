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

package org.apache.hadoop.mapred;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.io.IOException;

import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.jobcontrol.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.mapred.GridMixConfig;

public class GridMixRunner {

  private static int NUM_OF_LARGE_JOBS_PER_CLASS = 0;

  private static int NUM_OF_MEDIUM_JOBS_PER_CLASS = 0;

  private static int NUM_OF_SMALL_JOBS_PER_CLASS = 0;

  private static int NUM_OF_REDUCERS_FOR_LARGE_JOB = 370;

  private static int NUM_OF_REDUCERS_FOR_MEDIUM_JOB = 170;

  private static int NUM_OF_REDUCERS_FOR_SMALL_JOB = 15;

  private static String GRID_MIX_DATA = "/gridmix/data";

  private static String VARCOMPSEQ = GRID_MIX_DATA
      + "/WebSimulationBlockCompressed";

  private static String FIXCOMPSEQ = GRID_MIX_DATA
      + "/MonsterQueryBlockCompressed";

  private static String VARINFLTEXT = GRID_MIX_DATA + "/SortUncompressed";

  private JobControl gridmix;

  private FileSystem fs;

  private GridMixConfig config;

  private static final String GRIDMIXCONFIG = "gridmix_config.xml";

  private int numOfJobs = 0;

  private void initConfig() {
    String configFile = System.getenv("GRIDMIXCONFIG");
    if (configFile == null) {
      String configDir = System.getProperty("user.dir");
      if (configDir == null) {
        configDir = ".";
      }
      configFile = configDir + "/" + GRIDMIXCONFIG;
    }

    if (config == null) {
      try {
        Path fileResource = new Path(configFile);
        config = new GridMixConfig();
        config.addResource(fileResource);
      } catch (Exception e) {
        System.out.println("Error reading configuration file:" + configFile);
      }
    }
  }

  public GridMixRunner() throws IOException {
    gridmix = new JobControl("GridMix");
    Configuration conf = new Configuration();
    try {
      fs = FileSystem.get(conf);
    } catch (IOException ex) {
      System.out.println("fs initation error:" + ex.getMessage());
      throw ex;
    }
    initConfig();
  }

  private void addStreamSort(int num_of_reducers, boolean mapoutputCompressed,
      boolean outputCompressed, String size) {

    String defaultIndir = VARINFLTEXT + "/{part-00000,part-00001,part-00002}";
    String indir = getInputDirsFor("streamSort.smallJobs.inputFiles",
        defaultIndir);
    String outdir = addTSSuffix("perf-out/stream-out-dir-small_");
    if ("medium".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARINFLTEXT + "/{part-000*0,part-000*1,part-000*2}";
      indir = getInputDirsFor("streamSort.mediumJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/stream-out-dir-medium_");
    } else if ("large".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARINFLTEXT;
      indir = getInputDirsFor("streamSort.largeJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/stream-out-dir-large_");
    }

    StringBuffer sb = new StringBuffer();

    sb.append("-input ").append(indir).append(" ");
    sb.append("-output ").append(outdir).append(" ");
    sb.append("-mapper cat ");
    sb.append("-reducer cat ");
    sb.append("-numReduceTasks ").append(num_of_reducers);

    String[] args = sb.toString().split(" ");

    clearDir(outdir);
    try {
      JobConf jobconf = StreamJob.createJob(args);
      jobconf.setJobName("GridmixStreamingSorter." + size);
      jobconf.setCompressMapOutput(mapoutputCompressed);
      jobconf.setBoolean("mapred.output.compress", outputCompressed);

      Job job = new Job(jobconf);
      gridmix.addJob(job);
      numOfJobs++;
    } catch (Exception ex) {
      ex.printStackTrace();
      System.out.println(ex.toString());
    }

  }

  private String getInputDirsFor(String jobType, String defaultIndir) {
    String inputFile[] = config.getStrings(jobType, defaultIndir);
    StringBuffer indirBuffer = new StringBuffer();
    for (int i = 0; i < inputFile.length; i++) {
      indirBuffer = indirBuffer.append(inputFile[i]).append(",");
    }
    return indirBuffer.substring(0, indirBuffer.length() - 1);
  }

  private void addStreamSortSmall(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addStreamSort(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "small");
  }

  private void addStreamSortMedium(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addStreamSort(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "medium");
  }

  private void addStreamSortLarge(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addStreamSort(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "large");
  }

  private void clearDir(String dir) {
    try {
      Path outfile = new Path(dir);
      fs.delete(outfile);
    } catch (IOException ex) {
      ex.printStackTrace();
      System.out.println("delete file error:");
      System.out.println(ex.toString());
    }
  }

  private void addJavaSort(int num_of_reducers, boolean mapoutputCompressed,
      boolean outputCompressed, String size) {

    String defaultIndir = VARINFLTEXT + "/{part-00000,part-00001,part-00002}";
    String indir = getInputDirsFor("javaSort.smallJobs.inputFiles",
        defaultIndir);
    String outdir = addTSSuffix("perf-out/sort-out-dir-small_");
    if ("medium".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARINFLTEXT + "/{part-000*0,part-000*1,part-000*2}";
      indir = getInputDirsFor("javaSort.mediumJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/sort-out-dir-medium_");
    } else if ("large".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARINFLTEXT;
      indir = getInputDirsFor("javaSort.largeJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/sort-out-dir-large_");
    }

    clearDir(outdir);

    try {
      JobConf jobConf = new JobConf();
      jobConf.setJarByClass(Sort.class);
      jobConf.setJobName("GridmixJavaSorter." + size);
      jobConf.setMapperClass(IdentityMapper.class);
      jobConf.setReducerClass(IdentityReducer.class);

      jobConf.setNumReduceTasks(num_of_reducers);
      jobConf
          .setInputFormat(org.apache.hadoop.mapred.KeyValueTextInputFormat.class);
      jobConf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

      jobConf.setOutputKeyClass(org.apache.hadoop.io.Text.class);
      jobConf.setOutputValueClass(org.apache.hadoop.io.Text.class);
      jobConf.setCompressMapOutput(mapoutputCompressed);
      jobConf.setBoolean("mapred.output.compress", outputCompressed);

      FileInputFormat.addInputPaths(jobConf, indir);

      FileOutputFormat.setOutputPath(jobConf, new Path(outdir));

      Job job = new Job(jobConf);

      gridmix.addJob(job);
      numOfJobs++;

    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }

  private void addJavaSortSmall(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addJavaSort(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed, "small");
  }

  private void addJavaSortMedium(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addJavaSort(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "medium");
  }

  private void addJavaSortLarge(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addJavaSort(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed, "large");
  }

  private boolean select(int total, int selected, int index) {
    int step;
    if (selected > 0 && selected < total) {
      step = total / selected;
    } else if (selected <= 0) {
      return false;
    } else {
      return true;
    }

    int effectiveTotal = total - total % selected;

    if (index <= effectiveTotal - 1 && (index % step == 0)) {
      return true;
    } else {
      return false;
    }
  }

  private void addTextSortJobs() {

    int[] nums_of_small_streamsort_job = config.getInts(
        "streamSort.smallJobs.numOfJobs", NUM_OF_SMALL_JOBS_PER_CLASS);
    int[] nums_of_small_javasort_job = config.getInts(
        "javaSort.smallJobs.numOfJobs", NUM_OF_SMALL_JOBS_PER_CLASS);

    int num_of_small_streamsort_job_mapoutputCompressed = config.getInt(
        "streamSort.smallJobs.numOfMapoutputCompressed", 0);
    int num_of_small_javasort_job_mapoutputCompressed = config.getInt(
        "javaSort.smallJobs.numOfMapoutputCompressed", 0);

    int num_of_small_streamsort_job_outputCompressed = config.getInt(
        "streamSort.smallJobs.numOfOutputCompressed",
        NUM_OF_SMALL_JOBS_PER_CLASS);
    int num_of_small_javasort_job_outputCompressed = config
        .getInt("javaSort.smallJobs.numOfOutputCompressed",
            NUM_OF_SMALL_JOBS_PER_CLASS);

    int[] streamsort_smallJobs_numsOfReduces = config.getInts(
        "streamSort.smallJobs.numOfReduces", NUM_OF_REDUCERS_FOR_SMALL_JOB);
    int[] javasort_smallJobs_numsOfReduces = config.getInts(
        "javaSort.smallJobs.numOfReduces", NUM_OF_REDUCERS_FOR_SMALL_JOB);

    int len1, len2;

    len1 = nums_of_small_streamsort_job.length;
    len2 = streamsort_smallJobs_numsOfReduces.length;

    if (len1 != len2) {
      System.out
          .println(" Configuration error: "
              + "streamSort.smallJobs.numOfJobs and streamSort.smallJobs.numOfReduces must have the same number of items");

    }
    int totalNum = 0;
    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_small_streamsort_job[i];
    }
    int currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_small_streamsort_job = nums_of_small_streamsort_job[index];
      int streamsort_smallJobs_numOfReduces = streamsort_smallJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_small_streamsort_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_small_streamsort_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_small_streamsort_job_outputCompressed, currentIndex);
        addStreamSortSmall(streamsort_smallJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    len1 = nums_of_small_javasort_job.length;
    len2 = javasort_smallJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_small_javasort_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: javaSort.smallJobs.numOfJobs, "
              + "javaSort.smallJobs.numOfReduces must have the same number of items");

    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_small_javasort_job = nums_of_small_javasort_job[index];
      int javasort_smallJobs_numOfReduces = javasort_smallJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_small_javasort_job; i++) {

        boolean mapoutputCompressed = select(totalNum,
            num_of_small_javasort_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_small_javasort_job_outputCompressed, currentIndex);

        addJavaSortSmall(javasort_smallJobs_numOfReduces, mapoutputCompressed,
            outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_medium_streamsort_job = config.getInts(
        "streamSort.mediumJobs.numOfJobs", NUM_OF_MEDIUM_JOBS_PER_CLASS);
    int[] nums_of_medium_javasort_job = config.getInts(
        "javaSort.mediumJobs.numOfJobs", NUM_OF_MEDIUM_JOBS_PER_CLASS);

    int num_of_medium_streamsort_job_mapoutputCompressed = config.getInt(
        "streamSort.mediumJobs.numOfMapoutputCompressed", 0);
    int num_of_medium_javasort_job_mapoutputCompressed = config.getInt(
        "javaSort.mediumJobs.numOfMapoutputCompressed", 0);

    int num_of_medium_streamsort_job_outputCompressed = config.getInt(
        "streamSort.mediumJobs.numOfOutputCompressed",
        NUM_OF_MEDIUM_JOBS_PER_CLASS);
    int num_of_medium_javasort_job_outputCompressed = config.getInt(
        "javaSort.mediumJobs.numOfOutputCompressed",
        NUM_OF_MEDIUM_JOBS_PER_CLASS);

    int[] streamsort_mediumJobs_numsOfReduces = config.getInts(
        "streamSort.mediumJobs.numOfReduces", NUM_OF_REDUCERS_FOR_MEDIUM_JOB);
    int[] javasort_mediumJobs_numsOfReduces = config.getInts(
        "javaSort.mediumJobs.numOfReduces", NUM_OF_REDUCERS_FOR_MEDIUM_JOB);

    len1 = nums_of_medium_streamsort_job.length;
    len2 = streamsort_mediumJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_medium_streamsort_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: streamSort.mediumJobs.numOfJobs, "
              + "streamSort.mediumJobs.numOfReduces must have the same number of items");

    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_medium_streamsort_job = nums_of_medium_streamsort_job[index];
      int streamsort_mediumJobs_numOfReduces = streamsort_mediumJobs_numsOfReduces[index];

      for (int i = 0; i < num_of_medium_streamsort_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_medium_streamsort_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_medium_streamsort_job_outputCompressed, currentIndex);

        addStreamSortMedium(streamsort_mediumJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;

      }
    }

    len1 = nums_of_medium_javasort_job.length;
    len2 = javasort_mediumJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_medium_javasort_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: javaSort.mediumJobs.numOfJobs, "
              + "javaSort.mediumJobs.numOfReduces must have the same number of items");

    }
    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_medium_javasort_job = nums_of_medium_javasort_job[index];
      int javasort_mediumJobs_numOfReduces = javasort_mediumJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_medium_javasort_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_medium_javasort_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_medium_javasort_job_outputCompressed, currentIndex);

        addJavaSortMedium(javasort_mediumJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_large_streamsort_job = config.getInts(
        "streamSort.largeJobs.numOfJobs", NUM_OF_LARGE_JOBS_PER_CLASS);
    int[] nums_of_large_javasort_job = config.getInts(
        "javaSort.largeJobs.numOfJobs", NUM_OF_LARGE_JOBS_PER_CLASS);

    int num_of_large_streamsort_job_mapoutputCompressed = config.getInt(
        "streamSort.largeJobs.numOfMapoutputCompressed", 0);
    int num_of_large_javasort_job_mapoutputCompressed = config.getInt(
        "javaSort.largeJobs.numOfMapoutputCompressed", 0);

    int num_of_large_streamsort_job_outputCompressed = config.getInt(
        "streamSort.largeJobs.numOfOutputCompressed",
        NUM_OF_LARGE_JOBS_PER_CLASS);
    int num_of_large_javasort_job_outputCompressed = config
        .getInt("javaSort.largeJobs.numOfOutputCompressed",
            NUM_OF_LARGE_JOBS_PER_CLASS);

    int[] streamsort_largeJobs_numsOfReduces = config.getInts(
        "streamSort.largeJobs.numOfReduces", NUM_OF_REDUCERS_FOR_LARGE_JOB);
    int[] javasort_largeJobs_numsOfReduces = config.getInts(
        "javaSort.largeJobs.numOfReduces", NUM_OF_REDUCERS_FOR_LARGE_JOB);

    len1 = nums_of_large_streamsort_job.length;
    len2 = streamsort_largeJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_large_streamsort_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: streamSort.largeJobs.numOfJobs, "
              + "streamSort.largeJobs.numOfReduces must have the same number of items");

    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_large_streamsort_job = nums_of_large_streamsort_job[index];
      int streamsort_largeJobs_numOfReduces = streamsort_largeJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_large_streamsort_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_large_streamsort_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_large_streamsort_job_outputCompressed, currentIndex);
        addStreamSortLarge(streamsort_largeJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    len1 = nums_of_large_javasort_job.length;
    len2 = javasort_largeJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_large_javasort_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: javaSort.largeJobs.numOfJobs, "
              + "javaSort.largeJobs.numOfReduces must have the same number of items");

    }
    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_large_javasort_job = nums_of_large_javasort_job[index];
      int javasort_largeJobs_numOfReduces = javasort_largeJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_large_javasort_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_large_javasort_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_large_javasort_job_outputCompressed, currentIndex);

        addJavaSortLarge(javasort_largeJobs_numOfReduces, mapoutputCompressed,
            outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

  }

  private void addWebdataScan(int num_of_reducers, boolean mapoutputCompressed,
      boolean outputCompressed, String size) {
    String defaultIndir = VARCOMPSEQ + "/{part-00000,part-00001,part-00002}";
    String indir = getInputDirsFor("webdataScan.smallJobs.inputFiles",
        defaultIndir);
    String outdir = addTSSuffix("perf-out/webdata-scan-out-dir-small_");
    if ("medium".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARCOMPSEQ + "/{part-000*0,part-000*1,part-000*2}";
      indir = getInputDirsFor("webdataScan.mediumJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/webdata-scan-out-dir-medium_");
    } else if ("large".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARCOMPSEQ;
      indir = getInputDirsFor("webdataScan.largeJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/webdata-scan-dir-large_");
    }

    GenericMRLoadJobCreator jobcreator = new GenericMRLoadJobCreator();
    StringBuffer sb = new StringBuffer();
    sb.append("-keepmap 0.2 ");
    sb.append("-keepred 5 ");
    sb.append("-inFormat org.apache.hadoop.mapred.SequenceFileInputFormat ");
    sb.append("-outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ");
    sb.append("-outKey org.apache.hadoop.io.Text ");
    sb.append("-outValue org.apache.hadoop.io.Text ");
    sb.append("-indir ").append(indir).append(" ");
    sb.append("-outdir ").append(outdir).append(" ");
    sb.append("-r ").append(num_of_reducers);

    String[] args = sb.toString().split(" ");
    clearDir(outdir);
    try {
      JobConf jobconf = jobcreator.createJob(args, mapoutputCompressed,
          outputCompressed);
      jobconf.setJobName("GridmixWebdatascan." + size);
      Job job = new Job(jobconf);
      gridmix.addJob(job);
      numOfJobs++;
    } catch (Exception ex) {
      System.out.println(ex.getStackTrace());
    }

  }

  private void addWebdataScanSmall(int num_of_reducers,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addWebdataScan(num_of_reducers, mapoutputCompressed, outputCompressed,
        "small");
  }

  private void addWebdataScanMedium(int num_of_reducers,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addWebdataScan(num_of_reducers, mapoutputCompressed, outputCompressed,
        "medium");
  }

  private void addWebdataScanLarge(int num_of_reducers,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addWebdataScan(num_of_reducers, mapoutputCompressed, outputCompressed,
        "large");
  }

  private void addWebdataScanJobs() {

    int[] nums_of_small_webdatascan_job = config.getInts(
        "webdataScan.smallJobs.numOfJobs", NUM_OF_SMALL_JOBS_PER_CLASS);
    int num_of_small_webdatascan_job_mapoutputCompressed = config.getInt(
        "webdataScan.smallJobs.numOfMapoutputCompressed", 0);
    int num_of_small_webdatascan_job_outputCompressed = config.getInt(
        "webdataScan.smallJobs.numOfOutputCompressed",
        NUM_OF_REDUCERS_FOR_SMALL_JOB);

    int[] webdatascan_smallJobs_numsOfReduces = config.getInts(
        "webdataScan.smallJobs.numOfReduces", NUM_OF_REDUCERS_FOR_SMALL_JOB);
    int len1, len2, totalNum, currentIndex;

    len1 = nums_of_small_webdatascan_job.length;
    len2 = webdatascan_smallJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_small_webdatascan_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: webdataScan.smallJobs.numOfJobs, "
              + "webdataScan.smallJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_small_webdatascan_job = nums_of_small_webdatascan_job[index];
      int webdatascan_smallJobs_numOfReduces = webdatascan_smallJobs_numsOfReduces[index];

      for (int i = 0; i < num_of_small_webdatascan_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_small_webdatascan_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_small_webdatascan_job_outputCompressed, currentIndex);
        addWebdataScanSmall(webdatascan_smallJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_medium_webdatascan_job = config.getInts(
        "webdataScan.mediumJobs.numOfJobs", NUM_OF_MEDIUM_JOBS_PER_CLASS);
    int num_of_medium_webdatascan_job_mapoutputCompressed = config.getInt(
        "webdataScan.mediumJobs.numOfMapoutputCompressed", 0);
    int num_of_medium_webdatascan_job_outputCompressed = config.getInt(
        "webdataScan.mediumJobs.numOfOutputCompressed",
        NUM_OF_REDUCERS_FOR_MEDIUM_JOB);

    int[] webdatascan_mediumJobs_numsOfReduces = config.getInts(
        "webdataScan.mediumJobs.numOfReduces", NUM_OF_REDUCERS_FOR_MEDIUM_JOB);

    len1 = nums_of_medium_webdatascan_job.length;
    len2 = webdatascan_mediumJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_medium_webdatascan_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: webdataScan.mediumJobs.numOfJobs, "
              + "webdataScan.mediumJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_medium_webdatascan_job = nums_of_medium_webdatascan_job[index];
      int webdatascan_mediumJobs_numOfReduces = webdatascan_mediumJobs_numsOfReduces[index];

      for (int i = 0; i < num_of_medium_webdatascan_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_medium_webdatascan_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_medium_webdatascan_job_outputCompressed, currentIndex);
        addWebdataScanMedium(webdatascan_mediumJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_large_webdatascan_job = config.getInts(
        "webdataScan.largeJobs.numOfJobs", NUM_OF_LARGE_JOBS_PER_CLASS);
    int num_of_large_webdatascan_job_mapoutputCompressed = config.getInt(
        "webdataScan.largeJobs.numOfMapoutputCompressed", 0);
    int num_of_large_webdatascan_job_outputCompressed = config.getInt(
        "webdataScan.largeJobs.numOfOutputCompressed",
        NUM_OF_REDUCERS_FOR_LARGE_JOB);

    int[] webdatascan_largeJobs_numsOfReduces = config.getInts(
        "webdataScan.largeJobs.numOfReduces", NUM_OF_REDUCERS_FOR_LARGE_JOB);

    len1 = nums_of_large_webdatascan_job.length;
    len2 = webdatascan_largeJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_large_webdatascan_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: webdataScan.largeJobs.numOfJobs, "
              + "webdataScan.largeJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_large_webdatascan_job = nums_of_large_webdatascan_job[index];
      int webdatascan_largeJobs_numOfReduces = webdatascan_largeJobs_numsOfReduces[index];

      for (int i = 0; i < num_of_large_webdatascan_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_large_webdatascan_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_large_webdatascan_job_outputCompressed, currentIndex);
        addWebdataScanLarge(webdatascan_largeJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

  }

  private void addCombiner(int num_of_reducers, boolean mapoutputCompressed,
      boolean outputCompressed, String size) {

    String defaultIndir = VARCOMPSEQ + "/{part-00000,part-00001,part-00002}";
    String indir = getInputDirsFor("combiner.smallJobs.inputFiles",
        defaultIndir);
    String outdir = addTSSuffix("perf-out/combiner-out-dir-small_");
    if ("medium".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARCOMPSEQ + "/{part-000*0,part-000*1,part-000*2}";
      indir = getInputDirsFor("combiner.mediumJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/combiner-out-dir-medium_");
    } else if ("large".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARCOMPSEQ;
      indir = getInputDirsFor("combiner.largeJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/combiner-dir-large_");
    }

    CombinerJobCreator jobcreator = new CombinerJobCreator();
    StringBuffer sb = new StringBuffer();
    sb.append("-r ").append(num_of_reducers).append(" ");
    sb.append("-indir ").append(indir).append(" ");
    sb.append("-outdir ").append(outdir);
    sb.append("-mapoutputCompressed ").append(mapoutputCompressed).append(" ");
    sb.append("-outputCompressed ").append(outputCompressed);

    String[] args = sb.toString().split(" ");
    clearDir(outdir);
    try {
      JobConf jobconf = jobcreator.createJob(args);
      jobconf.setJobName("GridmixCombinerJob." + size);
      Job job = new Job(jobconf);
      gridmix.addJob(job);
      numOfJobs++;
    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }

  private void addCombinerSmall(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addCombiner(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed, "small");
  }

  private void addCombinerMedium(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addCombiner(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed, "medium");
  }

  private void addCombinerLarge(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addCombiner(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed, "large");
  }

  private void addCombinerJobs() {
    int[] nums_of_small_combiner_job = config.getInts(
        "combiner.smallJobs.numOfJobs", NUM_OF_SMALL_JOBS_PER_CLASS);
    int num_of_small_combiner_job_mapoutputCompressed = config.getInt(
        "combiner.smallJobs.numOfMapoutputCompressed", 0);
    int num_of_small_combiner_job_outputCompressed = config
        .getInt("combiner.smallJobs.numOfOutputCompressed",
            NUM_OF_SMALL_JOBS_PER_CLASS);
    int[] combiner_smallJobs_numsOfReduces = config.getInts(
        "combiner.smallJobs.numOfReduces", NUM_OF_REDUCERS_FOR_SMALL_JOB);
    int len1, len2, totalNum, currentIndex;

    len1 = nums_of_small_combiner_job.length;
    len2 = combiner_smallJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_small_combiner_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: combiner.smallJobs.numOfJobs, "
              + "combiner.smallJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_small_combiner_job = nums_of_small_combiner_job[index];
      int combiner_smallJobs_numOfReduces = combiner_smallJobs_numsOfReduces[index];

      for (int i = 0; i < num_of_small_combiner_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_small_combiner_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_small_combiner_job_outputCompressed, currentIndex);
        addCombinerSmall(combiner_smallJobs_numOfReduces, mapoutputCompressed,
            outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_medium_combiner_job = config.getInts(
        "combiner.mediumJobs.numOfJobs", NUM_OF_MEDIUM_JOBS_PER_CLASS);
    int num_of_medium_combiner_job_mapoutputCompressed = config.getInt(
        "combiner.mediumJobs.numOfMapoutputCompressed", 0);
    int num_of_medium_combiner_job_outputCompressed = config.getInt(
        "combiner.mediumJobs.numOfOutputCompressed",
        NUM_OF_MEDIUM_JOBS_PER_CLASS);
    int[] combiner_mediumJobs_numsOfReduces = config.getInts(
        "combiner.mediumJobs.numOfReduces", NUM_OF_REDUCERS_FOR_MEDIUM_JOB);

    len1 = nums_of_medium_combiner_job.length;
    len2 = combiner_mediumJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_medium_combiner_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: combiner.mediumJobs.numOfJobs, "
              + "combiner.mediumJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_medium_combiner_job = nums_of_medium_combiner_job[index];
      int combiner_mediumJobs_numOfReduces = combiner_mediumJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_medium_combiner_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_medium_combiner_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_medium_combiner_job_outputCompressed, currentIndex);

        addCombinerMedium(combiner_mediumJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_large_combiner_job = config.getInts(
        "combiner.largeJobs.numOfJobs", NUM_OF_LARGE_JOBS_PER_CLASS);
    int num_of_large_combiner_job_mapoutputCompressed = config.getInt(
        "combiner.largeJobs.numOfMapoutputCompressed", 0);
    int num_of_large_combiner_job_outputCompressed = config
        .getInt("combiner.largeJobs.numOfOutputCompressed",
            NUM_OF_LARGE_JOBS_PER_CLASS);
    int[] combiner_largeJobs_numsOfReduces = config.getInts(
        "combiner.largeJobs.numOfReduces", NUM_OF_REDUCERS_FOR_LARGE_JOB);

    len1 = nums_of_large_combiner_job.length;
    len2 = combiner_largeJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_large_combiner_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: combiner.largeJobs.numOfJobs, "
              + "combiner.largeJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_large_combiner_job = nums_of_large_combiner_job[index];
      int combiner_largeJobs_numOfReduces = combiner_largeJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_large_combiner_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_large_combiner_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_large_combiner_job_outputCompressed, currentIndex);

        addCombinerLarge(combiner_largeJobs_numOfReduces, mapoutputCompressed,
            outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

  }

  private void addMonsterQuery(int num_of_reducer, boolean mapoutputCompressed,
      boolean outputCompressed, String size) {
    GenericMRLoadJobCreator jobcreator = new GenericMRLoadJobCreator();
    String defaultIndir = FIXCOMPSEQ + "/{part-00000,part-00001,part-00002}";
    String indir = getInputDirsFor("monsterQuery.smallJobs.inputFiles",
        defaultIndir);
    String outdir = addTSSuffix("perf-out/mq-out-dir-small_");

    if ("medium".compareToIgnoreCase(size) == 0) {
      defaultIndir = FIXCOMPSEQ + "/{part-000*0,part-000*1,part-000*2}";
      indir = getInputDirsFor("monsterQuery.mediumJobs.inputFiles",
          defaultIndir);
      outdir = addTSSuffix("perf-out/mq-out-dir-medium_");
    } else if ("large".compareToIgnoreCase(size) == 0) {
      defaultIndir = FIXCOMPSEQ;
      indir = getInputDirsFor("monsterQuery.largeJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/mq-out-dir-large_");
    }

    int iter = 3;
    try {

      Job pjob = null;
      Job job = null;
      for (int i = 0; i < iter; i++) {
        String outdirfull = outdir + "." + i;
        String indirfull;
        if (i == 0) {
          indirfull = indir;
        } else {
          indirfull = outdir + "." + (i - 1);
        }
        Path outfile = new Path(outdirfull);

        StringBuffer sb = new StringBuffer();

        sb.append("-keepmap 10 ");
        sb.append("-keepred 40 ");
        sb
            .append("-inFormat org.apache.hadoop.mapred.SequenceFileInputFormat ");
        sb
            .append("-outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ");
        sb.append("-outKey org.apache.hadoop.io.Text ");
        sb.append("-outValue org.apache.hadoop.io.Text ");

        sb.append("-indir ").append(indirfull).append(" ");

        sb.append("-outdir ").append(outdirfull).append(" ");
        sb.append("-r ").append(num_of_reducer);

        String[] args = sb.toString().split(" ");

        try {
          fs.delete(outfile);
        } catch (IOException ex) {
          System.out.println(ex.toString());
        }

        JobConf jobconf = jobcreator.createJob(args, mapoutputCompressed,
            outputCompressed);
        jobconf.setJobName("GridmixMonsterQuery." + size);
        job = new Job(jobconf);
        if (pjob != null) {
          job.addDependingJob(pjob);
        }
        gridmix.addJob(job);
        numOfJobs++;
        pjob = job;

      }

    } catch (Exception e) {
      System.out.println(e.getStackTrace());
    }
  }

  private void addMonsterQuerySmall(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addMonsterQuery(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "small");
  }

  private void addMonsterQueryMedium(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addMonsterQuery(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "medium");
  }

  private void addMonsterQueryLarge(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addMonsterQuery(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "large");
  }

  private void addMonsterQueryJobs() {
    int[] nums_of_small_monsterquery_job = config.getInts(
        "monsterQuery.smallJobs.numOfJobs", NUM_OF_SMALL_JOBS_PER_CLASS);
    int num_of_small_monsterquery_job_mapoutputCompressed = config.getInt(
        "monsterQuery.smallJobs.numOfMapoutputCompressed", 0);
    int num_of_small_monsterquery_job_outputCompressed = config.getInt(
        "monsterQuery.smallJobs.numOfOutputCompressed",
        NUM_OF_SMALL_JOBS_PER_CLASS);
    int[] monsterquery_smallJobs_numsOfReduces = config.getInts(
        "monsterQuery.smallJobs.numOfReduces", NUM_OF_REDUCERS_FOR_SMALL_JOB);
    int len1, len2, totalNum, currentIndex;

    len1 = nums_of_small_monsterquery_job.length;
    len2 = monsterquery_smallJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_small_monsterquery_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: monseterquery.smallJobs.numOfJobs, "
              + "monsterquery.smallJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_small_monsterquery_job = nums_of_small_monsterquery_job[index];
      int monsterquery_smallJobs_numOfReduces = monsterquery_smallJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_small_monsterquery_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_small_monsterquery_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_small_monsterquery_job_outputCompressed, currentIndex);

        addMonsterQuerySmall(monsterquery_smallJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_medium_monsterquery_job = config.getInts(
        "monsterQuery.mediumJobs.numOfJobs", NUM_OF_MEDIUM_JOBS_PER_CLASS);
    int num_of_medium_monsterquery_job_mapoutputCompressed = config.getInt(
        "monsterQuery.mediumJobs.numOfMapoutputCompressed", 0);
    int num_of_medium_monsterquery_job_outputCompressed = config.getInt(
        "monsterQuery.mediumJobs.numOfOutputCompressed",
        NUM_OF_MEDIUM_JOBS_PER_CLASS);
    int[] monsterquery_mediumJobs_numsOfReduces = config.getInts(
        "monsterQuery.mediumJobs.numOfReduces", NUM_OF_REDUCERS_FOR_MEDIUM_JOB);
    len1 = nums_of_medium_monsterquery_job.length;
    len2 = monsterquery_mediumJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_medium_monsterquery_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: monseterquery.mediumJobs.numOfJobs, "
              + "monsterquery.mediumJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_medium_monsterquery_job = nums_of_medium_monsterquery_job[index];
      int monsterquery_mediumJobs_numOfReduces = monsterquery_mediumJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_medium_monsterquery_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_medium_monsterquery_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_medium_monsterquery_job_outputCompressed, currentIndex);

        addMonsterQueryMedium(monsterquery_mediumJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_large_monsterquery_job = config.getInts(
        "monsterQuery.largeJobs.numOfJobs", NUM_OF_LARGE_JOBS_PER_CLASS);
    int num_of_large_monsterquery_job_mapoutputCompressed = config.getInt(
        "monsterQuery.largeJobs.numOfMapoutputCompressed", 0);
    int num_of_large_monsterquery_job_outputCompressed = config.getInt(
        "monsterQuery.largeJobs.numOfOutputCompressed",
        NUM_OF_LARGE_JOBS_PER_CLASS);
    int[] monsterquery_largeJobs_numsOfReduces = config.getInts(
        "monsterQuery.largeJobs.numOfReduces", NUM_OF_REDUCERS_FOR_LARGE_JOB);

    len1 = nums_of_large_monsterquery_job.length;
    len2 = monsterquery_largeJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_large_monsterquery_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: monseterquery.largeJobs.numOfJobs, "
              + "monsterquery.largeJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_large_monsterquery_job = nums_of_large_monsterquery_job[index];
      int monsterquery_largeJobs_numOfReduces = monsterquery_largeJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_large_monsterquery_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_large_monsterquery_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_large_monsterquery_job_outputCompressed, currentIndex);

        addMonsterQueryLarge(monsterquery_largeJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }
  }

  private String addTSSuffix(String s) {
    Date date = Calendar.getInstance().getTime();
    String ts = String.valueOf(date.getTime());
    return s + ts;
  }

  private void addWebdataSort(int num_of_reducers, boolean mapoutputCompressed,
      boolean outputCompressed, String size) {
    String defaultIndir = VARCOMPSEQ + "/{part-00000,part-00001,part-00002}";
    String indir = getInputDirsFor("webdataSort.smallJobs.inputFiles",
        defaultIndir);

    String outdir = addTSSuffix("perf-out/webdata-sort-out-dir-small_");
    if ("medium".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARCOMPSEQ + "/{part-000*0,part-000*1,part-000*2}";
      indir = getInputDirsFor("webdataSort.mediumJobs.inputFiles", defaultIndir);
      outdir = addTSSuffix("perf-out/webdata-sort-out-dir-medium_");
    } else if ("large".compareToIgnoreCase(size) == 0) {
      defaultIndir = VARCOMPSEQ;
      indir = getInputDirsFor("webdataSort.largeJobs.inputFiles", defaultIndir);

      outdir = addTSSuffix("perf-out/webdata-sort-dir-large_");
    }
    GenericMRLoadJobCreator jobcreator = new GenericMRLoadJobCreator();
    StringBuffer sb = new StringBuffer();
    sb.append("-keepmap 100 ");
    sb.append("-keepred 100 ");
    sb.append("-inFormat org.apache.hadoop.mapred.SequenceFileInputFormat ");
    sb.append("-outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ");
    sb.append("-outKey org.apache.hadoop.io.Text ");
    sb.append("-outValue org.apache.hadoop.io.Text ");
    sb.append("-indir ").append(indir).append(" ");
    sb.append("-outdir ").append(outdir).append(" ");
    sb.append("-r ").append(num_of_reducers);

    String[] args = sb.toString().split(" ");
    clearDir(outdir);
    try {
      JobConf jobconf = jobcreator.createJob(args, mapoutputCompressed,
          outputCompressed);
      jobconf.setJobName("GridmixWebdataSort." + size);
      Job job = new Job(jobconf);
      gridmix.addJob(job);
      numOfJobs++;
    } catch (Exception ex) {
      System.out.println(ex.getStackTrace());
    }

  }

  private void addWebdataSortSmall(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {
    addWebdataSort(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "small");
  }

  private void addWebdataSortMedium(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {

    addWebdataSort(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "medium");
  }

  private void addWebdataSortLarge(int NUM_OF_REDUCERS,
      boolean mapoutputCompressed, boolean outputCompressed) {

    addWebdataSort(NUM_OF_REDUCERS, mapoutputCompressed, outputCompressed,
        "large");
  }

  private void addWebdataSortJobs() {
    int[] nums_of_small_webdatasort_job = config.getInts(
        "webdataSort.smallJobs.numOfJobs", NUM_OF_SMALL_JOBS_PER_CLASS);
    int num_of_small_webdatasort_job_mapoutputCompressed = config.getInt(
        "webdataSort.smallJobs.numOfMapoutputCompressed", 0);
    int num_of_small_webdatasort_job_outputCompressed = config.getInt(
        "webdataSort.smallJobs.numOfOutputCompressed",
        NUM_OF_SMALL_JOBS_PER_CLASS);
    int[] webdatasort_smallJobs_numsOfReduces = config.getInts(
        "webdataSort.smallJobs.numOfReduces", NUM_OF_REDUCERS_FOR_SMALL_JOB);

    int len1, len2, totalNum, currentIndex;

    len1 = nums_of_small_webdatasort_job.length;
    len2 = webdatasort_smallJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_small_webdatasort_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: webdatasort.smallJobs.numOfJobs, "
              + "webdatasort.smallJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_small_webdatasort_job = nums_of_small_webdatasort_job[index];
      int webdatasort_smallJobs_numOfReduces = webdatasort_smallJobs_numsOfReduces[index];

      for (int i = 0; i < num_of_small_webdatasort_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_small_webdatasort_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_small_webdatasort_job_outputCompressed, currentIndex);

        addWebdataSortSmall(webdatasort_smallJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_medium_webdatasort_job = config.getInts(
        "webdataSort.mediumJobs.numOfJobs", NUM_OF_MEDIUM_JOBS_PER_CLASS);
    int num_of_medium_webdatasort_job_mapoutputCompressed = config.getInt(
        "webdataSort.mediumJobs.numOfMapoutputCompressed", 0);
    int num_of_medium_webdatasort_job_outputCompressed = config.getInt(
        "webdataSort.mediumJobs.numOfOutputCompressed",
        NUM_OF_MEDIUM_JOBS_PER_CLASS);
    int[] webdatasort_mediumJobs_numsOfReduces = config.getInts(
        "webdataSort.mediumJobs.numOfReduces", NUM_OF_REDUCERS_FOR_MEDIUM_JOB);

    len1 = nums_of_medium_webdatasort_job.length;
    len2 = webdatasort_mediumJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_medium_webdatasort_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: webdatasort.mediumJobs.numOfJobs, "
              + "webdatasort.mediumJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_medium_webdatasort_job = nums_of_medium_webdatasort_job[index];
      int webdatasort_mediumJobs_numOfReduces = webdatasort_mediumJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_medium_webdatasort_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_medium_webdatasort_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_medium_webdatasort_job_outputCompressed, currentIndex);

        addWebdataSortMedium(webdatasort_mediumJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

    int[] nums_of_large_webdatasort_job = config.getInts(
        "webdataSort.largeJobs.numOfJobs", NUM_OF_LARGE_JOBS_PER_CLASS);
    int num_of_large_webdatasort_job_mapoutputCompressed = config.getInt(
        "webdataSort.largeJobs.numOfMapoutputCompressed", 0);
    int num_of_large_webdatasort_job_outputCompressed = config.getInt(
        "webdataSort.largeJobs.numOfOutputCompressed",
        NUM_OF_LARGE_JOBS_PER_CLASS);
    int[] webdatasort_largeJobs_numsOfReduces = config.getInts(
        "webdataSort.largeJobs.numOfReduces", NUM_OF_REDUCERS_FOR_LARGE_JOB);

    len1 = nums_of_large_webdatasort_job.length;
    len2 = webdatasort_largeJobs_numsOfReduces.length;
    totalNum = 0;

    for (int i = 0; i < len1; i++) {
      totalNum = totalNum + nums_of_large_webdatasort_job[i];
    }

    if (len1 != len2) {
      System.out
          .println(" Configuration error: webdatasort.largeJobs.numOfJobs, "
              + "webdatasort.largeJobs.numOfReduces must have the same number of items");
    }

    currentIndex = 0;
    for (int index = 0; index < len1; index++) {
      int num_of_large_webdatasort_job = nums_of_large_webdatasort_job[index];
      int webdatasort_largeJobs_numOfReduces = webdatasort_largeJobs_numsOfReduces[index];
      for (int i = 0; i < num_of_large_webdatasort_job; i++) {
        boolean mapoutputCompressed = select(totalNum,
            num_of_large_webdatasort_job_mapoutputCompressed, currentIndex);
        boolean outputCompressed = select(totalNum,
            num_of_large_webdatasort_job_outputCompressed, currentIndex);

        addWebdataSortLarge(webdatasort_largeJobs_numOfReduces,
            mapoutputCompressed, outputCompressed);
        currentIndex = currentIndex + 1;
      }
    }

  }

  public void addjobs() {

    addTextSortJobs();

    addCombinerJobs();

    addMonsterQueryJobs();

    addWebdataScanJobs();

    addWebdataSortJobs();

    System.out.println("total " + gridmix.getWaitingJobs().size() + " jobs");
  }

  class SimpleStats {
    long minValue;

    long maxValue;

    long averageValue;

    long mediumValue;

    int n;

    SimpleStats(long[] data) {
      Arrays.sort(data);
      n = data.length;
      minValue = data[0];
      maxValue = data[n - 1];
      mediumValue = data[n / 2];
      long total = 0;
      for (int i = 0; i < n; i++) {
        total += data[i];
      }
      averageValue = total / n;
    }
  }

  class TaskExecutionStats {
    TreeMap<String, SimpleStats> theStats;

    void computeStats(String name, long[] data) {
      SimpleStats v = new SimpleStats(data);
      theStats.put(name, v);
    }

    TaskExecutionStats() {
      theStats = new TreeMap<String, SimpleStats>();
    }
  }

  private TreeMap<String, String> getStatForJob(Job job) {
    TreeMap<String, String> retv = new TreeMap<String, String>();
    String mapreduceID = job.getAssignedJobID().toString();
    JobClient jc = job.getJobClient();
    JobConf jobconf = job.getJobConf();
    String jobName = jobconf.getJobName();
    retv.put("JobId", mapreduceID);
    retv.put("JobName", jobName);

    TaskExecutionStats theTaskExecutionStats = new TaskExecutionStats();

    try {
      RunningJob running = jc.getJob(JobID.forName(mapreduceID));
      Counters jobCounters = running.getCounters();
      Iterator<Group> groups = jobCounters.iterator();
      while (groups.hasNext()) {
        Group g = groups.next();
        String gn = g.getName();
        Iterator<Counters.Counter> cs = g.iterator();
        while (cs.hasNext()) {
          Counters.Counter c = cs.next();
          String n = c.getName();
          long v = c.getCounter();
          retv.put(mapreduceID + "." + jobName + "." + gn + "." + n, "" + v);
        }
      }
      TaskReport[] maps = jc.getMapTaskReports(JobID.forName(mapreduceID));
      TaskReport[] reduces = jc
          .getReduceTaskReports(JobID.forName(mapreduceID));
      retv.put(mapreduceID + "." + jobName + "." + "numOfMapTasks", ""
          + maps.length);
      retv.put(mapreduceID + "." + jobName + "." + "numOfReduceTasks", ""
          + reduces.length);
      long[] mapExecutionTimes = new long[maps.length];
      long[] reduceExecutionTimes = new long[reduces.length];
      Date date = Calendar.getInstance().getTime();
      long startTime = date.getTime();
      long finishTime = 0;
      for (int j = 0; j < maps.length; j++) {
        TaskReport map = maps[j];
        long thisStartTime = map.getStartTime();
        long thisFinishTime = map.getFinishTime();
        if (thisStartTime > 0 && thisFinishTime > 0) {
          mapExecutionTimes[j] = thisFinishTime - thisStartTime;
        }
        if (startTime > thisStartTime) {
          startTime = thisStartTime;
        }
        if (finishTime < thisFinishTime) {
          finishTime = thisFinishTime;
        }
      }

      theTaskExecutionStats.computeStats("mapExecutionTimeStats",
          mapExecutionTimes);

      retv.put(mapreduceID + "." + jobName + "." + "mapStartTime", ""
          + startTime);
      retv.put(mapreduceID + "." + jobName + "." + "mapEndTime", ""
          + finishTime);
      for (int j = 0; j < reduces.length; j++) {
        TaskReport reduce = reduces[j];
        long thisStartTime = reduce.getStartTime();
        long thisFinishTime = reduce.getFinishTime();
        if (thisStartTime > 0 && thisFinishTime > 0) {
          reduceExecutionTimes[j] = thisFinishTime - thisStartTime;
        }
        if (startTime > thisStartTime) {
          startTime = thisStartTime;
        }
        if (finishTime < thisFinishTime) {
          finishTime = thisFinishTime;
        }
      }

      theTaskExecutionStats.computeStats("reduceExecutionTimeStats",
          reduceExecutionTimes);

      retv.put(mapreduceID + "." + jobName + "." + "reduceStartTime", ""
          + startTime);
      retv.put(mapreduceID + "." + jobName + "." + "reduceEndTime", ""
          + finishTime);
      if (job.getState() == Job.SUCCESS) {
        retv.put(mapreduceID + "." + "jobStatus", "successful");
      } else if (job.getState() == Job.FAILED) {
        retv.put(mapreduceID + "." + jobName + "." + "jobStatus", "failed");
      } else {
        retv.put(mapreduceID + "." + jobName + "." + "jobStatus", "unknown");
      }
      Iterator<Entry<String, SimpleStats>> entries = theTaskExecutionStats.theStats
          .entrySet().iterator();
      while (entries.hasNext()) {
        Entry<String, SimpleStats> e = entries.next();
        SimpleStats v = e.getValue();
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "." + "min",
            "" + v.minValue);
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "." + "max",
            "" + v.maxValue);
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "."
            + "medium", "" + v.mediumValue);
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "." + "avg",
            "" + v.averageValue);
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "."
            + "numOfItems", "" + v.n);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return retv;
  }

  private void printJobStat(TreeMap<String, String> stat) {
    Iterator<Entry<String, String>> entries = stat.entrySet().iterator();
    while (entries.hasNext()) {
      Entry<String, String> e = entries.next();
      System.out.println(e.getKey() + "\t" + e.getValue());
    }
  }

  private void printStatsForJobs(ArrayList<Job> jobs) {
    for (int i = 0; i < jobs.size(); i++) {
      printJobStat(getStatForJob(jobs.get(i)));
    }
  }

  public void run() {

    Thread theGridmixRunner = new Thread(gridmix);
    theGridmixRunner.start();
    long startTime = System.currentTimeMillis();
    while (!gridmix.allFinished()) {
      System.out.println("Jobs in waiting state: "
          + gridmix.getWaitingJobs().size());
      System.out.println("Jobs in ready state: "
          + gridmix.getReadyJobs().size());
      System.out.println("Jobs in running state: "
          + gridmix.getRunningJobs().size());
      System.out.println("Jobs in success state: "
          + gridmix.getSuccessfulJobs().size());
      System.out.println("Jobs in failed state: "
          + gridmix.getFailedJobs().size());
      System.out.println("\n");

      try {
        Thread.sleep(10 * 1000);
      } catch (Exception e) {

      }
    }
    long endTime = System.currentTimeMillis();
    ArrayList<Job> fail = gridmix.getFailedJobs();
    ArrayList<Job> succeed = gridmix.getSuccessfulJobs();
    int numOfSuccessfulJob = succeed.size();
    if (numOfSuccessfulJob > 0) {
      System.out.println(numOfSuccessfulJob + " jobs succeeded");
      printStatsForJobs(succeed);

    }
    int numOfFailedjob = fail.size();
    if (numOfFailedjob > 0) {
      System.out.println("------------------------------- ");
      System.out.println(numOfFailedjob + " jobs failed");
      printStatsForJobs(fail);
    }
    System.out.println("GridMix results:");
    System.out.println("Total num of Jobs: " + numOfJobs);
    System.out.println("ExecutionTime: " + ((endTime-startTime)/1000));
    gridmix.stop();
  }

  public static void main(String argv[]) {

    try {
      GridMixRunner gridmixRunner = new GridMixRunner();
      gridmixRunner.addjobs();
      gridmixRunner.run();
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
    }
  }

}
