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
package org.apache.hadoop.tools.rumen;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main driver of the Rumen Parser.
 */
public class TraceBuilder extends Configured implements Tool {
  static final private Log LOG = LogFactory.getLog(TraceBuilder.class);

  static final int RUN_METHOD_FAILED_EXIT_CODE = 3;

  TopologyBuilder topologyBuilder = new TopologyBuilder();
  JobConfigurationParser jobConfParser;
  Outputter<LoggedJob> traceWriter;
  Outputter<LoggedNetworkTopology> topologyWriter;

  static class MyOptions {
    Class<? extends InputDemuxer> inputDemuxerClass = DefaultInputDemuxer.class;

    @SuppressWarnings("unchecked")
    Class<? extends Outputter> clazzTraceOutputter = DefaultOutputter.class;
    Path traceOutput;
    Path topologyOutput;

    List<Path> inputs = new LinkedList<Path>();

    MyOptions(String[] args, Configuration conf) throws FileNotFoundException,
        IOException, ClassNotFoundException {
      int switchTop = 0;

      while (args[switchTop].startsWith("-")) {
        if (args[switchTop].equalsIgnoreCase("-demuxer")) {
          inputDemuxerClass =
              Class.forName(args[++switchTop]).asSubclass(InputDemuxer.class);

          ++switchTop;
        }
      }

      traceOutput = new Path(args[0 + switchTop]);
      topologyOutput = new Path(args[1 + switchTop]);

      for (int i = 2 + switchTop; i < args.length; ++i) {

        Path thisPath = new Path(args[i]);

        FileSystem fs = thisPath.getFileSystem(conf);
        if (fs.getFileStatus(thisPath).isDir()) {
          FileStatus[] statuses = fs.listStatus(thisPath);

          List<String> dirNames = new ArrayList<String>();

          for (FileStatus s : statuses) {
            if (s.isDir()) continue;
            String name = s.getPath().getName();

            if (!(name.endsWith(".crc") || name.startsWith("."))) {
              dirNames.add(name);
            }
          }

          String[] sortableNames = dirNames.toArray(new String[1]);

          Arrays.sort(sortableNames);

          for (String dirName : sortableNames) {
            inputs.add(new Path(thisPath, dirName));
          }
        } else {
          inputs.add(thisPath);
        }
      }
    }
  }

  public static void main(String[] args) {
    TraceBuilder builder = new TraceBuilder();
    int result = RUN_METHOD_FAILED_EXIT_CODE;

    try {
      result = ToolRunner.run(builder, args); 
    } catch (Throwable t) {
      t.printStackTrace(System.err);
    } finally {
      try {
        builder.finish();
      } finally {
        if (result == 0) {
          return;
        }

        System.exit(result);
      }
    }
  }

  private static String applyParser(String fileName, Pattern pattern) {
    Matcher matcher = pattern.matcher(fileName);

    if (!matcher.matches()) {
      return null;
    }

    return matcher.group(1);
  }

  /**
   * @param fileName
   * @return the jobID String, parsed out of the file name. We return a valid
   *         String for either a history log file or a config file. Otherwise,
   *         [especially for .crc files] we return null.
   */
  static String extractJobID(String fileName) {
    String jobId = applyParser(fileName, JobHistory.JOBHISTORY_FILENAME_REGEX);
    if (jobId == null) {
      // check if its a pre21 jobhistory file
      jobId = applyParser(fileName, 
                          Pre21JobHistoryConstants.JOBHISTORY_FILENAME_REGEX);
    }
    return jobId;
  }

  static boolean isJobConfXml(String fileName, InputStream input) {
    String jobId = applyParser(fileName, JobHistory.CONF_FILENAME_REGEX);
    if (jobId == null) {
      // check if its a pre21 jobhistory conf file
      jobId = applyParser(fileName, 
                          Pre21JobHistoryConstants.CONF_FILENAME_REGEX);
    }
    return jobId != null;
  }

  private void addInterestedProperties(List<String> interestedProperties,
      String[] names) {
    for (String name : names) {
      interestedProperties.add(name);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public int run(String[] args) throws Exception {
    MyOptions options = new MyOptions(args, getConf());
    List<String> interestedProperties = new ArrayList<String>();
    {
      for (JobConfPropertyNames candidateSet : JobConfPropertyNames.values()) {
        addInterestedProperties(interestedProperties, candidateSet
            .getCandidates());
      }
    }
    jobConfParser = new JobConfigurationParser(interestedProperties);
    traceWriter = options.clazzTraceOutputter.newInstance();
    traceWriter.init(options.traceOutput, getConf());
    topologyWriter = new DefaultOutputter<LoggedNetworkTopology>();
    topologyWriter.init(options.topologyOutput, getConf());

    try {
      JobBuilder jobBuilder = null;

      for (Path p : options.inputs) {
        InputDemuxer inputDemuxer = options.inputDemuxerClass.newInstance();

        try {
          inputDemuxer.bindTo(p, getConf());
        } catch (IOException e) {
          LOG.warn("Unable to bind Path " + p + " .  Skipping...", e);

          continue;
        }

        Pair<String, InputStream> filePair = null;

        try {
          while ((filePair = inputDemuxer.getNext()) != null) {
            RewindableInputStream ris =
                new RewindableInputStream(filePair.second());

            JobHistoryParser parser = null;

            try {
              String jobID = extractJobID(filePair.first());
              if (jobID == null) {
                LOG.warn("File skipped: Invalid file name: "
                    + filePair.first());
                continue;
              }
              if ((jobBuilder == null)
                  || (!jobBuilder.getJobID().equals(jobID))) {
                if (jobBuilder != null) {
                  traceWriter.output(jobBuilder.build());
                }
                jobBuilder = new JobBuilder(jobID);
              }

              if (isJobConfXml(filePair.first(), ris)) {
                processJobConf(jobConfParser.parse(ris.rewind()), jobBuilder);
              } else {
                parser = JobHistoryParserFactory.getParser(ris);
                if (parser == null) {
                  LOG.warn("File skipped: Cannot find suitable parser: "
                      + filePair.first());
                } else {
                  processJobHistory(parser, jobBuilder);
                }
              }
            } finally {
              if (parser == null) {
                ris.close();
              } else {
                parser.close();
                parser = null;
              }
            }
          }
        } catch (Throwable t) {
          if (filePair != null) {
            LOG.warn("TraceBuilder got an error while processing the [possibly virtual] file "
                + filePair.first() + " within Path " + p , t);
          }
        } finally {
          inputDemuxer.close();
        }
      }
      if (jobBuilder != null) {
        traceWriter.output(jobBuilder.build());
        jobBuilder = null;
      } else {
        LOG.warn("No job found in traces: ");
      }

      topologyWriter.output(topologyBuilder.build());
    } finally {
      traceWriter.close();
      topologyWriter.close();
    }

    return 0;
  }

  private void processJobConf(Properties properties, JobBuilder jobBuilder) {
    jobBuilder.process(properties);
    topologyBuilder.process(properties);
  }

  void processJobHistory(JobHistoryParser parser, JobBuilder jobBuilder)
      throws IOException {
    HistoryEvent e;
    while ((e = parser.nextEvent()) != null) {
      jobBuilder.process(e);
      topologyBuilder.process(e);
    }

    parser.close();
  }

  void finish() {
    IOUtils.cleanup(LOG, traceWriter, topologyWriter);
  }
}
