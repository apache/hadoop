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
package org.apache.hadoop.yarn.sls;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;

@Private
@Unstable
public class RumenToSLSConverter {
  private static final String EOL = System.getProperty("line.separator");

  private static long baseline = 0;
  private static Map<String, Set<String>> rackNodeMap =
          new TreeMap<String, Set<String>>();

  public static void main(String args[]) throws Exception {
    Options options = new Options();
    options.addOption("input", true, "input rumen json file");
    options.addOption("outputJobs", true, "output jobs file");
    options.addOption("outputNodes", true, "output nodes file");

    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);

    if (! cmd.hasOption("input") ||
            ! cmd.hasOption("outputJobs") ||
            ! cmd.hasOption("outputNodes")) {
      System.err.println();
      System.err.println("ERROR: Missing input or output file");
      System.err.println();
      System.err.println("LoadGenerator creates a SLS script " +
              "from a Hadoop Rumen output");
      System.err.println();
      System.err.println("Options: -input FILE -outputJobs FILE " +
              "-outputNodes FILE");
      System.err.println();
      System.exit(1);
    }

    String inputFile = cmd.getOptionValue("input");
    String outputJsonFile = cmd.getOptionValue("outputJobs");
    String outputNodeFile = cmd.getOptionValue("outputNodes");

    // check existing
    if (! new File(inputFile).exists()) {
      System.err.println();
      System.err.println("ERROR: input does not exist");
      System.exit(1);
    }
    if (new File(outputJsonFile).exists()) {
      System.err.println();
      System.err.println("ERROR: output job file is existing");
      System.exit(1);
    }
    if (new File(outputNodeFile).exists()) {
      System.err.println();
      System.err.println("ERROR: output node file is existing");
      System.exit(1);
    }

    File jsonFile = new File(outputJsonFile);
    if (! jsonFile.getParentFile().exists()
            && ! jsonFile.getParentFile().mkdirs()) {
      System.err.println("ERROR: Cannot create output directory in path: "
              + jsonFile.getParentFile().getAbsoluteFile());
      System.exit(1);
    }
    File nodeFile = new File(outputNodeFile);
    if (! nodeFile.getParentFile().exists()
            && ! nodeFile.getParentFile().mkdirs()) {
      System.err.println("ERROR: Cannot create output directory in path: "
              + nodeFile.getParentFile().getAbsoluteFile());
      System.exit(1);
    }

    generateSLSLoadFile(inputFile, outputJsonFile);
    generateSLSNodeFile(outputNodeFile);
  }

  private static void generateSLSLoadFile(String inputFile, String outputFile)
          throws IOException {
    try (Reader input =
        new InputStreamReader(new FileInputStream(inputFile), "UTF-8")) {
      try (Writer output =
          new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8")) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        Iterator<Map> i = mapper.readValues(
            new JsonFactory().createParser(input), Map.class);
        while (i.hasNext()) {
          Map m = i.next();
          output.write(writer.writeValueAsString(createSLSJob(m)) + EOL);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void generateSLSNodeFile(String outputFile)
          throws IOException {
    try (Writer output =
        new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8")) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
      for (Map.Entry<String, Set<String>> entry : rackNodeMap.entrySet()) {
        Map rack = new LinkedHashMap();
        rack.put("rack", entry.getKey());
        List nodes = new ArrayList();
        for (String name : entry.getValue()) {
          Map node = new LinkedHashMap();
          node.put("node", name);
          nodes.add(node);
        }
        rack.put("nodes", nodes);
        output.write(writer.writeValueAsString(rack) + EOL);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Map createSLSJob(Map rumenJob) {
    Map json = new LinkedHashMap();
    long jobStart = (Long) rumenJob.get("submitTime");
    long jobFinish = (Long) rumenJob.get("finishTime");
    String jobId = rumenJob.get("jobID").toString();
    String queue = rumenJob.get("queue").toString();
    String user = rumenJob.get("user").toString();
    if (baseline == 0) {
      baseline = jobStart;
    }
    jobStart -= baseline;
    jobFinish -= baseline;
    long offset = 0;
    if (jobStart < 0) {
      System.out.println("Warning: reset job " + jobId + " start time to 0.");
      offset = -jobStart;
      jobFinish = jobFinish - jobStart;
      jobStart = 0;
    }

    json.put("am.type", "mapreduce");
    json.put("job.start.ms", jobStart);
    json.put("job.end.ms", jobFinish);
    json.put("job.queue.name", queue);
    json.put("job.id", jobId);
    json.put("job.user", user);

    List maps = createSLSTasks("map",
            (List) rumenJob.get("mapTasks"), offset);
    List reduces = createSLSTasks("reduce",
            (List) rumenJob.get("reduceTasks"), offset);
    List tasks = new ArrayList();
    tasks.addAll(maps);
    tasks.addAll(reduces);
    json.put("job.tasks", tasks);
    return json;
  }

  @SuppressWarnings("unchecked")
  private static List createSLSTasks(String taskType,
                                     List rumenTasks, long offset) {
    int priority = taskType.equals("reduce") ? 10 : 20;
    List array = new ArrayList();
    for (Object e : rumenTasks) {
      Map rumenTask = (Map) e;
      for (Object ee : (List) rumenTask.get("attempts"))  {
        Map rumenAttempt = (Map) ee;
        long taskStart = (Long) rumenAttempt.get("startTime");
        long taskFinish = (Long) rumenAttempt.get("finishTime");
        String hostname = (String) rumenAttempt.get("hostName");
        taskStart = taskStart - baseline + offset;
        taskFinish = taskFinish - baseline + offset;
        Map task = new LinkedHashMap();
        task.put("container.host", hostname);
        task.put("container.start.ms", taskStart);
        task.put("container.end.ms", taskFinish);
        task.put("container.priority", priority);
        task.put("container.type", taskType);
        array.add(task);
        String rackHost[] = SLSUtils.getRackHostName(hostname);
        if (rackNodeMap.containsKey(rackHost[0])) {
          rackNodeMap.get(rackHost[0]).add(rackHost[1]);
        } else {
          Set<String> hosts = new TreeSet<String>();
          hosts.add(rackHost[1]);
          rackNodeMap.put(rackHost[0], hosts);
        }
      }
    }
    return array;
  }
}
