/*
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

/** Rumen is a data extraction and analysis tool built for 
 * <a href="http://hadoop.apache.org/">Apache Hadoop</a>. Rumen mines job history
 * logs to extract meaningful data and stores it into an easily-parsed format.
 * 
 * The default output format of Rumen is <a href="http://www.json.org">JSON</a>.
 * Rumen uses the <a href="http://jackson.codehaus.org/">Jackson</a> library to 
 * create JSON objects.
 * <br><br>
 * 
 * The following classes can be used to programmatically invoke Rumen:
 * <ol>
 *   <li>
 *    {@link org.apache.hadoop.tools.rumen.JobConfigurationParser}<br>
 *      A parser to parse and filter out interesting properties from job 
 *      configuration.
 *      
 *      <br><br>
 *      <i>Sample code</i>:
 *      <pre>
 *      <code>
 *        // An example to parse and filter out job name
 *        
 *        String conf_filename = .. // assume the job configuration filename here
 *        
 *        // construct a list of interesting properties
 *        List&lt;String&gt; interestedProperties = new ArrayList&lt;String&gt;();
 *        interestedProperties.add("mapreduce.job.name");
 *        
 *        JobConfigurationParser jcp = 
 *          new JobConfigurationParser(interestedProperties);
 *
 *        InputStream in = new FileInputStream(conf_filename);
 *        Properties parsedProperties = jcp.parse(in);
 *     </code>
 *     </pre>
 *     Some of the commonly used interesting properties are enumerated in 
 *     {@link org.apache.hadoop.tools.rumen.JobConfPropertyNames}. <br><br>
 *     
 *     <b>Note:</b>
 *        A single instance of {@link org.apache.hadoop.tools.rumen.JobConfigurationParser} 
 *        can be used to parse multiple job configuration files. 
 *     
 *   </li>
 *   <li>
 *    {@link org.apache.hadoop.tools.rumen.JobHistoryParser} <br>
 *      A parser that parses job history files. It is an interface and actual 
 *      implementations are defined as Enum in 
 *      {@link org.apache.hadoop.tools.rumen.JobHistoryParserFactory}. Note that
 *      {@link org.apache.hadoop.tools.rumen.RewindableInputStream}<br>
 *      is a wrapper class around {@link java.io.InputStream} to make the input 
 *      stream rewindable.
 *      
 *      <br>
 *      <i>Sample code</i>:
 *      <pre>
 *      <code>
 *        // An example to parse a current job history file i.e a job history 
 *        // file for which the version is known
 *        
 *        String filename = .. // assume the job history filename here
 *        
 *        InputStream in = new FileInputStream(filename);
 *        
 *        HistoryEvent event = null;
 *        
 *        JobHistoryParser parser = new CurrentJHParser(in);
 *        
 *        event = parser.nextEvent();
 *        // process all the events
 *        while (event != null) {
 *          // ... process all event
 *          event = parser.nextEvent();
 *        }
 *        
 *        // close the parser and the underlying stream
 *        parser.close();
 *      </code>
 *      </pre>
 *      
 *      {@link org.apache.hadoop.tools.rumen.JobHistoryParserFactory} provides a 
 *      {@link org.apache.hadoop.tools.rumen.JobHistoryParserFactory#getParser(org.apache.hadoop.tools.rumen.RewindableInputStream)}
 *      API to get a parser for parsing the job history file. Note that this
 *      API can be used if the job history version is unknown.<br><br>
 *      <i>Sample code</i>:
 *      <pre>
 *      <code>
 *        // An example to parse a job history for which the version is not 
 *        // known i.e using JobHistoryParserFactory.getParser()
 *        
 *        String filename = .. // assume the job history filename here
 *        
 *        InputStream in = new FileInputStream(filename);
 *        RewindableInputStream ris = new RewindableInputStream(in);
 *        
 *        // JobHistoryParserFactory will check and return a parser that can
 *        // parse the file
 *        JobHistoryParser parser = JobHistoryParserFactory.getParser(ris);
 *        
 *        // now use the parser to parse the events
 *        HistoryEvent event = parser.nextEvent();
 *        while (event != null) {
 *          // ... process the event
 *          event = parser.nextEvent();
 *        }
 *        
 *        parser.close();
 *      </code>
 *      </pre>
 *      <b>Note:</b>
 *        Create one instance to parse a job history log and close it after use.
 *  </li>
 *  <li>
 *    {@link org.apache.hadoop.tools.rumen.TopologyBuilder}<br>
 *      Builds the cluster topology based on the job history events. Every 
 *      job history file consists of events. Each event can be represented using
 *      {@link org.apache.hadoop.mapreduce.jobhistory.HistoryEvent}. 
 *      These events can be passed to {@link org.apache.hadoop.tools.rumen.TopologyBuilder} using 
 *      {@link org.apache.hadoop.tools.rumen.TopologyBuilder#process(org.apache.hadoop.mapreduce.jobhistory.HistoryEvent)}.
 *      A cluster topology can be represented using {@link org.apache.hadoop.tools.rumen.LoggedNetworkTopology}.
 *      Once all the job history events are processed, the cluster 
 *      topology can be obtained using {@link org.apache.hadoop.tools.rumen.TopologyBuilder#build()}.
 *      
 *      <br><br>
 *      <i>Sample code</i>:
 *      <pre>
 *      <code>
 *        // Building topology for a job history file represented using 
 *        // 'filename' and the corresponding configuration file represented 
 *        // using 'conf_filename'
 *        String filename = .. // assume the job history filename here
 *        String conf_filename = .. // assume the job configuration filename here
 *        
 *        InputStream jobConfInputStream = new FileInputStream(filename);
 *        InputStream jobHistoryInputStream = new FileInputStream(conf_filename);
 *        
 *        TopologyBuilder tb = new TopologyBuilder();
 *        
 *        // construct a list of interesting properties
 *        List&lt;String&gt; interestingProperties = new ArrayList%lt;String&gt;();
 *        // add the interesting properties here
 *        interestingProperties.add("mapreduce.job.name");
 *        
 *        JobConfigurationParser jcp = 
 *          new JobConfigurationParser(interestingProperties);
 *        
 *        // parse the configuration file
 *        tb.process(jcp.parse(jobConfInputStream));
 *        
 *        // read the job history file and pass it to the 
 *        // TopologyBuilder.
 *        JobHistoryParser parser = new CurrentJHParser(jobHistoryInputStream);
 *        HistoryEvent e;
 *        
 *        // read and process all the job history events
 *        while ((e = parser.nextEvent()) != null) {
 *          tb.process(e);
 *        }
 *        
 *        LoggedNetworkTopology topology = tb.build();
 *      </code>
 *      </pre>
 *  </li>
 *  <li>
 *    {@link org.apache.hadoop.tools.rumen.JobBuilder}<br>
 *      Summarizes a job history file.
 *      {@link org.apache.hadoop.tools.rumen.JobHistoryUtils} provides  
 *      {@link org.apache.hadoop.tools.rumen.JobHistoryUtils#extractJobID(String)} 
 *      API for extracting job id from job history or job configuration files
 *      which can be used for instantiating {@link org.apache.hadoop.tools.rumen.JobBuilder}. 
 *      {@link org.apache.hadoop.tools.rumen.JobBuilder} generates a 
 *      {@link org.apache.hadoop.tools.rumen.LoggedJob} object via 
 *      {@link org.apache.hadoop.tools.rumen.JobBuilder#build()}. 
 *      See {@link org.apache.hadoop.tools.rumen.LoggedJob} for more details.
 *      
 *      <br><br>
 *      <i>Sample code</i>:
 *      <pre>
 *      <code>
 *        // An example to summarize a current job history file 'filename'
 *        // and the corresponding configuration file 'conf_filename'
 *        
 *        String filename = .. // assume the job history filename here
 *        String conf_filename = .. // assume the job configuration filename here
 *        
 *        InputStream jobConfInputStream = new FileInputStream(job_filename);
 *        InputStream jobHistoryInputStream = new FileInputStream(conf_filename);
 *        
 *        String jobID = TraceBuilder.extractJobID(job_filename);
 *        JobBuilder jb = new JobBuilder(jobID);
 *        
 *        // construct a list of interesting properties
 *        List&lt;String&gt; interestingProperties = new ArrayList%lt;String&gt;();
 *        // add the interesting properties here
 *        interestingProperties.add("mapreduce.job.name");
 *        
 *        JobConfigurationParser jcp = 
 *          new JobConfigurationParser(interestingProperties);
 *        
 *        // parse the configuration file
 *        jb.process(jcp.parse(jobConfInputStream));
 *        
 *        // parse the job history file
 *        JobHistoryParser parser = new CurrentJHParser(jobHistoryInputStream);
 *        try {
 *          HistoryEvent e;
 *          // read and process all the job history events
 *          while ((e = parser.nextEvent()) != null) {
 *            jobBuilder.process(e);
 *          }
 *        } finally {
 *          parser.close();
 *        }
 *        
 *        LoggedJob job = jb.build();
 *      </code>
 *      </pre>
 *     <b>Note:</b>
 *       The order of parsing the job configuration file or job history file is 
 *       not important. Create one instance to parse the history file and job 
 *       configuration.
 *   </li>
 *   <li>
 *    {@link org.apache.hadoop.tools.rumen.DefaultOutputter}<br>
 *      Implements {@link org.apache.hadoop.tools.rumen.Outputter} and writes 
 *      JSON object in text format to the output file. 
 *      {@link org.apache.hadoop.tools.rumen.DefaultOutputter} can be 
 *      initialized with the output filename.
 *      
 *      <br><br>
 *      <i>Sample code</i>:  
 *      <pre>
 *      <code>
 *        // An example to summarize a current job history file represented by
 *        // 'filename' and the configuration filename represented using 
 *        // 'conf_filename'. Also output the job summary to 'out.json' along 
 *        // with the cluster topology to 'topology.json'.
 *        
 *        String filename = .. // assume the job history filename here
 *        String conf_filename = .. // assume the job configuration filename here
 *        
 *        Configuration conf = new Configuration();
 *        DefaultOutputter do = new DefaultOutputter();
 *        do.init("out.json", conf);
 *        
 *        InputStream jobConfInputStream = new FileInputStream(filename);
 *        InputStream jobHistoryInputStream = new FileInputStream(conf_filename);
 *        
 *        // extract the job-id from the filename
 *        String jobID = TraceBuilder.extractJobID(filename);
 *        JobBuilder jb = new JobBuilder(jobID);
 *        TopologyBuilder tb = new TopologyBuilder();
 *        
 *        // construct a list of interesting properties
 *        List&lt;String&gt; interestingProperties = new ArrayList%lt;String&gt;();
 *        // add the interesting properties here
 *        interestingProperties.add("mapreduce.job.name");
 *        
 *        JobConfigurationParser jcp =
 *          new JobConfigurationParser(interestingProperties);
 *          
 *        // parse the configuration file
 *        tb.process(jcp.parse(jobConfInputStream));
 *        
 *        // read the job history file and pass it to the
 *        // TopologyBuilder.
 *        JobHistoryParser parser = new CurrentJHParser(jobHistoryInputStream);
 *        HistoryEvent e;
 *        while ((e = parser.nextEvent()) != null) {
 *          jb.process(e);
 *          tb.process(e);
 *        }
 *        
 *        LoggedJob j = jb.build();
 *        
 *        // serialize the job summary in json (text) format
 *        do.output(j);
 *        
 *        // close
 *        do.close();
 *        
 *        do.init("topology.json", conf);
 *        
 *        // get the job summary using TopologyBuilder
 *        LoggedNetworkTopology topology = topologyBuilder.build();
 *        
 *        // serialize the cluster topology in json (text) format
 *        do.output(topology);
 *        
 *        // close
 *        do.close();
 *      </code>
 *      </pre>
 *   </li>
 *   <li>
 *    {@link org.apache.hadoop.tools.rumen.JobTraceReader}<br>
 *      A reader for reading {@link org.apache.hadoop.tools.rumen.LoggedJob} serialized using 
 *      {@link org.apache.hadoop.tools.rumen.DefaultOutputter}. {@link org.apache.hadoop.tools.rumen.LoggedJob} 
 *      provides various APIs for extracting job details. Following are the most
 *      commonly used ones
 *        <ul>
 *          <li>{@link org.apache.hadoop.tools.rumen.LoggedJob#getMapTasks()} : Get the map tasks</li>
 *          <li>{@link org.apache.hadoop.tools.rumen.LoggedJob#getReduceTasks()} : Get the reduce tasks</li>
 *          <li>{@link org.apache.hadoop.tools.rumen.LoggedJob#getOtherTasks()} : Get the setup/cleanup tasks</li>
 *          <li>{@link org.apache.hadoop.tools.rumen.LoggedJob#getOutcome()} : Get the job's outcome</li>
 *          <li>{@link org.apache.hadoop.tools.rumen.LoggedJob#getSubmitTime()} : Get the job's submit time</li>
 *          <li>{@link org.apache.hadoop.tools.rumen.LoggedJob#getFinishTime()} : Get the job's finish time</li>
 *        </ul>
 *        
 *      <br><br>
 *      <i>Sample code</i>:
 *      <pre>
 *      <code>
 *        // An example to read job summary from a trace file 'out.json'.
 *        JobTraceReader reader = new JobTracerReader("out.json");
 *        LoggedJob job = reader.getNext();
 *        while (job != null) {
 *          // .... process job level information
 *          for (LoggedTask task : job.getMapTasks()) {
 *            // process all the map tasks in the job
 *            for (LoggedTaskAttempt attempt : task.getAttempts()) {
 *              // process all the map task attempts in the job
 *            }
 *          }
 *          
 *          // get the next job
 *          job = reader.getNext();
 *        }
 *        reader.close();
 *      </code>
 *      </pre>         
 *   </li>
 *   <li>
 *    {@link org.apache.hadoop.tools.rumen.ClusterTopologyReader}<br>
 *      A reader to read {@link org.apache.hadoop.tools.rumen.LoggedNetworkTopology} serialized using 
 *      {@link org.apache.hadoop.tools.rumen.DefaultOutputter}. {@link org.apache.hadoop.tools.rumen.ClusterTopologyReader} can be 
 *      initialized using the serialized topology filename. 
 *      {@link org.apache.hadoop.tools.rumen.ClusterTopologyReader#get()} can
 *      be used to get the 
 *      {@link org.apache.hadoop.tools.rumen.LoggedNetworkTopology}. 
 *      
 *      <br><br>
 *      <i>Sample code</i>:
 *      <pre>
 *      <code>
 *        // An example to read the cluster topology from a topology output file
 *        // 'topology.json'
 *        ClusterTopologyReader reader = new ClusterTopologyReader("topology.json");
 *        LoggedNetworkTopology topology  = reader.get();
 *        for (LoggedNetworkTopology t : topology.getChildren()) {
 *          // process the cluster topology
 *        }
 *        reader.close();
 *      </code>
 *      </pre>
 *   </li>
 * </ol>     
 */

package org.apache.hadoop.tools.rumen;