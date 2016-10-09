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

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.tools.rumen.datatypes.*;
import org.apache.hadoop.tools.rumen.serializers.*;
import org.apache.hadoop.tools.rumen.state.*;

public class Anonymizer extends Configured implements Tool {
  private boolean anonymizeTrace = false;
  private Path inputTracePath = null;
  private Path outputTracePath = null;
  private boolean anonymizeTopology = false;
  private Path inputTopologyPath = null;
  private Path outputTopologyPath = null;
  
  //TODO Make this final if not using JSON
  // private final StatePool statePool = new StatePool();
  private StatePool statePool;
  
  private ObjectMapper outMapper = null;
  private JsonFactory outFactory = null;
  
  private void initialize(String[] args) throws Exception {
    try {
      for (int i = 0; i < args.length; ++i) {
        if ("-trace".equals(args[i])) {
          anonymizeTrace = true;
          inputTracePath = new Path(args[i+1]);
          outputTracePath = new Path(args[i+2]);
          i +=2;
        }
        if ("-topology".equals(args[i])) {
          anonymizeTopology = true;
          inputTopologyPath = new Path(args[i+1]);
          outputTopologyPath = new Path(args[i+2]);
          i +=2;
        }
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Illegal arguments list!", e);
    }
    
    if (!anonymizeTopology && !anonymizeTrace) {
      throw new IllegalArgumentException("Invalid arguments list!");
    }
    
    statePool = new StatePool();
    // initialize the state manager after the anonymizers are registered
    statePool.initialize(getConf());
     
    outMapper = new ObjectMapper();
    // define a module
    SimpleModule module = new SimpleModule(
        "Anonymization Serializer", new Version(0, 1, 1, "FINAL", "", ""));
    // add various serializers to the module
    // use the default (as-is) serializer for default data types
    module.addSerializer(DataType.class, new DefaultRumenSerializer());
    // use a blocking serializer for Strings as they can contain sensitive 
    // information
    module.addSerializer(String.class, new BlockingSerializer());
    // use object.toString() for object of type ID
    module.addSerializer(ID.class, new ObjectStringSerializer<ID>());
    // use getAnonymizedValue() for data types that have the anonymizing 
    // feature
    module.addSerializer(AnonymizableDataType.class, 
        new DefaultAnonymizingRumenSerializer(statePool, getConf()));
    
    // register the module with the object-mapper
    outMapper.registerModule(module);
    
    outFactory = outMapper.getFactory();
  }
  
  // anonymize the job trace file
  private void anonymizeTrace() throws Exception {
    if (anonymizeTrace) {
      System.out.println("Anonymizing trace file: " + inputTracePath);
      JobTraceReader reader = null;
      JsonGenerator outGen = null;
      Configuration conf = getConf();
      
      try {
        // create a generator
        outGen = createJsonGenerator(conf, outputTracePath);

        // define the input trace reader
        reader = new JobTraceReader(inputTracePath, conf);
        
        // read the plain unanonymized logged job
        LoggedJob job = reader.getNext();
        
        while (job != null) {
          // write it via an anonymizing channel
          outGen.writeObject(job);
          // read the next job
          job = reader.getNext();
        }
        
        System.out.println("Anonymized trace file: " + outputTracePath);
      } finally {
        if (outGen != null) {
          outGen.close();
        }
        if (reader != null) {
          reader.close();
        }
      }
    }
  }

  // anonymize the cluster topology file
  private void anonymizeTopology() throws Exception {
    if (anonymizeTopology) {
      System.out.println("Anonymizing topology file: " + inputTopologyPath);
      ClusterTopologyReader reader = null;
      JsonGenerator outGen = null;
      Configuration conf = getConf();

      try {
        // create a generator
        outGen = createJsonGenerator(conf, outputTopologyPath);

        // define the input cluster topology reader
        reader = new ClusterTopologyReader(inputTopologyPath, conf);
        
        // read the plain unanonymized logged job
        LoggedNetworkTopology job = reader.get();
        
        // write it via an anonymizing channel
        outGen.writeObject(job);
        
        System.out.println("Anonymized topology file: " + outputTopologyPath);
      } finally {
        if (outGen != null) {
          outGen.close();
        }
      }
    }
  }
  
  // Creates a JSON generator
  private JsonGenerator createJsonGenerator(Configuration conf, Path path) 
  throws IOException {
    FileSystem outFS = path.getFileSystem(conf);
    CompressionCodec codec =
      new CompressionCodecFactory(conf).getCodec(path);
    OutputStream output;
    Compressor compressor = null;
    if (codec != null) {
      compressor = CodecPool.getCompressor(codec);
      output = codec.createOutputStream(outFS.create(path), compressor);
    } else {
      output = outFS.create(path);
    }

    JsonGenerator outGen =
        outFactory.createGenerator(output, JsonEncoding.UTF8);
    outGen.useDefaultPrettyPrinter();
    
    return outGen;
  }
  
  @Override
  public int run(String[] args) throws Exception {
    try {
      initialize(args);
    } catch (Exception e) {
      e.printStackTrace();
      printUsage();
      return -1;
    }
    
    return run();
  }

  /**
   * Runs the actual anonymization tool.
   */
  public int run() throws Exception {
    try {
      anonymizeTrace();
    } catch (IOException ioe) {
      System.err.println("Error running the trace anonymizer!");
      ioe.printStackTrace();
      System.out.println("\n\nAnonymization unsuccessful!");
      return -1;
    }
    
    try {
      anonymizeTopology();
    } catch (IOException ioe) {
      System.err.println("Error running the cluster topology anonymizer!");
      ioe.printStackTrace();
      System.out.println("\n\nAnonymization unsuccessful!");
      return -1;
    }
    
    statePool.persist();
    
    System.out.println("Anonymization completed successfully!");
    
    return 0;
  }

  private static void printUsage() {
    System.out.println("\nUsage:-");
    System.out.print("  Anonymizer");
    System.out.print(" [-trace <input-trace-path> <output-trace-path>]");
    System.out.println(" [-topology <input-topology-path> " 
                       + "<output-topology-path>] ");
    System.out.print("\n");
  }
  
  /**
   * The main driver program to use the anonymization utility.
   * @param args
   */
  public static void main(String[] args) {
    Anonymizer instance = new Anonymizer();
    int result = 0;
    
    try {
      result = ToolRunner.run(instance, args);
    } catch (Exception e) {
      e.printStackTrace(System.err);
      System.exit(-1);
    }

    if (result != 0) {
      System.exit(result);
    }

    return;
  }
}