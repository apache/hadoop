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

package org.apache.hadoop.hive.ql.exec;

import java.util.*;
import java.io.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


/**
 * Map operator. This triggers overall map side processing.
 * This is a little different from regular operators in that
 * it starts off by processing a Writable data structure from
 * a Table (instead of a Hive Object).
 **/
public class MapOperator extends Operator <mapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  public static enum Counter {DESERIALIZE_ERRORS}
  transient private LongWritable deserialize_error_count = new LongWritable ();
  transient private Deserializer deserializer;
  
  transient private Object row;
  transient private Object[] rowWithPart;
  transient private StructObjectInspector rowObjectInspector;

  transient private List<String> partNames;
  transient private List<String> partValues;
  transient private List<ObjectInspector> partObjectInspectors;
  

  public void initialize(Configuration hconf) throws HiveException {
    super.initialize(hconf);
    Path fpath = new Path((new Path (HiveConf.getVar(hconf, HiveConf.ConfVars.HADOOPMAPFILENAME))).toUri().getPath());
    ArrayList<Operator<? extends Serializable>> todo = new ArrayList<Operator<? extends Serializable>> ();
    statsMap.put(Counter.DESERIALIZE_ERRORS, deserialize_error_count);

    // for each configuration path that fpath can be relativized against ..
    for(String onefile: conf.getPathToAliases().keySet()) {
      Path onepath = new Path(new Path(onefile).toUri().getPath());
      if(!onepath.toUri().relativize(fpath.toUri()).equals(fpath.toUri())) {

        // pick up work corresponding to this configuration path
        List<String> aliases = conf.getPathToAliases().get(onefile);
        for(String onealias: aliases) {
          LOG.info("Adding alias " + onealias + " to work list for file " + fpath.toUri().getPath());
          todo.add(conf.getAliasToWork().get(onealias));
        }

        // initialize decoder once based on what table we are processing
        if(deserializer != null) {
          continue;
        }

        partitionDesc pd = conf.getPathToPartitionInfo().get(onefile);
        LinkedHashMap<String, String> partSpec = pd.getPartSpec();
        tableDesc td = pd.getTableDesc();
        Properties p = td.getProperties();
        // Add alias, table name, and partitions to hadoop conf
        HiveConf.setVar(hconf, HiveConf.ConfVars.HIVETABLENAME, String.valueOf(p.getProperty("name")));
        HiveConf.setVar(hconf, HiveConf.ConfVars.HIVEPARTITIONNAME, String.valueOf(partSpec));
        try {
          Class sdclass = td.getDeserializerClass();
          if(sdclass == null) {
            String className = td.getSerdeClassName();
            if ((className == "") || (className == null)) {
              throw new HiveException("SerDe class or the SerDe class name is not set for table: " + td.getProperties().getProperty("name"));
            }
            sdclass = MapOperator.class.getClassLoader().loadClass(className);
          }
          deserializer = (Deserializer) sdclass.newInstance();
          deserializer.initialize(hconf, p);
          rowObjectInspector = (StructObjectInspector)deserializer.getObjectInspector();
          
          // Next check if this table has partitions and if so
          // get the list of partition names as well as allocate
          // the serdes for the partition columns
          String pcols = p.getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
          if (pcols != null && pcols.length() > 0) {
            partNames = new ArrayList<String>();
            partValues = new ArrayList<String>();
            partObjectInspectors = new ArrayList<ObjectInspector>();
            String[] partKeys = pcols.trim().split("/");
            for(String key: partKeys) {
              partNames.add(key);
              partValues.add(partSpec.get(key));
              partObjectInspectors.add(
                  ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class));
            }
            StructObjectInspector partObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(partNames, partObjectInspectors);
            
            rowWithPart = new Object[2];
            rowWithPart[1] = partValues;
            rowObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(
                Arrays.asList(new StructObjectInspector[]{
                    rowObjectInspector, 
                    partObjectInspector}));
          }
          else {
            partNames = null;
            partValues = null;
          }

          LOG.info("Got partitions: " + pcols);
        } catch (SerDeException e) {
          e.printStackTrace();
          throw new HiveException (e);
        } catch (InstantiationException e) {
          throw new HiveException (e);
        } catch (IllegalAccessException e) {
          throw new HiveException (e);
        } catch (ClassNotFoundException e) {
          throw new HiveException (e);
        }
      }
    }

    if(todo.size() == 0) {
      // didn't find match for input file path in configuration!
      // serious problem ..
      LOG.error("Configuration does not have any alias for path: " + fpath.toUri().getPath());
      throw new HiveException("Configuration and input path are inconsistent");
    }

    // we found all the operators that we are supposed to process. now bootstrap
    this.setChildOperators(todo);
    // the child operators may need the global mr configuration. set it now so
    // that they can get access during initiaize.
    this.setMapredWork(conf);
    // way hacky - need to inform child operators about output collector
    this.setOutputCollector(out);

    for(Operator op: todo) {
      op.initialize(hconf);
    }
  }

  public void process(Writable value) throws HiveException {
    try {
      if (partNames == null) {
        row = deserializer.deserialize(value);
        forward(row, rowObjectInspector);
      } else {
        rowWithPart[0] = deserializer.deserialize(value);
        forward(rowWithPart, rowObjectInspector);
      }
    } catch (SerDeException e) {
      // TODO: policy on deserialization errors
      deserialize_error_count.set(deserialize_error_count.get()+1);
      throw new HiveException (e);
    }
  }

  public void process(Object row, ObjectInspector rowInspector)
      throws HiveException {
    throw new HiveException("Hive 2 Internal error: should not be called!");
  }
}
