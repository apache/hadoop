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

package org.apache.hadoop.vertica;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
  
/**
 * Input formatter that returns the results of a query executed against Vertica.
 * The key is a record number within the result set of each mapper The value is
 * a VerticaRecord, which uses a similar interface to JDBC ResultSets for
 * returning values.
 * 
 */
public class VerticaInputFormat extends
    InputFormat<LongWritable, VerticaRecord> {

  /**
   * Set the input query for a job
   * 
   * @param job
   * @param inputQuery
   *          query to run against Vertica
   */
  public static void setInput(Job job, String inputQuery) {
    job.setInputFormatClass(VerticaInputFormat.class);
    VerticaConfiguration config = new VerticaConfiguration(job
        .getConfiguration());
    config.setInputQuery(inputQuery);
  }

  /**
   * Set a parameterized input query for a job and the query that returns the
   * parameters.
   * 
   * @param job
   * @param inputQuery
   *          SQL query that has parameters specified by question marks ("?")
   * @param segmentParamsQuery
   *          SQL query that returns parameters for the input query
   */
  public static void setInput(Job job, String inputQuery,
      String segmentParamsQuery) {
    job.setInputFormatClass(VerticaInputFormat.class);
    VerticaConfiguration config = new VerticaConfiguration(job
        .getConfiguration());
    config.setInputQuery(inputQuery);
    config.setParamsQuery(segmentParamsQuery);
  }

  /**
   * Set the input query and any number of comma delimited literal list of
   * parameters
   * 
   * @param job
   * @param inputQuery
   *          SQL query that has parameters specified by question marks ("?")
   * @param segmentParams
   *          any numer of comma delimited strings with literal parameters to
   *          substitute in the input query
   */
  @SuppressWarnings("serial")
  public static void setInput(Job job, String inputQuery,
      String... segmentParams) throws IOException {
    // transform each param set into array
    DateFormat datefmt = DateFormat.getDateInstance();
    Collection<List<Object>> params = new HashSet<List<Object>>() {
    };
    for (String strParams : segmentParams) {
      List<Object> param = new ArrayList<Object>();

      for (String strParam : strParams.split(",")) {
        strParam = strParam.trim();
        if (strParam.charAt(0) == '\''
            && strParam.charAt(strParam.length() - 1) == '\'')
          param.add(strParam.substring(1, strParam.length() - 1));
        else {
          try {
            param.add(datefmt.parse(strParam));
          } catch (ParseException e1) {
            try {
              param.add(Integer.parseInt(strParam));
            } catch (NumberFormatException e2) {
              throw new IOException("Error parsing argument " + strParam);
            }
          }
        }
      }

      params.add(param);
    }

    setInput(job, inputQuery, params);
  }

  /**
   * Set the input query and a collection of parameter lists
   * 
   * @param job
   * @param inpuQuery
   *          SQL query that has parameters specified by question marks ("?")
   * @param segmentParams
   *          collection of ordered lists to subtitute into the input query
   * @throws IOException
   */
  public static void setInput(Job job, String inpuQuery,
      Collection<List<Object>> segmentParams) throws IOException {
    job.setInputFormatClass(VerticaInputFormat.class);
    VerticaConfiguration config = new VerticaConfiguration(job
        .getConfiguration());
    config.setInputQuery(inpuQuery);
    config.setInputParams(segmentParams);
  }

  /** {@inheritDoc} */
  public RecordReader<LongWritable, VerticaRecord> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    try {
      return new VerticaRecordReader((VerticaInputSplit) split, context
          .getConfiguration());
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /** {@inheritDoc} */
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    return VerticaUtil.getSplits(context);
  }
}
