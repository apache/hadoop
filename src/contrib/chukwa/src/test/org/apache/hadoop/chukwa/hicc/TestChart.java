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
package org.apache.hadoop.chukwa.hicc;

import junit.framework.TestCase;
import javax.servlet.http.HttpServletRequest;
import java.util.TreeMap;
import java.util.ArrayList;

public class TestChart extends TestCase {


  public void testLineChart()
  {
    HttpServletRequest request=null;
    Chart c = new Chart(request);
    String render= "line";
    TreeMap<String, TreeMap<String, Double>> dataMap = new TreeMap<String, TreeMap<String, Double>>();
    TreeMap<String, Double> series = new TreeMap<String, Double>();
    ArrayList<String> labels = new ArrayList<String>();
    for(int i=0;i<5;i++) {
      labels.add(""+i);
      series.put(""+i,1.0*i);
    }
    dataMap.put("series1", series);
    c.setXLabelsRange(labels);
    c.setDataSet(render,dataMap);
    String output = c.plot();
    assertTrue(output.contains("lines"));
  }

  public void testBarChart()
  {
    HttpServletRequest request=null;
    Chart c = new Chart(request);
    String render= "bar";
    TreeMap<String, TreeMap<String, Double>> dataMap = new TreeMap<String, TreeMap<String, Double>>();
    TreeMap<String, Double> series = new TreeMap<String, Double>();
    ArrayList<String> labels = new ArrayList<String>();
    for(int i=0;i<5;i++) {
      labels.add(""+i);
      series.put(""+i,1.0*i);
    }
    dataMap.put("series1", series);
    c.setXLabelsRange(labels);
    c.setDataSet(render,dataMap);
    String output = c.plot();
    assertTrue(output.contains("bar"));
  }

  public void testScatterChart()
  {
    HttpServletRequest request=null;
    Chart c = new Chart(request);
    String render= "point";
    TreeMap<String, TreeMap<String, Double>> dataMap = new TreeMap<String, TreeMap<String, Double>>();
    TreeMap<String, Double> series = new TreeMap<String, Double>();
    ArrayList<String> labels = new ArrayList<String>();
    for(int i=0;i<5;i++) {
      labels.add(""+i);
      series.put(""+i,1.0*i);
    }
    dataMap.put("series1", series);
    c.setXLabelsRange(labels);
    c.setDataSet(render,dataMap);
    String output = c.plot();
    assertTrue(output.contains("point"));
  }
}
