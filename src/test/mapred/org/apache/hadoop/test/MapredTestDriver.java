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

package org.apache.hadoop.test;

import org.apache.hadoop.io.TestSequenceFile;
import org.apache.hadoop.mapred.BigMapOutput;
import org.apache.hadoop.mapred.GenericMRLoadGenerator;
import org.apache.hadoop.mapred.MRBench;
import org.apache.hadoop.mapred.ReliabilityTest;
import org.apache.hadoop.mapred.SortValidator;
import org.apache.hadoop.mapred.TestMapRed;
import org.apache.hadoop.mapred.TestSequenceFileInputFormat;
import org.apache.hadoop.mapred.TestTextInputFormat;
import org.apache.hadoop.mapred.ThreadedMapBenchmark;
import org.apache.hadoop.util.ProgramDriver;

/**
 * Driver for Map-reduce tests.
 *
 */
public class MapredTestDriver {

  private ProgramDriver pgd;
  
  public MapredTestDriver() {
    this(new ProgramDriver());
  }
  
  public MapredTestDriver(ProgramDriver pgd) {
    this.pgd = pgd;
    try {
      pgd.addClass("testsequencefile", TestSequenceFile.class, 
      "A test for flat files of binary key value pairs.");
      pgd.addClass("threadedmapbench", ThreadedMapBenchmark.class, 
          "A map/reduce benchmark that compares the performance " + 
          "of maps with multiple spills over maps with 1 spill");
      pgd.addClass("mrbench", MRBench.class, 
          "A map/reduce benchmark that can create many small jobs");
      pgd.addClass("mapredtest", TestMapRed.class, "A map/reduce test check.");
      pgd.addClass("testsequencefileinputformat", 
          TestSequenceFileInputFormat.class, 
          "A test for sequence file input format.");
      pgd.addClass("testtextinputformat", TestTextInputFormat.class, 
          "A test for text input format.");
      pgd.addClass("testmapredsort", SortValidator.class, 
          "A map/reduce program that validates the " +
          "map-reduce framework's sort.");
      pgd.addClass("testbigmapoutput", BigMapOutput.class, 
          "A map/reduce program that works on a very big " +
          "non-splittable file and does identity map/reduce");
      pgd.addClass("loadgen", GenericMRLoadGenerator.class, 
          "Generic map/reduce load generator");
      pgd.addClass("MRReliabilityTest", ReliabilityTest.class,
          "A program that tests the reliability of the MR framework by " +
          "injecting faults/failures");
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }

  public void run(String argv[]) {
    try {
      pgd.driver(argv);
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }

  public static void main(String argv[]){
    new MapredTestDriver().run(argv);
  }
}

