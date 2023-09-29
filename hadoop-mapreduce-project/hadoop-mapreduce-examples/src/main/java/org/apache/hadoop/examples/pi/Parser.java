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
package org.apache.hadoop.examples.pi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.examples.pi.math.Bellard;
import org.apache.hadoop.examples.pi.math.Bellard.Parameter;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;

/** A class for parsing outputs */
public final class Parser {
  static final String VERBOSE_PROPERTY = "pi.parser.verbose";

  final boolean isVerbose;
  
  public Parser(boolean isVerbose) {
    this.isVerbose = isVerbose;
  }
  
  private void println(String s) {
    if (isVerbose)
      Util.out.println(s);
  }

  /** Parse a line */
  private static void parseLine(final String line, Map<Parameter, List<TaskResult>> m) {
//      LOG.info("line = " + line);
    final Map.Entry<String, TaskResult> e = DistSum.string2TaskResult(line);
    if (e != null) {
      final List<TaskResult> sums = m.get(Parameter.get(e.getKey()));
      if (sums == null)
        throw new IllegalArgumentException("sums == null, line=" + line + ", e=" + e);
      sums.add(e.getValue());
    }
  }

  /** Parse a file or a directory tree */
  private void parse(File f, Map<Parameter, List<TaskResult>> sums) throws IOException {
    if (f.isDirectory()) {
      println("Process directory " + f);
      File[] files = f.listFiles();
      if (files != null) {
        for(File child : files) {
          parse(child, sums);
        }
      }
    } else if (f.getName().endsWith(".txt")) {
      println("Parse file " + f);
      final Map<Parameter, List<TaskResult>> m = new TreeMap<Parameter, List<TaskResult>>();    
      for(Parameter p : Parameter.values())
        m.put(p, new ArrayList<TaskResult>());

      final BufferedReader in = new BufferedReader(
          new InputStreamReader(new FileInputStream(f), Charsets.UTF_8)); 
      try {
        for(String line; (line = in.readLine()) != null; )
          try {
            parseLine(line, m);
          } catch(RuntimeException e) {
            Util.err.println("line = " + line);
            throw e;
          }
      } finally {
        in.close();
      }

      for(Parameter p : Parameter.values()) {
        final List<TaskResult> combined = Util.combine(m.get(p));
        if (!combined.isEmpty()) {
          println(p + " (size=" + combined.size() + "):");
          for(TaskResult r : combined)
            println("  " + r);
        }
        sums.get(p).addAll(m.get(p));
      }
    }
  }

  /** Parse a path */
  private Map<Parameter, List<TaskResult>> parse(String f) throws IOException {
    final Map<Parameter, List<TaskResult>> m = new TreeMap<Parameter, List<TaskResult>>();
    for(Parameter p : Parameter.values())
      m.put(p, new ArrayList<TaskResult>());
    parse(new File(f), m);

    //LOG.info("m=" + m.toString().replace(", ", ",\n  "));
    for(Parameter p : Parameter.values())
      m.put(p, m.get(p));
    return m;
  }

  /** Parse input and re-write results. */
  Map<Parameter, List<TaskResult>> parse(String inputpath, String outputdir
      ) throws IOException {
    //parse input
    Util.out.print("\nParsing " + inputpath + " ... ");
    Util.out.flush();
    final Map<Parameter, List<TaskResult>> parsed = parse(inputpath);
    Util.out.println("DONE");

    //re-write the results
    if (outputdir != null) {
      Util.out.print("\nWriting to " + outputdir + " ...");
      Util.out.flush();
      for(Parameter p : Parameter.values()) {
        final List<TaskResult> results = parsed.get(p);
        Collections.sort(results);

        final PrintWriter out = new PrintWriter(
            new OutputStreamWriter(new FileOutputStream(
                new File(outputdir, p + ".txt")), Charsets.UTF_8), true);
        try {
          for(int i = 0; i < results.size(); i++)
            out.println(DistSum.taskResult2string(p + "." + i, results.get(i)));
        }
        finally {
          out.close();
        }
      }
      Util.out.println("DONE");
    }
    return parsed;
  }

  /** Combine results */
  static <T extends Combinable<T>> Map<Parameter, T> combine(Map<Parameter, List<T>> m) {
    final Map<Parameter, T> combined = new TreeMap<Parameter, T>();
    for(Parameter p : Parameter.values()) {
      //note: results would never be null due to the design of Util.combine
      final List<T> results = Util.combine(m.get(p));
      Util.out.format("%-6s => ", p); 
      if (results.size() != 1)
        Util.out.println(results.toString().replace(", ", ",\n           "));
      else {
        final T r = results.get(0);
        combined.put(p, r); 
        Util.out.println(r);
      }
    }
    return combined;
  }

  /** main */
  public static void main(String[] args) throws IOException {
    if (args.length < 2 || args.length > 3)
      Util.printUsage(args, Parser.class.getName()
          + " <b> <inputpath> [<outputdir>]");

    int i = 0;
    final long b = Util.string2long(args[i++]);
    final String inputpath = args[i++];
    final String outputdir = args.length >= 3? args[i++]: null;

    //read input
    final Map<Parameter, List<TaskResult>> parsed = new Parser(true).parse(inputpath, outputdir);
    final Map<Parameter, TaskResult> combined = combine(parsed);
    long duration = 0;
    for(TaskResult r : combined.values())
      duration += r.getDuration();

    //print pi
    final double pi = Bellard.computePi(b, combined);
    Util.printBitSkipped(b);
    Util.out.println(Util.pi2string(pi, Bellard.bit2terms(b)));
    Util.out.println("cpu time = " + Util.millis2String(duration));
  }
}
