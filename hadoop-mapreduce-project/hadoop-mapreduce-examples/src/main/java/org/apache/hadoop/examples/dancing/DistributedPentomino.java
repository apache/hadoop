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

package org.apache.hadoop.examples.dancing;

import java.io.*;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

import com.google.common.base.Charsets;

/**
 * Launch a distributed pentomino solver.
 * It generates a complete list of prefixes of length N with each unique prefix
 * as a separate line. A prefix is a sequence of N integers that denote the 
 * index of the row that is choosen for each column in order. Note that the
 * next column is heuristically choosen by the solver, so it is dependant on
 * the previous choice. That file is given as the input to
 * map/reduce. The output key/value are the move prefix/solution as Text/Text.
 */
public class DistributedPentomino extends Configured implements Tool {

  private static final int PENT_DEPTH = 5;
  private static final int PENT_WIDTH = 9;
  private static final int PENT_HEIGHT = 10;
  private static final int DEFAULT_MAPS = 2000;
  
  /**
   * Each map takes a line, which represents a prefix move and finds all of 
   * the solutions that start with that prefix. The output is the prefix as
   * the key and the solution as the value.
   */
  public static class PentMap extends 
      Mapper<WritableComparable<?>, Text, Text, Text> {
    
    private int width;
    private int height;
    private int depth;
    private Pentomino pent;
    private Text prefixString;
    private Context context;
    
    /**
     * For each solution, generate the prefix and a string representation
     * of the solution. The solution starts with a newline, so that the output
     * looks like:
     * <prefix>,
     * <solution>
     * 
     */
    class SolutionCatcher 
    implements DancingLinks.SolutionAcceptor<Pentomino.ColumnName> {
      public void solution(List<List<Pentomino.ColumnName>> answer) {
        String board = Pentomino.stringifySolution(width, height, answer);
        try {
          context.write(prefixString, new Text("\n" + board));
          context.getCounter(pent.getCategory(answer)).increment(1);
        } catch (IOException e) {
          System.err.println(StringUtils.stringifyException(e));
        } catch (InterruptedException ie) {
          System.err.println(StringUtils.stringifyException(ie));
        }
      }
    }
    
    /**
     * Break the prefix string into moves (a sequence of integer row ids that 
     * will be selected for each column in order). Find all solutions with
     * that prefix.
     */
    public void map(WritableComparable<?> key, Text value,Context context) 
        throws IOException {
      prefixString = value;
      StringTokenizer itr = new StringTokenizer(prefixString.toString(), ",");
      int[] prefix = new int[depth];
      int idx = 0;
      while (itr.hasMoreTokens()) {
        String num = itr.nextToken();
        prefix[idx++] = Integer.parseInt(num);
      }
      pent.solve(prefix);
    }
    
    @Override
    public void setup(Context context) {
      this.context = context;
      Configuration conf = context.getConfiguration();
      depth = conf.getInt(Pentomino.DEPTH, PENT_DEPTH);
      width = conf.getInt(Pentomino.WIDTH, PENT_WIDTH);
      height = conf.getInt(Pentomino.HEIGHT, PENT_HEIGHT);
      pent = (Pentomino) 
        ReflectionUtils.newInstance(conf.getClass(Pentomino.CLASS, 
                                                  OneSidedPentomino.class), 
                                    conf);
      pent.initialize(width, height);
      pent.setPrinter(new SolutionCatcher());
    }
  }
  
  /**
   * Create the input file with all of the possible combinations of the 
   * given depth.
   * @param fs the filesystem to write into
   * @param dir the directory to write the input file into
   * @param pent the puzzle 
   * @param depth the depth to explore when generating prefixes
   */
  private static long createInputDirectory(FileSystem fs, 
                                           Path dir,
                                           Pentomino pent,
                                           int depth
                                           ) throws IOException {
    fs.mkdirs(dir);
    List<int[]> splits = pent.getSplits(depth);
    Path input = new Path(dir, "part1");
    PrintWriter file = 
      new PrintWriter(new OutputStreamWriter(new BufferedOutputStream
                      (fs.create(input), 64*1024), Charsets.UTF_8));
    for(int[] prefix: splits) {
      for(int i=0; i < prefix.length; ++i) {
        if (i != 0) {
          file.print(',');          
        }
        file.print(prefix[i]);
      }
      file.print('\n');
    }
    file.close();
    return fs.getFileStatus(input).getLen();
  }
  
  /**
   * Launch the solver on 9x10 board and the one sided pentominos.
   * This takes about 2.5 hours on 20 nodes with 2 cpus/node.
   * Splits the job into 2000 maps and 1 reduce.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), 
                new DistributedPentomino(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    if (args.length == 0) {
      System.out.println("Usage: pentomino <output> [-depth #] [-height #] [-width #]");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }
    // check for passed parameters, otherwise use defaults
    int width = conf.getInt(Pentomino.WIDTH, PENT_WIDTH);
    int height = conf.getInt(Pentomino.HEIGHT, PENT_HEIGHT);
    int depth = conf.getInt(Pentomino.DEPTH, PENT_DEPTH);
    for (int i = 0; i < args.length; i++) {
      if (args[i].equalsIgnoreCase("-depth")) {
        depth = Integer.parseInt(args[++i].trim());
      } else if (args[i].equalsIgnoreCase("-height")) {
        height = Integer.parseInt(args[++i].trim());
      } else if (args[i].equalsIgnoreCase("-width") ) {
        width = Integer.parseInt(args[++i].trim());
      }
    }
    // now set the values within conf for M/R tasks to read, this
    // will ensure values are set preventing MAPREDUCE-4678
    conf.setInt(Pentomino.WIDTH, width);
    conf.setInt(Pentomino.HEIGHT, height);
    conf.setInt(Pentomino.DEPTH, depth);
    Class<? extends Pentomino> pentClass = conf.getClass(Pentomino.CLASS, 
      OneSidedPentomino.class, Pentomino.class);
    int numMaps = conf.getInt(MRJobConfig.NUM_MAPS, DEFAULT_MAPS);
    Path output = new Path(args[0]);
    Path input = new Path(output + "_input");
    FileSystem fileSys = FileSystem.get(conf);
    try {
      Job job = new Job(conf);
      FileInputFormat.setInputPaths(job, input);
      FileOutputFormat.setOutputPath(job, output);
      job.setJarByClass(PentMap.class);
      
      job.setJobName("dancingElephant");
      Pentomino pent = ReflectionUtils.newInstance(pentClass, conf);
      pent.initialize(width, height);
      long inputSize = createInputDirectory(fileSys, input, pent, depth);
      // for forcing the number of maps
      FileInputFormat.setMaxInputSplitSize(job, (inputSize/numMaps));
   
      // the keys are the prefix strings
      job.setOutputKeyClass(Text.class);
      // the values are puzzle solutions
      job.setOutputValueClass(Text.class);
      
      job.setMapperClass(PentMap.class);        
      job.setReducerClass(Reducer.class);
      
      job.setNumReduceTasks(1);
      
      return (job.waitForCompletion(true) ? 0 : 1);
      } finally {
      fileSys.delete(input, true);
    }
  }
}
