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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.pi.DistSum.Computation;
import org.apache.hadoop.examples.pi.DistSum.Parameters;
import org.apache.hadoop.examples.pi.math.Bellard;
import org.apache.hadoop.examples.pi.math.Summation;
import org.apache.hadoop.examples.pi.math.Bellard.Parameter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map/reduce program that uses a BBP-type method to compute exact 
 * binary digits of Pi.
 * This program is designed for computing the n th bit of Pi,
 * for large n, say n &gt;= 10^8.
 * For computing lower bits of Pi, consider using bbp.
 *
 * The actually computation is done by DistSum jobs.
 * The steps for launching the jobs are:
 * 
 * (1) Initialize parameters.
 * (2) Create a list of sums.
 * (3) Read computed values from the given local directory.
 * (4) Remove the computed values from the sums.
 * (5) Partition the remaining sums into computation jobs.
 * (6) Submit the computation jobs to a cluster and then wait for the results.
 * (7) Write job outputs to the given local directory.
 * (8) Combine the job outputs and print the Pi bits.
 */
/*
 * The command line format is:
 * > hadoop org.apache.hadoop.examples.pi.DistBbp \
 *          <b> <nThreads> <nJobs> <type> <nPart> <remoteDir> <localDir>
 * 
 * And the parameters are:
 *  <b>         The number of bits to skip, i.e. compute the (b+1)th position.
 *  <nThreads>  The number of working threads.
 *  <nJobs>     The number of jobs per sum.
 *  <type>      'm' for map side job, 'r' for reduce side job, 'x' for mix type.
 *  <nPart>     The number of parts per job.
 *  <remoteDir> Remote directory for submitting jobs.
 *  <localDir>  Local directory for storing output files.
 *
 * Note that it may take a long time to finish all the jobs when <b> is large.
 * If the program is killed in the middle of the execution, the same command with
 * a different <remoteDir> can be used to resume the execution.  For example, suppose
 * we use the following command to compute the (10^15+57)th bit of Pi.
 * 
 * > hadoop org.apache.hadoop.examples.pi.DistBbp \
 *          1,000,000,000,000,056 20 1000 x 500 remote/a local/output
 *
 * It uses 20 threads to summit jobs so that there are at most 20 concurrent jobs.
 * Each sum (there are totally 14 sums) is partitioned into 1000 jobs.
 * The jobs will be executed in map-side or reduce-side.  Each job has 500 parts.
 * The remote directory for the jobs is remote/a and the local directory
 * for storing output is local/output.  Depends on the cluster configuration,
 * it may take many days to finish the entire execution.  If the execution is killed,
 * we may resume it by
 * 
 * > hadoop org.apache.hadoop.examples.pi.DistBbp \
 *          1,000,000,000,000,056 20 1000 x 500 remote/b local/output
 */
public final class DistBbp extends Configured implements Tool {
  public static final String DESCRIPTION
      = "A map/reduce program that uses a BBP-type formula to compute exact bits of Pi.";

  private final Util.Timer timer = new Util.Timer(true);

  /** {@inheritDoc} */
  public int run(String[] args) throws Exception {
    //parse arguments
    if (args.length != DistSum.Parameters.COUNT + 1)
      return Util.printUsage(args,
          getClass().getName() + " <b> " + Parameters.LIST
          + "\n  <b> The number of bits to skip, i.e. compute the (b+1)th position."
          + Parameters.DESCRIPTION);

    int i = 0;
    final long b = Util.string2long(args[i++]);
    final DistSum.Parameters parameters = DistSum.Parameters.parse(args, i);

    if (b < 0)
      throw new IllegalArgumentException("b = " + b + " < 0");
    Util.printBitSkipped(b);
    Util.out.println(parameters);
    Util.out.println();

    //initialize sums
    final DistSum distsum = new DistSum();
    distsum.setConf(getConf());
    distsum.setParameters(parameters);
    final boolean isVerbose = getConf().getBoolean(Parser.VERBOSE_PROPERTY, false);
    final Map<Parameter, List<TaskResult>> existings = new Parser(isVerbose).parse(parameters.localDir.getPath(), null);
    Parser.combine(existings);
    for(List<TaskResult> tr : existings.values())
      Collections.sort(tr);
    Util.out.println();
    final Map<Bellard.Parameter, Bellard.Sum> sums = Bellard.getSums(b, parameters.nJobs, existings);
    Util.out.println();

    //execute the computations
    execute(distsum, sums);

    //compute Pi from the sums 
    final double pi = Bellard.computePi(b, sums);
    Util.printBitSkipped(b);
    Util.out.println(Util.pi2string(pi, Bellard.bit2terms(b)));
    return 0;
  }
  
  /** Execute DistSum computations */
  private void execute(DistSum distsum,
      final Map<Bellard.Parameter, Bellard.Sum> sums) throws Exception {
    final List<Computation> computations = new ArrayList<Computation>();
    int i = 0;
    for(Bellard.Parameter p : Bellard.Parameter.values())
      for(Summation s : sums.get(p))
        if (s.getValue() == null)
          computations.add(distsum.new Computation(i++, p.toString(), s));

    if (computations.isEmpty())
      Util.out.println("No computation");
    else {
      timer.tick("execute " + computations.size() + " computation(s)");
      Util.execute(distsum.getParameters().nThreads, computations);
      timer.tick("done");
    }
  }

  /** main */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(null, new DistBbp(), args));
  }
}
