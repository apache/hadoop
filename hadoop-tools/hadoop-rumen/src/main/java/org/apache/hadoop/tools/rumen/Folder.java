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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Folder extends Configured implements Tool {
  private long outputDuration = -1;
  private long inputCycle = -1;
  private double concentration = 1.0;
  private long randomSeed = 0; // irrelevant if seeded == false
  private boolean seeded = false;
  private boolean debug = false;
  private boolean allowMissorting = false;
  private int skewBufferLength = 0;
  private long startsAfter = -1;

  static final private Logger LOG = LoggerFactory.getLogger(Folder.class);

  private DeskewedJobTraceReader reader = null;
  private Outputter<LoggedJob> outGen = null;

  private List<Path> tempPaths = new LinkedList<Path>();

  private Path tempDir = null;

  private long firstJobSubmitTime;

  private double timeDilation;

  private double transcriptionRateFraction;

  private int transcriptionRateInteger;

  private Random random;

  static private final long TICKS_PER_SECOND = 1000L;

  // error return codes
  static private final int NON_EXISTENT_FILES = 1;
  static private final int NO_INPUT_CYCLE_LENGTH = 2;
  static private final int EMPTY_JOB_TRACE = 3;
  static private final int OUT_OF_ORDER_JOBS = 4;
  static private final int ALL_JOBS_SIMULTANEOUS = 5;
  static private final int IO_ERROR = 6;
  static private final int OTHER_ERROR = 7;

  private Set<Closeable> closees = new HashSet<Closeable>();
  private Set<Path> deletees = new HashSet<Path>();

  static long parseDuration(String durationString) {
    String numeral = durationString.substring(0, durationString.length() - 1);
    char durationCode = durationString.charAt(durationString.length() - 1);

    long result = Integer.parseInt(numeral);

    if (result <= 0) {
      throw new IllegalArgumentException("Negative durations are not allowed");
    }

    switch (durationCode) {
    case 'D':
    case 'd':
      return 24L * 60L * 60L * TICKS_PER_SECOND * result;
    case 'H':
    case 'h':
      return 60L * 60L * TICKS_PER_SECOND * result;
    case 'M':
    case 'm':
      return 60L * TICKS_PER_SECOND * result;
    case 'S':
    case 's':
      return TICKS_PER_SECOND * result;
    default:
      throw new IllegalArgumentException("Missing or invalid duration code");
    }
  }

  private int initialize(String[] args) throws IllegalArgumentException {
    String tempDirName = null;
    String inputPathName = null;
    String outputPathName = null;

    for (int i = 0; i < args.length; ++i) {
      String thisArg = args[i];
      if (thisArg.equalsIgnoreCase("-starts-after")) {
        startsAfter = parseDuration(args[++i]);
      } else if (thisArg.equalsIgnoreCase("-output-duration")) {
        outputDuration = parseDuration(args[++i]);
      } else if (thisArg.equalsIgnoreCase("-input-cycle")) {
        inputCycle = parseDuration(args[++i]);
      } else if (thisArg.equalsIgnoreCase("-concentration")) {
        concentration = Double.parseDouble(args[++i]);
      } else if (thisArg.equalsIgnoreCase("-debug")) {
        debug = true;
      } else if (thisArg.equalsIgnoreCase("-allow-missorting")) {
        allowMissorting = true;
      } else if (thisArg.equalsIgnoreCase("-seed")) {
        seeded = true;
        randomSeed = Long.parseLong(args[++i]);
      } else if (thisArg.equalsIgnoreCase("-skew-buffer-length")) {
        skewBufferLength = Integer.parseInt(args[++i]);
      } else if (thisArg.equalsIgnoreCase("-temp-directory")) {
        tempDirName = args[++i];
      } else if (thisArg.equals("") || thisArg.startsWith("-")) {
        throw new IllegalArgumentException("Illegal switch argument, "
            + thisArg + " at position " + i);
      } else {
        inputPathName = thisArg;
        outputPathName = args[++i];

        if (i != args.length - 1) {
          throw new IllegalArgumentException("Too many non-switch arguments");
        }
      }
    }

    try {
      Configuration conf = getConf();
      Path inPath = new Path(inputPathName);
      reader =
          new DeskewedJobTraceReader(new JobTraceReader(inPath, conf),
              skewBufferLength, !allowMissorting);
      Path outPath = new Path(outputPathName);

      outGen = new DefaultOutputter<LoggedJob>();
      outGen.init(outPath, conf);

      tempDir =
          tempDirName == null ? outPath.getParent() : new Path(tempDirName);

      FileSystem fs = tempDir.getFileSystem(getConf());
      if (!fs.getFileStatus(tempDir).isDirectory()) {
        throw new IOException("Your temp directory is not a directory");
      }

      if (inputCycle <= 0) {
        LOG.error("You must have an input cycle length.");
        return NO_INPUT_CYCLE_LENGTH;
      }

      if (outputDuration <= 0) {
        outputDuration = 60L * 60L * TICKS_PER_SECOND;
      }

      if (inputCycle <= 0) {
        inputCycle = outputDuration;
      }

      timeDilation = (double) outputDuration / (double) inputCycle;

      random = seeded ? new Random(randomSeed) : new Random();

      if (debug) {
        randomSeed = random.nextLong();

        LOG.warn("This run effectively has a -seed of " + randomSeed);

        random = new Random(randomSeed);

        seeded = true;
      }
    } catch (IOException e) {
      e.printStackTrace(System.err);

      return NON_EXISTENT_FILES;
    }

    return 0;
  }

  @Override
  public int run(String[] args) throws IOException {
    int result = initialize(args);

    if (result != 0) {
      return result;
    }

    return run();
  }

  public int run() throws IOException {
    class JobEntryComparator implements
        Comparator<Pair<LoggedJob, JobTraceReader>> {
      public int compare(Pair<LoggedJob, JobTraceReader> p1,
          Pair<LoggedJob, JobTraceReader> p2) {
        LoggedJob j1 = p1.first();
        LoggedJob j2 = p2.first();

        return (j1.getSubmitTime() < j2.getSubmitTime()) ? -1 : (j1
            .getSubmitTime() == j2.getSubmitTime()) ? 0 : 1;
      }
    }

    // we initialize an empty heap so if we take an error before establishing
    // a real one the finally code goes through
    Queue<Pair<LoggedJob, JobTraceReader>> heap =
        new PriorityQueue<Pair<LoggedJob, JobTraceReader>>();

    try {
      LoggedJob job = reader.nextJob();

      if (job == null) {
        LOG.error("The job trace is empty");

        return EMPTY_JOB_TRACE;
      }
      
      // If starts-after time is specified, skip the number of jobs till we reach
      // the starting time limit.
      if (startsAfter > 0) {
        LOG.info("starts-after time is specified. Initial job submit time : " 
                 + job.getSubmitTime());

        long approximateTime = job.getSubmitTime() + startsAfter;
        job = reader.nextJob();
        long skippedCount = 0;
        while (job != null && job.getSubmitTime() < approximateTime) {
          job = reader.nextJob();
          skippedCount++;
        }

        LOG.debug("Considering jobs with submit time greater than " 
                  + startsAfter + " ms. Skipped " + skippedCount + " jobs.");

        if (job == null) {
          LOG.error("No more jobs to process in the trace with 'starts-after'"+
                    " set to " + startsAfter + "ms.");
          return EMPTY_JOB_TRACE;
        }
        LOG.info("The first job has a submit time of " + job.getSubmitTime());
      }

      firstJobSubmitTime = job.getSubmitTime();
      long lastJobSubmitTime = firstJobSubmitTime;

      int numberJobs = 0;

      long currentIntervalEnd = Long.MIN_VALUE;

      Path nextSegment = null;
      Outputter<LoggedJob> tempGen = null;

      if (debug) {
        LOG.debug("The first job has a submit time of " + firstJobSubmitTime);
      }

      final Configuration conf = getConf();

      try {
        // At the top of this loop, skewBuffer has at most
        // skewBufferLength entries.
        while (job != null) {
          final Random tempNameGenerator = new Random();

          lastJobSubmitTime = job.getSubmitTime();

          ++numberJobs;

          if (job.getSubmitTime() >= currentIntervalEnd) {
            if (tempGen != null) {
              tempGen.close();
            }
            
            nextSegment = null;
            for (int i = 0; i < 3 && nextSegment == null; ++i) {
              try {
                nextSegment =
                    new Path(tempDir, "segment-" + tempNameGenerator.nextLong()
                        + ".json.gz");

                if (debug) {
                  LOG.debug("The next segment name is " + nextSegment);
                }

                FileSystem fs = nextSegment.getFileSystem(conf);

                try {
                  if (!fs.exists(nextSegment)) {
                    break;
                  }

                  continue;
                } catch (IOException e) {
                  // no code -- file did not already exist
                }
              } catch (IOException e) {
                // no code -- file exists now, or directory bad. We try three
                // times.
              }
            }

            if (nextSegment == null) {
              throw new RuntimeException("Failed to create a new file!");
            }
            
            if (debug) {
              LOG.debug("Creating " + nextSegment
                  + " for a job with a submit time of " + job.getSubmitTime());
            }

            deletees.add(nextSegment);

            tempPaths.add(nextSegment);

            tempGen = new DefaultOutputter<LoggedJob>();
            tempGen.init(nextSegment, conf);

            long currentIntervalNumber =
                (job.getSubmitTime() - firstJobSubmitTime) / inputCycle;

            currentIntervalEnd =
                firstJobSubmitTime + ((currentIntervalNumber + 1) * inputCycle);
          }

          // the temp files contain UDadjusted times, but each temp file's
          // content is in the same input cycle interval.
          if (tempGen != null) {
            tempGen.output(job);
          }

          job = reader.nextJob();
        }
      } catch (DeskewedJobTraceReader.OutOfOrderException e) {
        return OUT_OF_ORDER_JOBS;
      } finally {
        if (tempGen != null) {
          tempGen.close();
        }
      }

      if (lastJobSubmitTime <= firstJobSubmitTime) {
        LOG.error("All of your job[s] have the same submit time."
            + "  Please just use your input file.");

        return ALL_JOBS_SIMULTANEOUS;
      }

      double submitTimeSpan = lastJobSubmitTime - firstJobSubmitTime;

      LOG.warn("Your input trace spans "
          + (lastJobSubmitTime - firstJobSubmitTime) + " ticks.");

      double foldingRatio =
          submitTimeSpan * (numberJobs + 1) / numberJobs / inputCycle;

      if (debug) {
        LOG.warn("run: submitTimeSpan = " + submitTimeSpan + ", numberJobs = "
            + numberJobs + ", inputCycle = " + inputCycle);
      }

      if (reader.neededSkewBufferSize() > 0) {
        LOG.warn("You needed a -skew-buffer-length of "
            + reader.neededSkewBufferSize() + " but no more, for this input.");
      }

      double tProbability = timeDilation * concentration / foldingRatio;

      if (debug) {
        LOG.warn("run: timeDilation = " + timeDilation + ", concentration = "
            + concentration + ", foldingRatio = " + foldingRatio);
        LOG.warn("The transcription probability is " + tProbability);
      }

      transcriptionRateInteger = (int) Math.floor(tProbability);
      transcriptionRateFraction = tProbability - Math.floor(tProbability);

      // Now read all the inputs in parallel
      heap =
          new PriorityQueue<Pair<LoggedJob, JobTraceReader>>(tempPaths.size(),
              new JobEntryComparator());

      for (Path tempPath : tempPaths) {
        JobTraceReader thisReader = new JobTraceReader(tempPath, conf);

        closees.add(thisReader);

        LoggedJob streamFirstJob = thisReader.getNext();

        long thisIndex =
            (streamFirstJob.getSubmitTime() - firstJobSubmitTime) / inputCycle;

        if (debug) {
          LOG.debug("A job with submit time of "
              + streamFirstJob.getSubmitTime() + " is in interval # "
              + thisIndex);
        }

        adjustJobTimes(streamFirstJob);

        if (debug) {
          LOG.debug("That job's submit time is adjusted to "
              + streamFirstJob.getSubmitTime());
        }

        heap
            .add(new Pair<LoggedJob, JobTraceReader>(streamFirstJob, thisReader));
      }

      Pair<LoggedJob, JobTraceReader> next = heap.poll();

      while (next != null) {
        maybeOutput(next.first());

        if (debug) {
          LOG.debug("The most recent job has an adjusted submit time of "
              + next.first().getSubmitTime());
          LOG.debug(" Its replacement in the heap will come from input engine "
              + next.second());
        }

        LoggedJob replacement = next.second().getNext();

        if (replacement == null) {
          next.second().close();

          if (debug) {
            LOG.debug("That input engine is depleted.");
          }
        } else {
          adjustJobTimes(replacement);

          if (debug) {
            LOG.debug("The replacement has an adjusted submit time of "
                + replacement.getSubmitTime());
          }

          heap.add(new Pair<LoggedJob, JobTraceReader>(replacement, next
              .second()));
        }

        next = heap.poll();
      }
    } finally {
      IOUtils.cleanup(null, reader);
      if (outGen != null) {
        outGen.close();
      }
      for (Pair<LoggedJob, JobTraceReader> heapEntry : heap) {
        heapEntry.second().close();
      }
      for (Closeable closee : closees) {
        closee.close();
      }
      if (!debug) {
        Configuration conf = getConf();

        for (Path deletee : deletees) {
          FileSystem fs = deletee.getFileSystem(conf);

          try {
            fs.delete(deletee, false);
          } catch (IOException e) {
            // no code
          }
        }
      }
    }

    return 0;
  }

  private void maybeOutput(LoggedJob job) throws IOException {
    for (int i = 0; i < transcriptionRateInteger; ++i) {
      outGen.output(job);
    }

    if (random.nextDouble() < transcriptionRateFraction) {
      outGen.output(job);
    }
  }

  private void adjustJobTimes(LoggedJob adjustee) {
    long offsetInCycle =
        (adjustee.getSubmitTime() - firstJobSubmitTime) % inputCycle;

    long outputOffset = (long) ((double) offsetInCycle * timeDilation);

    long adjustment =
        firstJobSubmitTime + outputOffset - adjustee.getSubmitTime();

    adjustee.adjustTimes(adjustment);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    Folder instance = new Folder();

    int result = 0;

    try {
      result = ToolRunner.run(instance, args);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      System.exit(IO_ERROR);
    } catch (Exception e) {
      e.printStackTrace(System.err);
      System.exit(OTHER_ERROR);
    }

    if (result != 0) {
      System.exit(result);
    }

    return;
  }
}
