package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Multithreaded implementation for @link org.apache.hadoop.mapred.MapRunnable.
 * <p>
 * It can be used instead of the default implementation,
 * @link org.apache.hadoop.mapred.MapRunner, when the Map operation is not CPU
 * bound in order to improve throughput.
 * <p>
 * Map implementations using this MapRunnable must be thread-safe.
 * <p>
 * The Map-Reduce job has to be configured to use this MapRunnable class (using
 * the <b>mapred.map.runner.class</b> property) and
 * the number of thread the thread-pool can use (using the
 * <b>mapred.map.multithreadedrunner.threads</b> property).
 * <p>
 *
 * @author Alejandro Abdelnur
 */
public class MultithreadedMapRunner implements MapRunnable {
  private static final Log LOG =
      LogFactory.getLog(MultithreadedMapRunner.class.getName());

  private JobConf job;
  private Mapper mapper;
  private ExecutorService executorService;
  private volatile IOException ioException;

  public void configure(JobConf job) {
    int numberOfThreads =
        job.getInt("mapred.map.multithreadedrunner.threads", 10);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configuring job " + job.getJobName() +
          " to use " + numberOfThreads + " threads" );
    }

    this.job = job;
    this.mapper = (Mapper)ReflectionUtils.newInstance(job.getMapperClass(),
                                                      job);

    // Creating a threadpool of the configured size to execute the Mapper
    // map method in parallel.
    executorService = Executors.newFixedThreadPool(numberOfThreads);
  }

  public void run(RecordReader input, OutputCollector output,
                  Reporter reporter)
    throws IOException {
    try {
      // allocate key & value instances these objects will not be reused
      // because execution of Mapper.map is not serialized.
      WritableComparable key = input.createKey();
      Writable value = input.createValue();

      while (input.next(key, value)) {

        // Run Mapper.map execution asynchronously in a separate thread.
        // If threads are not available from the thread-pool this method
        // will block until there is a thread available.
        executorService.execute(
            new MapperInvokeRunable(key, value, output, reporter));

        // Checking if a Mapper.map within a Runnable has generated an
        // IOException. If so we rethrow it to force an abort of the Map
        // operation thus keeping the semantics of the default
        // implementation.
        if (ioException != null) {
          throw ioException;
        }

        // Allocate new key & value instances as mapper is running in parallel
        key = input.createKey();
        value = input.createValue();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished dispatching all Mappper.map calls, job "
            + job.getJobName());
      }

      // Graceful shutdown of the Threadpool, it will let all scheduled
      // Runnables to end.
      executorService.shutdown();

      try {

        // Now waiting for all Runnables to end.
        while (!executorService.awaitTermination(100, TimeUnit.MILLISECONDS)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Awaiting all running Mappper.map calls to finish, job "
                + job.getJobName());
          }

          // Checking if a Mapper.map within a Runnable has generated an
          // IOException. If so we rethrow it to force an abort of the Map
          // operation thus keeping the semantics of the default
          // implementation.
          // NOTE: while Mapper.map dispatching has concluded there are still
          // map calls in progress.
          if (ioException != null) {
            throw ioException;
          }
        }

        // Checking if a Mapper.map within a Runnable has generated an
        // IOException. If so we rethrow it to force an abort of the Map
        // operation thus keeping the semantics of the default
        // implementation.
        // NOTE: it could be that a map call has had an exception after the
        // call for awaitTermination() returing true. And edge case but it
        // could happen.
        if (ioException != null) {
          throw ioException;
        }
      }
      catch (IOException ioEx) {
        // Forcing a shutdown of all thread of the threadpool and rethrowing
        // the IOException
        executorService.shutdownNow();
        throw ioEx;
      }
      catch (InterruptedException iEx) {
        throw new IOException(iEx.getMessage());
      }

    } finally {
        mapper.close();
    }
  }


  /**
   * Runnable to execute a single Mapper.map call from a forked thread.
   */
  private class MapperInvokeRunable implements Runnable {
    private WritableComparable key;
    private Writable value;
    private OutputCollector output;
    private Reporter reporter;

    /**
     * Collecting all required parameters to execute a Mapper.map call.
     * <p>
     *
     * @param key
     * @param value
     * @param output
     * @param reporter
     */
    public MapperInvokeRunable(WritableComparable key, Writable value,
        OutputCollector output, Reporter reporter) {
      this.key = key;
      this.value = value;
      this.output = output;
      this.reporter = reporter;
    }

    /**
     * Executes a Mapper.map call with the given Mapper and parameters.
     * <p>
     * This method is called from the thread-pool thread.
     *
     */
    public void run() {
      try {
        // map pair to output
        MultithreadedMapRunner.this.mapper.map(key, value, output, reporter);
      }
      catch (IOException ex) {
        // If there is an IOException during the call it is set in an instance
        // variable of the MultithreadedMapRunner from where it will be
        // rethrown.
        synchronized (MultithreadedMapRunner.this) {
          if (MultithreadedMapRunner.this.ioException == null) {
            MultithreadedMapRunner.this.ioException = ex;
          }
        }
      }
    }
  }

}
