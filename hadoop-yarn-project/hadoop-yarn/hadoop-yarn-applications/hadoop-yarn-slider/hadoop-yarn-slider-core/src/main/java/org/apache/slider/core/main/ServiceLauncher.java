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

package org.apache.slider.core.main;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class to launch any service by name.
 * 
 * It's designed to be subclassed for custom entry points.
 * 
 * 
 * Workflow
 * <ol>
 *   <li>An instance of the class is created</li>
 *   <li>If it implements RunService, it is given the binding args off the CLI</li>
 *   <li>Its service.init() and service.start() methods are called.</li>
 *   <li>If it implements RunService, runService() is called and its return
 *   code used as the exit code.</li>
 *   <li>Otherwise: it waits for the service to stop, assuming in its start() method
 *   it begins work</li>
 *   <li>If an exception returned an exit code, that becomes the exit code of the
 *   command.</li>
 * </ol>
 * Error and warning messages are logged to stderr. Why? If the classpath
 * is wrong & logger configurations not on it, then no error messages by
 * the started app will be seen and the caller is left trying to debug
 * using exit codes. 
 * 
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class ServiceLauncher<S extends Service>
  implements LauncherExitCodes, IrqHandler.Interrupted,
    Thread.UncaughtExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
      ServiceLauncher.class);

  protected static final int PRIORITY = 30;

  public static final String NAME = "ServiceLauncher";

  /**
   * Name of the "--conf" argument. 
   */
  public static final String ARG_CONF = "--conf";

  public static final String USAGE_MESSAGE =
      "Usage: " + NAME + " classname [" + ARG_CONF +
      "<conf file>] <service arguments> | ";
  static final int SHUTDOWN_TIME_ON_INTERRUPT = 30 * 1000;

  private volatile S service;
  private int serviceExitCode;
  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
  private final List<IrqHandler> interruptHandlers = new ArrayList<>(1);
  private Configuration configuration;
  private String serviceClassName;
  private static AtomicBoolean signalAlreadyReceived = new AtomicBoolean(false);


  /**
   * Create an instance of the launcher
   * @param serviceClassName classname of the service
   */
  public ServiceLauncher(String serviceClassName) {
    this.serviceClassName = serviceClassName;
  }

  /**
   * Get the service. Null until and unless
   * {@link #launchService(Configuration, String[], boolean)} has completed
   * @return the service
   */
  public S getService() {
    return service;
  }

  /**
   * Get the configuration constructed from the command line arguments
   * @return the configuration used to create the service
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * The exit code from a successful service execution
   * @return the exit code. 
   */
  public int getServiceExitCode() {
    return serviceExitCode;
  }

  @Override
  public String toString() {
    return "ServiceLauncher for " + serviceClassName;
  }

  /**
   * Launch the service, by creating it, initing it, starting it and then
   * maybe running it. {@link RunService#bindArgs(Configuration, String...)} is invoked
   * on the service between creation and init.
   *
   * All exceptions that occur are propagated upwards.
   *
   * If the method returns a status code, it means that it got as far starting
   * the service, and if it implements {@link RunService}, that the 
   * method {@link RunService#runService()} has completed. 
   *
   * At this point, the service is returned by {@link #getService()}.
   *
   * @param conf configuration
   * @param processedArgs arguments after the configuration parameters
   * have been stripped out.
   * @param addProcessHooks should process failure handlers be added to
   * terminate this service on shutdown. Tests should set this to false.
   * @throws ClassNotFoundException classname not on the classpath
   * @throws IllegalAccessException not allowed at the class
   * @throws InstantiationException not allowed to instantiate it
   * @throws InterruptedException thread interrupted
   * @throws Throwable any other failure
   */
  public int launchService(Configuration conf,
      String[] processedArgs,
      boolean addProcessHooks)
    throws Throwable {

    instantiateService(conf);

    // add any process shutdown hooks
    if (addProcessHooks) {
      ServiceShutdownHook shutdownHook = new ServiceShutdownHook(service);
      ShutdownHookManager.get().addShutdownHook(shutdownHook, PRIORITY);
    }
    RunService runService = null;

    if (service instanceof RunService) {
      //if its a runService, pass in the conf and arguments before init)
      runService = (RunService) service;
      configuration = runService.bindArgs(configuration, processedArgs);
      Preconditions.checkNotNull(configuration,
          "null configuration returned by bindArgs()");
    }

    //some class constructors init; here this is picked up on.
    if (!service.isInState(Service.STATE.INITED)) {
      service.init(configuration);
    }
    service.start();
    int exitCode = EXIT_SUCCESS;
    if (runService != null) {
      //assume that runnable services are meant to run from here
      exitCode = runService.runService();
      LOG.debug("Service exited with exit code {}", exitCode);

    } else {
      //run the service until it stops or an interrupt happens on a different thread.
      LOG.debug("waiting for service threads to terminate");
      service.waitForServiceToStop(0);
    }
    //exit
    serviceExitCode = exitCode;
    return serviceExitCode;
  }

  /**
   * Instantiate the service defined in <code>serviceClassName</code>
   * . Sets the <code>configuration</code> field
   * to the configuration, and <code>service</code> to the service.
   *
   * @param conf configuration to use
   * @throws ClassNotFoundException classname not on the classpath
   * @throws IllegalAccessException not allowed at the class
   * @throws InstantiationException not allowed to instantiate it
   */
  @SuppressWarnings("unchecked")
  public Service instantiateService(Configuration conf)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException,
      ExitUtil.ExitException, NoSuchMethodException, InvocationTargetException {
    Preconditions.checkArgument(conf != null, "null conf");
    configuration = conf;

    //Instantiate the class -this requires the service to have a public
    // zero-argument constructor
    Class<?> serviceClass =
        this.getClass().getClassLoader().loadClass(serviceClassName);
    Object instance = serviceClass.getConstructor().newInstance();
    if (!(instance instanceof Service)) {
      //not a service
      throw new ExitUtil.ExitException(EXIT_COMMAND_ARGUMENT_ERROR,
          "Not a Service class: " + serviceClassName);
    }

    service = (S) instance;
    return service;
  }

  /**
   * Register this class as the handler for the control-C interrupt.
   * Can be overridden for testing.
   */
  protected void registerInterruptHandler() {
    try {
      interruptHandlers.add(new IrqHandler(IrqHandler.CONTROL_C, this));
      interruptHandlers.add(new IrqHandler(IrqHandler.SIGTERM, this));
    } catch (IOException e) {
      error("Signal handler setup failed : {}" + e, e);
    }
  }

  /**
   * The service has been interrupted -try to shut down the service.
   * Give the service time to do this before the exit operation is called 
   * @param interruptData the interrupted data.
   */
  @Override
  public void interrupted(IrqHandler.InterruptData interruptData) {
    String message = "Service interrupted by " + interruptData.toString();
    warn(message);
    if (!signalAlreadyReceived.compareAndSet(false, true)) {
      warn("Repeated interrupt: escalating to a JVM halt");
      // signal already received. On a second request to a hard JVM
      // halt and so bypass any blocking shutdown hooks.
      ExitUtil.halt(EXIT_INTERRUPTED, message);
    }
    int shutdownTimeMillis = SHUTDOWN_TIME_ON_INTERRUPT;
    //start an async shutdown thread with a timeout
    ServiceForcedShutdown forcedShutdown =
      new ServiceForcedShutdown(shutdownTimeMillis);
    Thread thread = new Thread(forcedShutdown);
    thread.setDaemon(true);
    thread.start();
    //wait for that thread to finish
    try {
      thread.join(shutdownTimeMillis);
    } catch (InterruptedException ignored) {
      //ignored
    }
    if (!forcedShutdown.isServiceStopped()) {
      warn("Service did not shut down in time");
    }
    exit(EXIT_INTERRUPTED, message);
  }

  /**
   * Uncaught exception handler.
   * If an error is raised: shutdown
   * The state of the system is unknown at this point -attempting
   * a clean shutdown is dangerous. Instead: exit
   * @param thread thread that failed
   * @param exception exception
   */
  @Override
  public void uncaughtException(Thread thread, Throwable exception) {
    if (ShutdownHookManager.get().isShutdownInProgress()) {
      LOG.error("Thread {} threw an error during shutdown: {}.",
          thread.toString(),
          exception,
          exception);
    } else if (exception instanceof Error) {
      try {
        LOG.error("Thread {} threw an error: {}. Shutting down",
            thread.toString(),
            exception,
            exception);
      } catch (Throwable err) {
        // We don't want to not exit because of an issue with logging
      }
      if (exception instanceof OutOfMemoryError) {
        // After catching an OOM java says it is undefined behavior, so don't
        // even try to clean up or we can get stuck on shutdown.
        try {
          System.err.println("Halting due to Out Of Memory Error...");
        } catch (Throwable err) {
          // Again we don't want to exit because of logging issues.
        }
        ExitUtil.halt(EXIT_EXCEPTION_THROWN);
      } else {
        // error other than OutOfMemory
        exit(convertToExitException(exception));
      }
    } else {
      // simple exception in a thread. There's a policy decision here:
      // terminate the service vs. keep going after a thread has failed
      LOG.error("Thread {} threw an exception: {}",
          thread.toString(),
          exception,
          exception);
    }
  }

  /**
   * Print a warning: currently this goes to stderr
   * @param text
   */
  protected void warn(String text) {
    System.err.println(text);
  }

  /**
   * Report an error. The message is printed to stderr; the exception
   * is logged via the current logger.
   * @param message message for the user
   * @param thrown the exception thrown
   */
  protected void error(String message, Throwable thrown) {
    String text = "Exception: " + message;
    warn(text);
    LOG.error(text, thrown);
  }

  /**
   * Exit the code.
   * This is method can be overridden for testing, throwing an 
   * exception instead. Any subclassed method MUST raise an 
   * <code>ExitUtil.ExitException</code> instance.
   * The service launcher code assumes that after this method is invoked,
   * no other code in the same method is called.
   * @param exitCode code to exit
   */
  protected void exit(int exitCode, String message) {
    ExitUtil.terminate(exitCode, message);
  }

  /**
   * Exit off an exception. This can be subclassed for testing
   * @param ee exit exception
   */
  protected void exit(ExitUtil.ExitException ee) {
    ExitUtil.terminate(ee.status, ee);
  }

  /**
   * Get the service name via {@link Service#getName()}.
   * If the service is not instantiated, the classname is returned instead.
   * @return the service name
   */
  public String getServiceName() {
    Service s = service;
    String name = null;
    if (s != null) {
      try {
        name = s.getName();
      } catch (Exception ignored) {
        // ignored
      }
    }
    if (name != null) {
      return "service " + name;
    } else {
      return "service classname " + serviceClassName;
    }
  }

  /**
   * Parse the command line, building a configuration from it, then
   * launch the service and wait for it to finish. finally, exit
   * passing the status code to the #exit(int) method.
   * @param args arguments to the service. arg[0] is 
   * assumed to be the service classname and is automatically
   */
  public void launchServiceAndExit(List<String> args) {

    registerInterruptHandler();
    //Currently the config just the default
    Configuration conf = new Configuration();
    String[] processedArgs = extractConfigurationArgs(conf, args);
    ExitUtil.ExitException ee = launchServiceRobustly(conf, processedArgs);
    System.out.flush();
    System.err.flush();
    exit(ee);
  }

  /**
   * Extract the configuration arguments and apply them to the configuration,
   * building an array of processed arguments to hand down to the service.
   *
   * @param conf configuration to update
   * @param args main arguments. args[0] is assumed to be the service
   * classname and is skipped
   * @return the processed list.
   */
  public static String[] extractConfigurationArgs(Configuration conf,
                                                  List<String> args) {

    //convert args to a list
    int argCount = args.size();
    if (argCount <= 1 ) {
      return new String[0];
    }
    List<String> argsList = new ArrayList<String>(argCount);
    ListIterator<String> arguments = args.listIterator();
    //skip that first entry
    arguments.next();
    while (arguments.hasNext()) {
      String arg = arguments.next();
      if (arg.equals(ARG_CONF)) {
        //the argument is a --conf file tuple: extract the path and load
        //it in as a configuration resource.

        //increment the loop iterator
        if (!arguments.hasNext()) {
          //overshot the end of the file
          exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
              ARG_CONF + ": missing configuration file after ");
        }
        File file = new File(arguments.next());
        if (!file.exists()) {
          exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
              ARG_CONF + ": configuration file not found: " + file);
        }
        try {
          conf.addResource(file.toURI().toURL());
        } catch (MalformedURLException e) {
          LOG.debug("File {} cannot be converted to URL", file, e);
          exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
              ARG_CONF + ": configuration file path invalid: " + file);
        }
      } else {
        argsList.add(arg);
      }
    }
    String[] processedArgs = new String[argsList.size()];
    argsList.toArray(processedArgs);
    return processedArgs;
  }

  /**
   * Launch a service catching all exceptions and downgrading them to exit codes
   * after logging.
   * @param conf configuration to use
   * @param processedArgs command line after the launcher-specific arguments have
   * been stripped out
   * @return an exit exception, which will have a status code of 0 if it worked
   */
  public ExitUtil.ExitException launchServiceRobustly(Configuration conf,
                                   String[] processedArgs) {
    ExitUtil.ExitException exitException;
    try {
      int exitCode = launchService(conf, processedArgs, true);
      if (service != null) {
        Throwable failure = service.getFailureCause();
        if (failure != null) {
          //the service exited with a failure.
          //check what state it is in
          Service.STATE failureState = service.getFailureState();
          if (failureState == Service.STATE.STOPPED) {
            //the failure occurred during shutdown, not important enough to bother
            //the user as it may just scare them
            LOG.debug("Failure during shutdown:{} ", failure, failure);
          } else {
            //throw it for the catch handlers to deal with
            throw failure;
          }
        }
      }
      exitException = new ExitUtil.ExitException(exitCode,
                                     "In " + serviceClassName);
      // either the service succeeded, or an error raised during shutdown, 
      // which we don't worry that much about
    } catch (ExitUtil.ExitException ee) {
      exitException = ee;
    } catch (Throwable thrown) {
      exitException = convertToExitException(thrown);
    }
    return exitException;
  }

  /**
   * Convert the exception to one that can be handed off to ExitUtils;
   * if it is of the write type it is passed throw as is. If not, a 
   * new exception with the exit code {@link #EXIT_EXCEPTION_THROWN}
   * is created, with the argument <code>thrown</code> as the inner cause
   * @param thrown the exception thrown
   * @return an exception to terminate the process with
   */
  protected ExitUtil.ExitException convertToExitException(Throwable thrown) {
    ExitUtil.ExitException exitException;
    int exitCode;
    String message = thrown.getMessage();
    if (message == null) {
      message = thrown.toString();
    }
    if (thrown instanceof ExitCodeProvider) {
      exitCode = ((ExitCodeProvider) thrown).getExitCode();
      if (LOG.isDebugEnabled()) {
        LOG.debug("While running {}: {}", getServiceName(), message, thrown);
      }
      LOG.error(message);
    } else {
      // not any of the service launcher exceptions -assume something worse
      error(message, thrown);
      exitCode = EXIT_EXCEPTION_THROWN;
    }
    exitException = new ExitUtil.ExitException(exitCode, message);
    exitException.initCause(thrown);
    return exitException;
  }


  /**
   * Build a log message for starting up and shutting down. 
   * This was grabbed from the ToolRunner code.
   * @param classname the class of the server
   * @param args arguments
   */
  public static String startupShutdownMessage(String classname,
                                              List<String> args) {
    final String hostname = NetUtils.getHostname();

    return toStartupShutdownString("STARTUP_MSG: ", new String[]{
      "Starting " + classname,
      "  host = " + hostname,
      "  args = " + args,
      "  version = " + VersionInfo.getVersion(),
      "  classpath = " + System.getProperty("java.class.path"),
      "  build = " + VersionInfo.getUrl() + " -r "
      + VersionInfo.getRevision()
      + "; compiled by '" + VersionInfo.getUser()
      + "' on " + VersionInfo.getDate(),
      "  java = " + System.getProperty("java.version")
    });
  }

  /**
   * Exit with a printed message
   * @param status status code
   * @param message message
   */
  private static void exitWithMessage(int status, String message) {
    System.err.println(message);
    ExitUtil.terminate(status);
  }

  private static String toStartupShutdownString(String prefix, String[] msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for (String s : msg) {
      b.append("\n").append(prefix).append(s);
    }
    b.append("\n************************************************************/");
    return b.toString();
  }

  /**
   * forced shutdown runnable.
   */
  protected class ServiceForcedShutdown implements Runnable {

    private final int shutdownTimeMillis;
    private boolean serviceStopped;

    public ServiceForcedShutdown(int shutdownTimeoutMillis) {
      this.shutdownTimeMillis = shutdownTimeoutMillis;
    }

    @Override
    public void run() {
      if (service != null) {
        service.stop();
        serviceStopped = service.waitForServiceToStop(shutdownTimeMillis);
      } else {
        serviceStopped = true;
      }
    }

    private boolean isServiceStopped() {
      return serviceStopped;
    }
  }

  /**
   * The real main function, which takes the arguments as a list
   * arg 0 must be the service classname
   * @param argsList the list of arguments
   */
  public static void serviceMain(List<String> argsList) {
    if (argsList.isEmpty()) {
      exitWithMessage(EXIT_USAGE, USAGE_MESSAGE);
    } else {
      String serviceClassName = argsList.get(0);

      if (LOG.isDebugEnabled()) {
        LOG.debug(startupShutdownMessage(serviceClassName, argsList));
        StringBuilder builder = new StringBuilder();
        for (String arg : argsList) {
          builder.append('"').append(arg).append("\" ");
        }
        LOG.debug(builder.toString());
      }
      Thread.setDefaultUncaughtExceptionHandler(
        new YarnUncaughtExceptionHandler());

      ServiceLauncher serviceLauncher = new ServiceLauncher<>(serviceClassName);
      serviceLauncher.launchServiceAndExit(argsList);
    }
  }

  /**
   * This is the main entry point for the service launcher.
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    List<String> argsList = Arrays.asList(args);
    serviceMain(argsList);
  }
}
