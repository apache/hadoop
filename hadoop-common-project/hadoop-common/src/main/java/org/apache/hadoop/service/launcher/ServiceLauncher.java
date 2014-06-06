/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service.launcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitCodeProvider;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A class to launch any service by name.
 * <p>
 * It's designed to be subclassed for custom entry points.
 * <p>
 * 
 * Workflow
 * <ol>
 *   <li>An instance of the class is created</li>
 *   <li>If it implements LaunchedService, it is given the binding args off the CLI</li>
 *   <li>Its service.init() and service.start() methods are called.</li>
 *   <li>If it implements LaunchedService, runService() is called and its return
 *   code used as the exit code.</li>
 *   <li>Otherwise: it waits for the service to stop, assuming in its start() method
 *   it begins work</li>
 *   <li>If an exception returned an exit code, that becomes the exit code of the
 *   command.</li>
 * </ol>
 * Error and warning messages are logged to stderr. Why? If the classpath
 * is wrong and logger configurations not on it, then no error messages by
 * the started app will be seen and the caller is left trying to debug
 * using exit codes. 
 * 
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class ServiceLauncher<S extends Service> implements LauncherExitCodes {

  /**
   * Logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      ServiceLauncher.class);

  /**
   * Priority for the shutdown hook: {@value}
   */
  protected static final int SHUTDOWN_PRIORITY = 30;

  /**
   * The name of this class
   */
  public static final String NAME = "ServiceLauncher";

  /**
   * Name of the configuration argument on the CLI: {@value} 
   */
  public static final String ARG_CONF = "--conf";

  /**
   * Usage message.
   * <p>
   * {@value}
   */
  public static final String USAGE_MESSAGE =
      "Usage: " + NAME + " classname " +
      "[" + ARG_CONF + " <conf file>] " +
      "<service arguments> ";

  /**
   * The shutdown time on an interrupt: {@value}
   */
  private static final int SHUTDOWN_TIME_ON_INTERRUPT = 30 * 1000;

  /**
   * The launched service.
   * <p>
   * Invalid until the service has been created. 
   */
  private volatile S service;
  
  /**
   * Exit code of the service.
   * <p>
   * Invalid until a service has
   * executed or stopped, depending on the service type.
   */
  private int serviceExitCode;
  
  /**
   * Exception raised during execution.
   */
  private ExitUtil.ExitException serviceException;

  /**
   * The interrupt escalator for the service.
   */
  private InterruptEscalator interruptEscalator;

  /**
   * Configuration used for the service.
   */
  private Configuration configuration;

  /**
   * Classname for the service to create.
   */
  private String serviceClassName;

  /**
   * Create an instance of the launcher
   * @param serviceClassName classname of the service
   */
  public ServiceLauncher(String serviceClassName) {
    this.serviceClassName = serviceClassName;
  }

  /**
   * Get the service.
   * <p>
   * Null until and unless
   * {@link #coreServiceLaunch(Configuration, List, boolean, boolean)} has completed
   * @return the service
   */
  public S getService() {
    return service;
  }

  /**
   * Setter is to give subclasses the ability to manipulate the service.
   * @param service the new service
   */
  protected void setService(S service) {
    this.service = service;
  }

  /**
   * Get the configuration constructed from the command line arguments.
   * @return the configuration used to create the service
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * The exit code from a successful service execution.
   * @return the exit code. 
   */
  @VisibleForTesting
  public int getServiceExitCode() {
    return serviceExitCode;
  }

  @VisibleForTesting
  public ExitUtil.ExitException getServiceException() {
    return serviceException;
  }

  @Override
  public String toString() {
    return "ServiceLauncher for " + serviceClassName;
  }

  /**
   * Launch the service and exit.
   * <p>
   *   <ol>
   * <li>Parse the command line.</li> 
   * <li>Build the service configuration from it.</li>
   * <li>Start the service.</li>.
   * <li>If it is a {@link LaunchableService}: execute it</li>
   * <li>Otherwise: wait for it to finish.</li>
   * <li>Exit passing the status code to the {@link #exit(int, String)} method.</li>
   * @param args arguments to the service. <code>arg[0]</code> is 
   * assumed to be the service classname.
   */
  public void launchServiceAndExit(List<String> args) {

    registerFailureHandling();
    // Currently the config just the default
    Configuration conf = createConfiguration();
    List<String> processedArgs = extractConfigurationArgs(conf, args);
    ExitUtil.ExitException ee = launchService(conf, processedArgs, true, true);
    exit(ee);
  }

  /**
   * Override point: create the base configuration for the service.
   * <p>
   * Subclasses can override to create HDFS/YARN configurations etc.
   * @return the configuration to use as the service initalizer.
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Launch a service catching all exceptions and downgrading them to exit codes
   * after logging.
   * <p>
   * Sets {@link #serviceException} to this value.
   * @param conf configuration to use
   * @param processedArgs command line after the launcher-specific arguments have
   * been stripped out.
   * @param addShutdownHook should a shutdown hook be added to terminate
   * this service on shutdown. Tests should set this to false.
   * @param execute execute/wait for the service to stop.
   * @return an exit exception, which will have a status code of 0 if it worked
   */
  @VisibleForTesting
  public ExitUtil.ExitException launchService(Configuration conf,
      List<String> processedArgs,
      boolean addShutdownHook,
      boolean execute) {
    
    ExitUtil.ExitException exitException;
    
    try {
      int exitCode = coreServiceLaunch(conf, processedArgs, addShutdownHook,
          execute);
      if (service != null) {
        //check to see if the service failed
        Throwable failure = service.getFailureCause();
        if (failure != null) {
          //the service exited with a failure.
          //check what state it is in
          Service.STATE failureState = service.getFailureState();
          if (failureState == Service.STATE.STOPPED) {
            //the failure occurred during shutdown, not important enough to bother
            //the user as it may just scare them
            LOG.debug("Failure during shutdown: {} ", failure, failure);
          } else {
            //throw it for the catch handlers to deal with
            throw failure;
          }
        }
      }
      String serviceName = getServiceName();

      if (exitCode == 0) {
        exitException = new ServiceLaunchException(exitCode,
            "%s succeeded",
            serviceName);
      } else {
        exitException = new ServiceLaunchException(exitCode,
            "%s failed ", serviceName);
      }
      // either the service succeeded, or an error raised during shutdown, 
      // which we don't worry that much about
    } catch (ExitUtil.ExitException ee) {
      // exit exceptions are passed through unchanged
      exitException = ee;
    } catch (Throwable thrown) {
      exitException = convertToExitException(thrown);
    }
    serviceExitCode = exitException.getExitCode();
    serviceException = exitException;
    return exitException;
  }

  /**
   * Launch the service.
   * <p>
   *
   * All exceptions that occur are propagated upwards.
   * <p>
   * If the method returns a status code, it means that it got as far starting
   * the service, and if it implements {@link LaunchableService}, that the 
   * method {@link LaunchableService#execute()} has completed. 
   *<p>
   * After this method returns, the service can be retrieved returned by {@link #getService()}.
   *<p>
   * @param conf configuration
   * @param processedArgs arguments after the configuration parameters
   * have been stripped out.
   * @param addShutdownHook should a shutdown hook be added to terminate
   * this service on shutdown. Tests should set this to false.
   * @param execute execute/wait for the service to stop
   * @throws ClassNotFoundException classname not on the classpath
   * @throws IllegalAccessException not allowed at the class
   * @throws InstantiationException not allowed to instantiate it
   * @throws InterruptedException thread interrupted
   * @throws ExitUtil.ExitException any exception defining the status code.
   * @throws Exception any other failure -if it implements {@link ExitCodeProvider}
   * then it defines the exit code for any containing exception
   */

  protected int coreServiceLaunch(Configuration conf,
      List<String> processedArgs,
      boolean addShutdownHook,
      boolean execute) throws Exception {

    // create the service instance
    instantiateService(conf);
    ServiceShutdownHook shutdownHook = null;

    // and the shutdown hook if requested
    if (addShutdownHook) {
      shutdownHook = new ServiceShutdownHook(service);
      shutdownHook.register(SHUTDOWN_PRIORITY);
    }
    String serviceName = getServiceName();
    LOG.debug("Launched service {}", serviceName);
    LaunchableService launchableService = null;

    if (service instanceof LaunchableService) {
      // it's a launchedService, pass in the conf and arguments before init)
      LOG.debug("Service {} implements LaunchedService", serviceName);
      launchableService = (LaunchableService) service;
      if (launchableService.isInState(Service.STATE.INITED)) {
        LOG.warn("LaunchedService {}"
                 + " initialized in constructor before CLI arguments passed in",
            serviceName);
      }
      Configuration newconf = launchableService.bindArgs(configuration, processedArgs);
      if (newconf != null) {
        configuration = newconf;
      }
    }

    //some class constructors init; here this is picked up on.
    if (!service.isInState(Service.STATE.INITED)) {
      service.init(configuration);
    }
    int exitCode;

    try {
      // start the service
      service.start();
      exitCode = EXIT_SUCCESS;
      if (execute && service.isInState(Service.STATE.STARTED) ) {
        if (launchableService != null) {
          // assume that runnable services are meant to run from here
          try {
            exitCode = launchableService.execute();
            LOG.debug("Service {} execution returned exit code {}", serviceName, exitCode);
          } finally {
            // then stop the service
            service.stop();
          }
        } else {
          //run the service until it stops or an interrupt happens on a different thread.
          LOG.debug("waiting for service threads to terminate");
          service.waitForServiceToStop(0);
        }
      }
    } finally {
      if (shutdownHook != null) {
        shutdownHook.unregister();
      }
    }
    return exitCode;
  }

  /**
   * Instantiate the service defined in <code>serviceClassName</code>.
   * <p>
   * Sets the <code>configuration</code> field
   * to the the value of <code>conf</code>,
   * and the <code>service</code> field to the service created.
   *
   * @param conf configuration to use
   */
  public Service instantiateService(Configuration conf) {
    Preconditions.checkArgument(conf != null, "null conf");
    configuration = conf;

    //Instantiate the class -this requires the service to have a public
    // zero-argument constructor
    Object instance;
    
    // in Java7+ the exception catch logic should be consolidated
    try {
      Class<?> serviceClass = getClassLoader().loadClass(serviceClassName);
      try {
        instance = serviceClass.getConstructor().newInstance();
      } catch (NoSuchMethodException noEmptyConstructor) {
        // no simple constructor, fall back to a string
        LOG.debug("No empty constructor {}", noEmptyConstructor,
            noEmptyConstructor);
        instance = serviceClass.getConstructor(String.class).newInstance(
            serviceClassName);
      }
    } catch (ClassNotFoundException e) {
      throw serviceCreationFailure(e);
    } catch (InstantiationException e) {
      throw serviceCreationFailure(e);
    } catch (IllegalAccessException e) {
      throw serviceCreationFailure(e);
    } catch (IllegalArgumentException e) {
      throw serviceCreationFailure(e);
    } catch (InvocationTargetException e) {
      throw serviceCreationFailure(e);
    } catch (NoSuchMethodException e) {
      throw serviceCreationFailure(e);
    } catch (SecurityException e) {
      throw serviceCreationFailure(e);
    }
    if (!(instance instanceof Service)) {
      //not a service
      throw new ServiceLaunchException(
          LauncherExitCodes.EXIT_SERVICE_CREATION_FAILURE,
          "Not a service class: \"%s\"", serviceClassName);
    }

    // cast to the specific instance type of this ServiceLauncher
    service = (S) instance;
    return service;
  }

  /**
   * Convert an exception to an <code>ExitException</code>.
   * <p>
   * This process may just be a simple pass through, otherwise a new
   * exception is created with an exit code, the text of the supplied
   * exception, and the supplied exception as an inner cause.
   * 
   * <ol>
   *   <li>If is already the right type, pass it through.</li>
   *   <li>If it implements {@link ExitCodeProvider#getExitCode()},
   *   the exit code is extracted and used in the new exception.</li>
   *   <li>Otherwise, the exit code
   *   {@link LauncherExitCodes#EXIT_EXCEPTION_THROWN} is used.</li>
   * </ol>
   *  
   * @param thrown the exception thrown
   * @return an <code>ExitException</code> with a status code
   */
  protected ExitUtil.ExitException convertToExitException(Throwable thrown) {
    ExitUtil.ExitException
        exitException;// other exceptions are converted to ExitExceptions
    // get the exception message
    String message = thrown.toString();
    int exitCode;
    if (thrown instanceof ExitCodeProvider) {
      // the exception provides a status code -extract it
      exitCode = ((ExitCodeProvider) thrown).getExitCode();
      message = thrown.getMessage();
    } else {
      // no exception code: use the default
      exitCode = EXIT_EXCEPTION_THROWN;
    }
    // construct the new exception with the original message and
    // an exit code
    exitException = new ServiceLaunchException(exitCode, message);
    exitException.initCause(thrown);
    return exitException;
  }

  /**
   * Generate an exception announcing a failure to create the service.
   * @param exception inner exception.
   * @return a new exception, with the exit code
   * {@link LauncherExitCodes#EXIT_SERVICE_CREATION_FAILURE}
   */
  protected ServiceLaunchException serviceCreationFailure(Exception exception) {
    return new ServiceLaunchException(EXIT_SERVICE_CREATION_FAILURE, exception);
  }
  
  /**
   * Register this class as the handler for the control-C interrupt.
   * <p>
   * Subclasses can extend this with extra operations, such as
   * an exception handler:
   * <pre>
   *  Thread.setDefaultUncaughtExceptionHandler(
   *     new YarnUncaughtExceptionHandler());
   * </pre>
   */
  protected void registerFailureHandling() {
    try {
      interruptEscalator = new InterruptEscalator(this, SHUTDOWN_TIME_ON_INTERRUPT);
      interruptEscalator.register(IrqHandler.CONTROL_C);
      interruptEscalator.register(IrqHandler.SIGTERM);
    } catch (IllegalArgumentException e) {
      // downgrade interrupt registration to warnings
      LOG.warn("{}", e, e);
    }
  }

  /**
   * Get the service name via {@link Service#getName()}.
   * <p>
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
   * Print a warning message.
   * <p>
   *   This tries to log to the log's warn() operation.
   *   If the log at that level is disabled it logs to system error
   * @param text warning text
   */
  protected void warn(String text) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(text);
    } else {
      System.err.println(text);
    }
  }

  /**
   * Report an error. 
   * <p>
   * This tries to log to the log's <code>error()</code> operation.
   * If disabled the message is logged to system error along
   * with <code>thrown.toString()</code>
   * @param message message for the user
   * @param thrown the exception thrown
   */
  protected void error(String message, Throwable thrown) {
    String text = "Exception: " + message;
    if (LOG.isErrorEnabled()) {
      LOG.error(text, thrown);
    } else {
      System.err.println(text);
      if (thrown != null) {
        System.err.println(thrown.toString());
      }
    }
  }
  
  /**
   * Exit the JVM.
   * <p>
   * This is method can be overridden for testing, throwing an 
   * exception instead. Any subclassed method MUST raise an 
   * <code>ExitException</code> instance/subclass.
   * The service launcher code assumes that after this method is invoked,
   * no other code in the same method is called.
   * @param exitCode code to exit
   */
  protected void exit(int exitCode, String message) {
    ExitUtil.terminate(exitCode, message);
  }

  /**
   * Exit the JVM using an exception for the exit code and message.
   * <p>
   * This is the standard way a launched service exits.
   * An error code of 0 means success -nothing is printed.
   * <p>
   * By default, calls
   * {@link ExitUtil#terminate(ExitUtil.ExitException)}.
   * <p>
   * This can be subclassed for testing
   * @param ee exit exception
   */
  protected void exit(ExitUtil.ExitException ee) {
    ExitUtil.terminate(ee);
  }

  /**
   * Override point: get the classloader to use.
   * @return the classloader for loading a service class.
   */
  protected ClassLoader getClassLoader() {
    return this.getClass().getClassLoader();
  }

  /**
   * Extract the configuration arguments and apply them to the configuration,
   * building an array of processed arguments to hand down to the service.
   * <p>
   * @param conf configuration to update.
   * @param args main arguments. <code>args[0]</code>is assumed to be the service
   * classname and is skipped.
   * @return the processed list.
   * @throws ExitUtil.ExitException if JVM exiting is disabled.
   */
  public List<String> extractConfigurationArgs(Configuration conf,
      List<String> args) {

    //convert args to a list
    int size = args.size();
    if (size <= 1 ) {
      return new ArrayList<String>(0);
    }
    List<String> argsList = new ArrayList<String>(size);
    //skip that first entry
    int index = 1;
    while (index< size) {
      String arg = args.get(index);
      LOG.info("arg[{}]={}", index, arg);
      if (arg.equals(ARG_CONF)) {
        //the argument is a --conf file tuple: extract the path and load
        //it in as a configuration resource.

        //increment the loop iterator
        index++;
        if (index == size) {
          //overshot the end of the file
          exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
              ARG_CONF + ": missing configuration file");
          // never called, but retained for completeness
          break;
        }

        String filename = args.get(index);
        index++;
        LOG.info("arg[{}] = Conf file {}",index, filename);
        URL fileURL = extractConfFile(filename);
        conf.addResource(fileURL);
      } else {
        index++;
        argsList.add(arg);
      }
    }
    return argsList;
  }

  /**
   * Extract a {@link Configuration} file from the filename argument. 
   * <p>
   * This includes tests that the file exists and is a valid
   * XML Configuration file.
   * <p>
   * If the load fails for any reason, the JVM is exited with an error code
   * {@link LauncherExitCodes#EXIT_COMMAND_ARGUMENT_ERROR} and a message.
   * @param filename filename argument
   * @return the URL to the file.
   */
  private URL extractConfFile(String filename)  {
    File file = new File(filename);
    URL fileURL = null;
    if (!file.exists()) {
      exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
          ARG_CONF + ": configuration file not found: " + file);
      // never called, but retained for completeness
    } else {
      try {
        Configuration c = new Configuration(false);
        fileURL = file.toURI().toURL();
        c.addResource(fileURL);
        c.get("key");
      } catch (Exception e) {
        // this means that the file could not be parsed
        exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
            ARG_CONF + ": configuration file not loadable: " + file);
      }
    }
    return fileURL;

  }

  /**
   * Build a log message for starting up and shutting down. 
   * @param classname the class of the server
   * @param args arguments
   */
  public static String startupShutdownMessage(String classname,
                                              List<String> args) {
    final String hostname = NetUtils.getHostname();

    return StringUtils.createStartupShutdownMessage(classname, hostname,
        args.toArray(new String[args.size()]));
  }

  /**
   * Exit with a printed message. 
   * @param status status code
   * @param message message
   * @throws ExitUtil.ExitException if exceptions are disabled
   */
  private static void exitWithMessage(int status, String message) {
    ExitUtil.terminate(new ServiceLaunchException(status, message));
  }

  /**
   * This is the JVM entry point for the service launcher.
   * <p>
   * Converts the arguments to a list, then invokes {@link #serviceMain(List)}
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    List<String> argsList = Arrays.asList(args);
    serviceMain(argsList);
  }

  /**
   * Varargs version of the entry point for testing and other in-JVM use
   * -hands off to {@link #serviceMain(List)}
   * @param args command line arguments.
   */
  public static void serviceMain(String ... args) {
    List<String> argsList = Arrays.asList(args);
    serviceMain(argsList);
  }
    
  /* ====================================================================== */
  /**
   * The real main function, which takes the arguments as a list.
   * <p>
   * Argument 0 MUST be the service classname
   * @param argsList the list of arguments
   */
  /* ====================================================================== */

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

      ServiceLauncher<Service> serviceLauncher =
          new ServiceLauncher<Service>(serviceClassName);
      serviceLauncher.launchServiceAndExit(argsList);
    }
  }

}
