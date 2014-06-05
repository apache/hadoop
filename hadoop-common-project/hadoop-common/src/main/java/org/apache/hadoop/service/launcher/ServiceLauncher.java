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
import org.apache.hadoop.util.VersionInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A class to launch any service by name.
 * 
 * It's designed to be subclassed for custom entry points.
 * 
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
 * is wrong & logger configurations not on it, then no error messages by
 * the started app will be seen and the caller is left trying to debug
 * using exit codes. 
 * 
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class ServiceLauncher<S extends Service> implements LauncherExitCodes {
  private static final Logger LOG = LoggerFactory.getLogger(
      ServiceLauncher.class);

  protected static final int SHUTDOWN_PRIORITY = 30;

  public static final String NAME = "ServiceLauncher";

  /**
   * Name of the "--conf" argument. 
   */
  public static final String ARG_CONF = "--conf";

  /**
   * Usage message
   */
  public static final String USAGE_MESSAGE =
      "Usage: " + NAME + " classname " +
      "[" + ARG_CONF + " <conf file>] " +
      "<service arguments> ";
  private static final int SHUTDOWN_TIME_ON_INTERRUPT = 30 * 1000;

  /**
   * The launched service
   */
  private volatile S service;
  
  /**
   * Exit code
   */
  private int serviceExitCode;
  
  /**
   * Exception raised during execution
   */
  private ExitUtil.ExitException serviceException;

  /**
   * The interrupt escalator for the servie
   */
  private InterruptEscalator<S> interruptEscalator;

  /**
   * Configuration used for the service
   */
  private Configuration configuration;

  /**
   * Classname for the service to create
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
   * Get the service. Null until and unless
   * {@link #coreServiceLaunch(Configuration, List, boolean, boolean)} has completed
   * @return the service
   */
  public S getService() {
    return service;
  }

  /**
   * Setter is to give subclasses the ability to manipulate the service
   * @param service the new service
   */
  protected void setService(S service) {
    this.service = service;
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
   * Parse the command line, building a configuration from it, then
   * launch the service and wait for it to finish. finally, exit
   * passing the status code to the #exit(int) method.
   * @param args arguments to the service. arg[0] is 
   * assumed to be the service classname and is automatically
   */
  public void launchServiceAndExit(List<String> args) {

    registerFailureHandling();
    //Currently the config just the default
    Configuration conf = createConfiguration();
    List<String> processedArgs = extractConfigurationArgs(conf, args);
    ExitUtil.ExitException ee = launchService(conf, processedArgs, true, true);
    exit(ee);
  }

  /**
   * Override point: create the base configuration for the service.
   * Subclasses can override to create HDFS/YARN configurations &c.
   * @return the configuration to use as the service initalizer.
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Launch a service catching all exceptions and downgrading them to exit codes
   * after logging. Sets {@link #serviceException} to this value
   * @param conf configuration to use
   * @param processedArgs command line after the launcher-specific arguments have
   * been stripped out
   * @param addShutdownHook should a shutdown hook be added to terminate
   * this service on shutdown. Tests should set this to false.
   * @param execute execute/wait for the service to stop
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
   * Launch the service, by creating it, initing it, starting it and then
   * maybe running it. {@link LaunchedService#bindArgs(Configuration, List)} is invoked
   * on the service between creation and init.
   *
   * All exceptions that occur are propagated upwards.
   *
   * If the method returns a status code, it means that it got as far starting
   * the service, and if it implements {@link LaunchedService}, that the 
   * method {@link LaunchedService#execute()} has completed. 
   *
   * At this point, the service is returned by {@link #getService()}.
   *
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

    // and the shutdown hook if requested
    if (addShutdownHook) {
      ServiceShutdownHook shutdownHook = new ServiceShutdownHook(service);
      shutdownHook.register(SHUTDOWN_PRIORITY);
    }
    String serviceName = getServiceName();
    LOG.debug("Launched service {}", serviceName);
    LaunchedService launchedService = null;

    if (service instanceof LaunchedService) {
      // it's a launchedService, pass in the conf and arguments before init)
      LOG.debug("Service {} implements LaunchedService", serviceName);
      launchedService = (LaunchedService) service;
      if (launchedService.isInState(Service.STATE.INITED)) {
        LOG.warn("LaunchedService {}"
                 + " initialized in constructor before CLI arguments passed in",
            serviceName);
      }
      Configuration newconf = launchedService.bindArgs(configuration, processedArgs);
      if (newconf != null) {
        configuration = newconf;
      }
    }

    //some class constructors init; here this is picked up on.
    if (!service.isInState(Service.STATE.INITED)) {
      service.init(configuration);
    }
    
    // start the service
    service.start();
    int exitCode = EXIT_SUCCESS;
    if (execute) {
      if (launchedService != null) {
        // assume that runnable services are meant to run from here
        try {
          exitCode = launchedService.execute();
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
    return exitCode;
  }

  /**
   * Instantiate the service defined in <code>serviceClassName</code>.
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
      throw serviceCreationFailure(serviceClassName, e);
    } catch (InstantiationException e) {
      throw serviceCreationFailure(serviceClassName, e);
    } catch (IllegalAccessException e) {
      throw serviceCreationFailure(serviceClassName, e);
    } catch (IllegalArgumentException e) {
      throw serviceCreationFailure(serviceClassName, e);
    } catch (InvocationTargetException e) {
      throw serviceCreationFailure(serviceClassName, e);
    } catch (NoSuchMethodException e) {
      throw serviceCreationFailure(serviceClassName, e);
    } catch (SecurityException e) {
      throw serviceCreationFailure(serviceClassName, e);
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
   * Convert an exception to an ExitException
   * @param thrown the exception thrown
   * @return an ExitException with a status code
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
  
  protected ExitUtil.ExitException serviceCreationFailure(String serviceClassName,
      Exception e) {
    return new ServiceLaunchException(EXIT_SERVICE_CREATION_FAILURE, e);
  }
  
  /**
   * Register this class as the handler for the control-C interrupt.
   * Subclasses can extend this with extra operations, such as
   * an exception handler:
   * <pre>
   *  Thread.setDefaultUncaughtExceptionHandler(
   *     new YarnUncaughtExceptionHandler());
   * </pre>
   */
  protected void registerFailureHandling() {
    interruptEscalator = new InterruptEscalator<S>(this, SHUTDOWN_TIME_ON_INTERRUPT);
    try {
      interruptEscalator.register(IrqHandler.CONTROL_C);
      interruptEscalator.register(IrqHandler.SIGTERM);
    } catch (IllegalArgumentException e) {
      // downgrade interrupt registration to warnings
      LOG.warn("{}", e, e);
    }
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
   * Print a warning: this tries to log to the log's warn() operation
   * -if disabled it logs to system error
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
   * This tries to log to the log's error() operation
   * -if disabled the message is logged to system error along
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
   * Exit the code.
   * 
   * This is method can be overridden for testing, throwing an 
   * exception instead. Any subclassed method MUST raise an 
   * {@link ExitUtil.ExitException} instance.
   * The service launcher code assumes that after this method is invoked,
   * no other code in the same method is called.
   * @param exitCode code to exit
   */
  protected void exit(int exitCode, String message) {
    ExitUtil.terminate(exitCode, message);
  }

  /**
   * Exit off an exception.
   * 
   * This is the standard way a launched service exits.
   * Error code 0 means success
   * 
   * By default, calls
   * {@link ExitUtil#terminate(ExitUtil.ExitException)}.
   * 
   * This can be subclassed for testing
   * @param ee exit exception
   */
  protected void exit(ExitUtil.ExitException ee) {
    ExitUtil.terminate(ee);
  }

  /**
   * Override point: get the classloader to use
   * @return the classloader
   */
  protected ClassLoader getClassLoader() {
    return this.getClass().getClassLoader();
  }


  /**
   * Extract the configuration arguments and apply them to the configuration,
   * building an array of processed arguments to hand down to the service.
   *
   * @param conf configuration to update
   * @param args main arguments. args[0] is assumed to be the service
   * classname and is skipped
   * @return the processed list.
   * @throws ExitUtil.ExitException if exceptions are disabled
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
        File file = new File(filename);
        LOG.info("arg[{}] = Conf file {}",index, filename);
        if (!file.exists()) {
          exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
              ARG_CONF + ": configuration file not found: " + file);
          // never called, but retained for completeness
          break;
        }
        try {
          conf.addResource(file.toURI().toURL());
        } catch (MalformedURLException e) {
          exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
              ARG_CONF + ": configuration file path invalid: " + file);
          // never called, but retained for completeness
          break;
        }
      } else {
        index++;
        argsList.add(arg);
      }
    }
    return argsList;
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
   * Exit with a printed message. 
   * @param status status code
   * @param message message
   * @throws ExitUtil.ExitException if exceptions are disabled
   */
  private static void exitWithMessage(int status, String message) {
    ExitUtil.terminate(new ServiceLaunchException(status, message));
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
   * This is the JVM main entry point for the service launcher
   * -hands off to {@link #serviceMain(List)}
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
   * The real main function, which takes the arguments as a list
   * arg 0 must be the service classname
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
