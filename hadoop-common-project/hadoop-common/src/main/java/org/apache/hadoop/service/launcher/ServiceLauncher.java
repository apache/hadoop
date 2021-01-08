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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitCodeProvider;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

/**
 * A class to launch any YARN service by name.
 *
 * It's designed to be subclassed for custom entry points.
 *
 * Workflow:
 * <ol>
 *   <li>An instance of the class is created. It must be of the type
 *   {@link Service}</li>
 *   <li>If it implements
 *   {@link LaunchableService#bindArgs(Configuration, List)},
 *    it is given the binding args off the CLI after all general configuration
 *    arguments have been stripped.</li>
 *   <li>Its {@link Service#init(Configuration)} and {@link Service#start()}
 *   methods are called.</li>
 *   <li>If it implements it, {@link LaunchableService#execute()}
 *   is called and its return code used as the exit code.</li>
 *   <li>Otherwise: it waits for the service to stop, assuming that the
 *   {@link Service#start()} method spawns one or more thread
 *   to perform work</li>
 *   <li>If any exception is raised and provides an exit code,
 *   that is, it implements {@link ExitCodeProvider},
 *   the return value of {@link ExitCodeProvider#getExitCode()},
 *   becomes the exit code of the command.</li>
 * </ol>
 * Error and warning messages are logged to {@code stderr}.
 * 
 * @param <S> service class to cast the generated service to.
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class ServiceLauncher<S extends Service>
    implements LauncherExitCodes, LauncherArguments,
    Thread.UncaughtExceptionHandler {

  /**
   * Logger.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceLauncher.class);

  /**
   * Priority for the shutdown hook: {@value}.
   */
  protected static final int SHUTDOWN_PRIORITY = 30;

  /**
   * The name of this class.
   */
  public static final String NAME = "ServiceLauncher";

  protected static final String USAGE_NAME = "Usage: " + NAME;
  protected static final String USAGE_SERVICE_ARGUMENTS =
      "service-classname <service arguments>";

  /**
   * Usage message.
   *
   * Text: {@value}
   */
  public static final String USAGE_MESSAGE =
      USAGE_NAME
          + " [" + ARG_CONF_PREFIXED + " <conf file>]"
          + " [" + ARG_CONFCLASS_PREFIXED + " <configuration classname>]"
          + " "  + USAGE_SERVICE_ARGUMENTS;

  /**
   * The shutdown time on an interrupt: {@value}.
   */
  private static final int SHUTDOWN_TIME_ON_INTERRUPT = 30 * 1000;

  /**
   * The launched service.
   *
   * Invalid until the service has been created. 
   */
  private volatile S service;
  
  /**
   * Exit code of the service.
   *
   * Invalid until a service has
   * executed or stopped, depending on the service type.
   */
  private int serviceExitCode;
  
  /**
   * Any exception raised during execution.
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
   * Text description of service for messages.
   */
  private String serviceName;

  /**
   * Classname for the service to create.; empty string otherwise.
   */
  private String serviceClassName = "";

  /**
   * List of the standard configurations to create (and so load in properties).
   * The values are Hadoop, HDFS and YARN configurations.
   */
  protected static final String[] DEFAULT_CONFIGS = {
      "org.apache.hadoop.conf.Configuration",
      "org.apache.hadoop.hdfs.HdfsConfiguration",
      "org.apache.hadoop.yarn.conf.YarnConfiguration"
  };

  /**
   * List of classnames to load to configuration before creating a
   * {@link Configuration} instance.
   */
  private List<String> confClassnames = new ArrayList<>(DEFAULT_CONFIGS.length);

  /**
   * URLs of configurations to load into the configuration instance created.
   */
  private List<URL> confResourceUrls = new ArrayList<>(1);

  /** Command options. Preserved for usage statements. */
  private Options commandOptions;

  /**
   * Create an instance of the launcher.
   * @param serviceClassName classname of the service
   */
  public ServiceLauncher(String serviceClassName) {
    this(serviceClassName, serviceClassName);
  }

  /**
   * Create an instance of the launcher.
   * @param serviceName name of service for text messages
   * @param serviceClassName classname of the service
   */
  public ServiceLauncher(String serviceName, String serviceClassName) {
    this.serviceClassName = serviceClassName;
    this.serviceName = serviceName;
    // set up initial list of configurations
    confClassnames.addAll(Arrays.asList(DEFAULT_CONFIGS));
  }

  /**
   * Get the service.
   *
   * Null until
   * {@link #coreServiceLaunch(Configuration, Service, List, boolean, boolean)}
   * has completed.
   * @return the service
   */
  public final S getService() {
    return service;
  }

  /**
   * Setter is to give subclasses the ability to manipulate the service.
   * @param s the new service
   */
  protected void setService(S s) {
    this.service = s;
  }

  /**
   * Get the configuration constructed from the command line arguments.
   * @return the configuration used to create the service
   */
  public final Configuration getConfiguration() {
    return configuration;
  }

  /**
   * The exit code from a successful service execution.
   * @return the exit code. 
   */
  public final int getServiceExitCode() {
    return serviceExitCode;
  }

  /**
   * Get the exit exception used to end this service.
   * @return an exception, which will be null until the service
   * has exited (and {@code System.exit} has not been called)
   */
  public final ExitUtil.ExitException getServiceException() {
    return serviceException;
  }

  /**
   * Probe for service classname being defined.
   * @return true if the classname is set
   */
  private boolean isClassnameDefined() {
    return serviceClassName != null && !serviceClassName.isEmpty();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("\"ServiceLauncher for \"");
    sb.append(serviceName);
    if (isClassnameDefined()) {
      sb.append(", serviceClassName='").append(serviceClassName).append('\'');
    }
    if (service != null) {
      sb.append(", service=").append(service);
    }
    return sb.toString();
  }

  /**
   * Launch the service and exit.
   *
   * <ol>
   * <li>Parse the command line.</li> 
   * <li>Build the service configuration from it.</li>
   * <li>Start the service.</li>
   * <li>If it is a {@link LaunchableService}: execute it</li>
   * <li>Otherwise: wait for it to finish.</li>
   * <li>Exit passing the status code to the {@link #exit(int, String)}
   * method.</li>
   * </ol>
   * @param args arguments to the service. {@code arg[0]} is 
   * assumed to be the service classname.
   */
  public void launchServiceAndExit(List<String> args) {
    StringBuilder builder = new StringBuilder();
    for (String arg : args) {
      builder.append('"').append(arg).append("\" ");
    }
    String argumentString = builder.toString();
    if (LOG.isDebugEnabled()) {
      LOG.debug(startupShutdownMessage(serviceName, args));
      LOG.debug(argumentString);
    }
    registerFailureHandling();
    // set up the configs, using reflection to push in the -site.xml files
    loadConfigurationClasses();
    Configuration conf = createConfiguration();
    for (URL resourceUrl : confResourceUrls) {
      conf.addResource(resourceUrl);
    }
    bindCommandOptions();
    ExitUtil.ExitException exitException;
    try {
      List<String> processedArgs = extractCommandOptions(conf, args);
      exitException = launchService(conf, processedArgs, true, true);
    } catch (ExitUtil.ExitException e) {
      exitException = e;
      noteException(exitException);
    }
    if (exitException.getExitCode() == LauncherExitCodes.EXIT_USAGE) {
      // something went wrong. Print the usage and commands
      System.err.println(getUsageMessage());
      System.err.println("Command: " + argumentString);
    }
    System.out.flush();
    System.err.flush();
    exit(exitException);
  }

  /**
   * Set the {@link #commandOptions} field to the result of
   * {@link #createOptions()}; protected for subclasses and test access.
   */
  protected void bindCommandOptions() {
    commandOptions = createOptions();
  }

  /**
   * Record that an Exit Exception has been raised.
   * Save it to {@link #serviceException}, with its exit code in
   * {@link #serviceExitCode}
   * @param exitException exception
   */
  void noteException(ExitUtil.ExitException exitException) {
    int exitCode = exitException.getExitCode();
    if (exitCode != 0) {
      LOG.debug("Exception raised with exit code {}",
          exitCode,
          exitException);
      Throwable cause = exitException.getCause();
      if (cause != null) {
        // log the nested exception in more detail
        LOG.warn("{}", cause.toString(), cause);
      }
    }
    serviceExitCode = exitCode;
    serviceException = exitException;
  }

  /**
   * Get the usage message, ideally dynamically.
   * @return the usage message
   */
  protected String getUsageMessage() {
    String message =   USAGE_MESSAGE;
    if (commandOptions != null) {
      message = USAGE_NAME
          + " " + commandOptions.toString()
          + " " + USAGE_SERVICE_ARGUMENTS;
    }
    return message;
  }

  /**
   * Override point: create an options instance to combine with the 
   * standard options set.
   * <i>Important. Synchronize uses of {@link OptionBuilder}</i>
   * with {@code OptionBuilder.class}
   * @return the new options
   */
  @SuppressWarnings("static-access")
  protected Options createOptions() {
    synchronized (OptionBuilder.class) {
      Options options = new Options();
      Option oconf = OptionBuilder.withArgName("configuration file")
          .hasArg()
          .withDescription("specify an application configuration file")
          .withLongOpt(ARG_CONF)
          .create(ARG_CONF_SHORT);
      Option confclass = OptionBuilder.withArgName("configuration classname")
          .hasArg()
          .withDescription(
              "Classname of a Hadoop Configuration subclass to load")
          .withLongOpt(ARG_CONFCLASS)
          .create(ARG_CONFCLASS_SHORT);
      Option property = OptionBuilder.withArgName("property=value")
          .hasArg()
          .withDescription("use value for given property")
          .create('D');
      options.addOption(oconf);
      options.addOption(property);
      options.addOption(confclass);
      return options;
    }
  }

  /**
   * Override point: create the base configuration for the service.
   *
   * Subclasses can override to create HDFS/YARN configurations etc.
   * @return the configuration to use as the service initializer.
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Override point: Get a list of configuration classes to create.
   * @return the array of configs to attempt to create. If any are off the
   * classpath, that is logged
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  protected List<String> getConfigurationsToCreate() {
    return confClassnames;
  }

  /**
   * This creates all the configurations defined by
   * {@link #getConfigurationsToCreate()} , ensuring that
   * the resources have been pushed in.
   * If one cannot be loaded it is logged and the operation continues
   * except in the case that the class does load but it isn't actually
   * a subclass of {@link Configuration}.
   * @throws ExitUtil.ExitException if a loaded class is of the wrong type
   */
  @VisibleForTesting
  public int loadConfigurationClasses() {
    List<String> toCreate = getConfigurationsToCreate();
    int loaded = 0;
    for (String classname : toCreate) {
      try {
        Class<?> loadClass = getClassLoader().loadClass(classname);
        Object instance = loadClass.getConstructor().newInstance();
        if (!(instance instanceof Configuration)) {
          throw new ExitUtil.ExitException(EXIT_SERVICE_CREATION_FAILURE,
              "Could not create " + classname
              + " because it is not a Configuration class/subclass");
        }
        loaded++;
      } catch (ClassNotFoundException e) {
        // class could not be found -implies it is not on the current classpath
        LOG.debug("Failed to load {} because it is not on the classpath",
            classname);
      } catch (ExitUtil.ExitException e) {
        // rethrow
        throw e;
      } catch (Exception e) {
        // any other exception
        LOG.info("Failed to create {}", classname, e);
      }
    }
    return loaded;
  }

  /**
   * Launch a service catching all exceptions and downgrading them to exit codes
   * after logging.
   *
   * Sets {@link #serviceException} to this value.
   * @param conf configuration to use
   * @param processedArgs command line after the launcher-specific arguments
   * have been stripped out.
   * @param addShutdownHook should a shutdown hook be added to terminate
   * this service on shutdown. Tests should set this to false.
   * @param execute execute/wait for the service to stop.
   * @return an exit exception, which will have a status code of 0 if it worked
   */
  public ExitUtil.ExitException launchService(Configuration conf,
      List<String> processedArgs,
      boolean addShutdownHook,
      boolean execute) {
    return launchService(conf, null, processedArgs, addShutdownHook, execute);
  }

  /**
   * Launch a service catching all exceptions and downgrading them to exit codes
   * after logging.
   *
   * Sets {@link #serviceException} to this value.
   * @param conf configuration to use
   * @param instance optional instance of the service.
   * @param processedArgs command line after the launcher-specific arguments
   * have been stripped out.
   * @param addShutdownHook should a shutdown hook be added to terminate
   * this service on shutdown. Tests should set this to false.
   * @param execute execute/wait for the service to stop.
   * @return an exit exception, which will have a status code of 0 if it worked
   */
  public ExitUtil.ExitException launchService(Configuration conf,
      S instance,
      List<String> processedArgs,
      boolean addShutdownHook,
      boolean execute) {

    ExitUtil.ExitException exitException;

    try {
      int exitCode = coreServiceLaunch(conf, instance, processedArgs,
          addShutdownHook, execute);
      if (service != null) {
        // check to see if the service failed
        Throwable failure = service.getFailureCause();
        if (failure != null) {
          // the service exited with a failure.
          // check what state it is in
          Service.STATE failureState = service.getFailureState();
          if (failureState == Service.STATE.STOPPED) {
            // the failure occurred during shutdown, not important enough
            // to bother the user as it may just scare them
            LOG.debug("Failure during shutdown: {} ", failure, failure);
          } else {
            //throw it for the catch handlers to deal with
            throw failure;
          }
        }
      }
      String name = getServiceName();

      if (exitCode == 0) {
        exitException = new ServiceLaunchException(exitCode,
            "%s succeeded",
            name);
      } else {
        exitException = new ServiceLaunchException(exitCode,
            "%s failed ", name);
      }
      // either the service succeeded, or an error raised during shutdown, 
      // which we don't worry that much about
    } catch (ExitUtil.ExitException ee) {
      // exit exceptions are passed through unchanged
      exitException = ee;
    } catch (Throwable thrown) {
      // other errors need a full log.
      LOG.error("Exception raised {}",
          service != null
              ? (service.toString() + " in state  " + service.getServiceState())
              : "during service instantiation",
          thrown);
      exitException = convertToExitException(thrown);
    }
    noteException(exitException);
    return exitException;
  }

  /**
   * Launch the service.
   *
   * All exceptions that occur are propagated upwards.
   *
   * If the method returns a status code, it means that it got as far starting
   * the service, and if it implements {@link LaunchableService}, that the 
   * method {@link LaunchableService#execute()} has completed. 
   *
   * After this method returns, the service can be retrieved returned by
   * {@link #getService()}.
   *
   * @param conf configuration
   * @param instance optional instance of the service.
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
   * @throws Exception any other failure -if it implements
   * {@link ExitCodeProvider} then it defines the exit code for any
   * containing exception
   */

  protected int coreServiceLaunch(Configuration conf,
      S instance,
      List<String> processedArgs,
      boolean addShutdownHook,
      boolean execute) throws Exception {

    // create the service instance
    if (instance == null) {
      instantiateService(conf);
    } else {
      // service already exists, so instantiate
      configuration = conf;
      service = instance;
    }
    ServiceShutdownHook shutdownHook = null;

    // and the shutdown hook if requested
    if (addShutdownHook) {
      shutdownHook = new ServiceShutdownHook(service);
      shutdownHook.register(SHUTDOWN_PRIORITY);
    }
    String name = getServiceName();
    LOG.debug("Launched service {}", name);
    LaunchableService launchableService = null;

    if (service instanceof LaunchableService) {
      // it's a LaunchableService, pass in the conf and arguments before init)
      LOG.debug("Service {} implements LaunchableService", name);
      launchableService = (LaunchableService) service;
      if (launchableService.isInState(Service.STATE.INITED)) {
        LOG.warn("LaunchableService {}" 
            + " initialized in constructor before CLI arguments passed in",
            name);
      }
      Configuration newconf = launchableService.bindArgs(configuration,
          processedArgs);
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
      if (execute && service.isInState(Service.STATE.STARTED)) {
        if (launchableService != null) {
          // assume that runnable services are meant to run from here
          try {
            exitCode = launchableService.execute();
            LOG.debug("Service {} execution returned exit code {}",
                name, exitCode);
          } finally {
            // then stop the service
            service.stop();
          }
        } else {
          //run the service until it stops or an interrupt happens
          // on a different thread.
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
   * Instantiate the service defined in {@code serviceClassName}.
   *
   * Sets the {@code configuration} field
   * to the the value of {@code conf},
   * and the {@code service} field to the service created.
   *
   * @param conf configuration to use
   */
  @SuppressWarnings("unchecked")
  public Service instantiateService(Configuration conf) {
    Preconditions.checkArgument(conf != null, "null conf");
    Preconditions.checkArgument(serviceClassName != null,
        "null service classname");
    Preconditions.checkArgument(!serviceClassName.isEmpty(),
        "undefined service classname");
    configuration = conf;

    // Instantiate the class. this requires the service to have a public
    // zero-argument or string-argument constructor
    Object instance;
    try {
      Class<?> serviceClass = getClassLoader().loadClass(serviceClassName);
      try {
        instance = serviceClass.getConstructor().newInstance();
      } catch (NoSuchMethodException noEmptyConstructor) {
        // no simple constructor, fall back to a string
        LOG.debug("No empty constructor {}", noEmptyConstructor,
            noEmptyConstructor);
        instance = serviceClass.getConstructor(String.class)
                               .newInstance(serviceClassName);
      }
    } catch (Exception e) {
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
   * Convert an exception to an {@code ExitException}.
   *
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
   * @return an {@code ExitException} with a status code
   */
  protected static ExitUtil.ExitException convertToExitException(
      Throwable thrown) {
    ExitUtil.ExitException exitException;
    // get the exception message
    String message = thrown.toString();
    int exitCode;
    if (thrown instanceof ExitCodeProvider) {
      // the exception provides a status code -extract it
      exitCode = ((ExitCodeProvider) thrown).getExitCode();
      message = thrown.getMessage();
      if (message == null) {
        // some exceptions do not have a message; fall back
        // to the string value.
        message = thrown.toString();
      }
    } else {
      // no exception code: use the default
      exitCode = EXIT_EXCEPTION_THROWN;
    }
    // construct the new exception with the original message and
    // an exit code
    exitException = new ServiceLaunchException(exitCode, thrown, message);
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
   * Override point: register this class as the handler for the control-C
   * and SIGINT interrupts.
   *
   * Subclasses can extend this with extra operations, such as
   * an exception handler:
   * <pre>
   *  Thread.setDefaultUncaughtExceptionHandler(
   *     new YarnUncaughtExceptionHandler());
   * </pre>
   */
  protected void registerFailureHandling() {
    try {
      interruptEscalator = new InterruptEscalator(this,
          SHUTDOWN_TIME_ON_INTERRUPT);
      interruptEscalator.register(IrqHandler.CONTROL_C);
      interruptEscalator.register(IrqHandler.SIGTERM);
    } catch (IllegalArgumentException e) {
      // downgrade interrupt registration to warnings
      LOG.warn("{}", e, e);
    }
    Thread.setDefaultUncaughtExceptionHandler(
        new HadoopUncaughtExceptionHandler(this));
  }

  /**
   * Handler for uncaught exceptions: terminate the service.
   * @param thread thread
   * @param exception exception
   */
  @Override
  public void uncaughtException(Thread thread, Throwable exception) {
    LOG.error("Uncaught exception in thread {} -exiting", thread, exception);
    exit(convertToExitException(exception));
  }

  /**
   * Get the service name via {@link Service#getName()}.
   *
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
      return "service " + serviceName;
    }
  }
  
  /**
   * Print a warning message.
   * <p>
   * This tries to log to the log's warn() operation.
   * If the log at that level is disabled it logs to system error
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
   * This tries to log to {@code LOG.error()}.
   * <p>
   * If that log level is disabled disabled the message
   * is logged to system error along with {@code thrown.toString()}
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
   *
   * This is method can be overridden for testing, throwing an 
   * exception instead. Any subclassed method MUST raise an 
   * {@code ExitException} instance/subclass.
   * The service launcher code assumes that after this method is invoked,
   * no other code in the same method is called.
   * @param exitCode code to exit
   */
  protected void exit(int exitCode, String message) {
    ExitUtil.terminate(exitCode, message);
  }

  /**
   * Exit the JVM using an exception for the exit code and message,
   * invoking {@link ExitUtil#terminate(ExitUtil.ExitException)}.
   *
   * This is the standard way a launched service exits.
   * An error code of 0 means success -nothing is printed.
   *
   * If {@link ExitUtil#disableSystemExit()} has been called, this
   * method will throw the exception.
   *
   * The method <i>may</i> be subclassed for testing
   * @param ee exit exception
   * @throws ExitUtil.ExitException if ExitUtil exceptions are disabled
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
   * Extract the command options and apply them to the configuration,
   * building an array of processed arguments to hand down to the service.
   *
   * @param conf configuration to update.
   * @param args main arguments. {@code args[0]}is assumed to be
   * the service classname and is skipped.
   * @return the remaining arguments
   * @throws ExitUtil.ExitException if JVM exiting is disabled.
   */
  public List<String> extractCommandOptions(Configuration conf,
      List<String> args) {
    int size = args.size();
    if (size <= 1) {
      return new ArrayList<>(0);
    }
    List<String> coreArgs = args.subList(1, size);

    return parseCommandArgs(conf, coreArgs);
  }

  /**
   * Parse the command arguments, extracting the service class as the last
   * element of the list (after extracting all the rest).
   *
   * The field {@link #commandOptions} field must already have been set.
   * @param conf configuration to use
   * @param args command line argument list
   * @return the remaining arguments
   * @throws ServiceLaunchException if processing of arguments failed
   */
  protected List<String> parseCommandArgs(Configuration conf,
      List<String> args) {
    Preconditions.checkNotNull(commandOptions,
        "Command options have not been created");
    StringBuilder argString = new StringBuilder(args.size() * 32);
    for (String arg : args) {
      argString.append("\"").append(arg).append("\" ");
    }
    LOG.debug("Command line: {}", argString);
    try {
      String[] argArray = args.toArray(new String[args.size()]);
      // parse this the standard way. This will
      // update the configuration in the parser, and potentially
      // patch the user credentials
      GenericOptionsParser parser = createGenericOptionsParser(conf, argArray);
      if (!parser.isParseSuccessful()) {
        throw new ServiceLaunchException(EXIT_COMMAND_ARGUMENT_ERROR,
            E_PARSE_FAILED + " %s", argString);
      }
      CommandLine line = parser.getCommandLine();
      List<String> remainingArgs = Arrays.asList(parser.getRemainingArgs());
      LOG.debug("Remaining arguments {}", remainingArgs);

      // Scan the list of configuration files
      // and bail out if they don't exist
      if (line.hasOption(ARG_CONF)) {
        String[] filenames = line.getOptionValues(ARG_CONF);
        verifyConfigurationFilesExist(filenames);
        // Add URLs of files as list of URLs to load
        for (String filename : filenames) {
          File file = new File(filename);
          LOG.debug("Configuration files {}", file);
          confResourceUrls.add(file.toURI().toURL());
        }
      }
      if (line.hasOption(ARG_CONFCLASS)) {
        // new resources to instantiate as configurations
        List<String> classnameList = Arrays.asList(
            line.getOptionValues(ARG_CONFCLASS));
        LOG.debug("Configuration classes {}", classnameList);
        confClassnames.addAll(classnameList);
      }
      // return the remainder
      return remainingArgs;
    } catch (IOException e) {
      // parsing problem: convert to a command argument error with
      // the original text
      throw new ServiceLaunchException(EXIT_COMMAND_ARGUMENT_ERROR, e);
    } catch (RuntimeException e) {
      // lower level issue such as XML parse failure
      throw new ServiceLaunchException(EXIT_COMMAND_ARGUMENT_ERROR, e,
          E_PARSE_FAILED + " %s : %s", argString, e);
    }
  }

  /**
   * Override point: create a generic options parser or subclass thereof.
   * @param conf Hadoop configuration
   * @param argArray array of arguments
   * @return a generic options parser to parse the arguments
   * @throws IOException on any failure
   */
  protected GenericOptionsParser createGenericOptionsParser(Configuration conf,
      String[] argArray) throws IOException {
    return new MinimalGenericOptionsParser(conf, commandOptions, argArray);
  }

  /**
   * Verify that all the specified filenames exist.
   * @param filenames a list of files
   * @throws ServiceLaunchException if a file is not found
   */
  protected void verifyConfigurationFilesExist(String[] filenames) {
    if (filenames == null) {
      return;
    }
    for (String filename : filenames) {
      File file = new File(filename);
      LOG.debug("Conf file {}", file.getAbsolutePath());
      if (!file.exists()) {
        // no configuration file
        throw new ServiceLaunchException(EXIT_NOT_FOUND,
            ARG_CONF_PREFIXED + ": configuration file not found: %s",
            file.getAbsolutePath());
      }
    }
  }

  /**
   * Build a log message for starting up and shutting down. 
   * @param classname the class of the server
   * @param args arguments
   */
  protected static String startupShutdownMessage(String classname,
      List<String> args) {
    final String hostname = NetUtils.getHostname();

    return StringUtils.createStartupShutdownMessage(classname, hostname,
        args.toArray(new String[args.size()]));
  }

  /**
   * Exit with a printed message. 
   * @param status status code
   * @param message message message to print before exiting
   * @throws ExitUtil.ExitException if exceptions are disabled
   */
  protected static void exitWithMessage(int status, String message) {
    ExitUtil.terminate(new ServiceLaunchException(status, message));
  }

  /**
   * Exit with the usage exit code {@link #EXIT_USAGE}
   * and message {@link #USAGE_MESSAGE}.
   * @throws ExitUtil.ExitException if exceptions are disabled
   */
  protected static void exitWithUsageMessage() {
    exitWithMessage(EXIT_USAGE, USAGE_MESSAGE);
  }

  /**
   * This is the JVM entry point for the service launcher.
   *
   * Converts the arguments to a list, then invokes {@link #serviceMain(List)}
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    serviceMain(Arrays.asList(args));
  }

  /**
   * Varargs version of the entry point for testing and other in-JVM use.
   * Hands off to {@link #serviceMain(List)}
   * @param args command line arguments.
   */
  public static void serviceMain(String... args) {
    serviceMain(Arrays.asList(args));
  }

  /* ====================================================================== */
  /**
   * The real main function, which takes the arguments as a list.
   * Argument 0 MUST be the service classname
   * @param argsList the list of arguments
   */
  /* ====================================================================== */

  public static void serviceMain(List<String> argsList) {
    if (argsList.isEmpty()) {
      // no arguments: usage message
      exitWithUsageMessage();
    } else {
      ServiceLauncher<Service> serviceLauncher =
          new ServiceLauncher<>(argsList.get(0));
      serviceLauncher.launchServiceAndExit(argsList);
    }
  }

  /**
   * A generic options parser which does not parse any of the traditional
   * Hadoop options.
   */
  protected static class MinimalGenericOptionsParser
      extends GenericOptionsParser {
    public MinimalGenericOptionsParser(Configuration conf,
        Options options, String[] args) throws IOException {
      super(conf, options, args);
    }

    @Override
    protected Options buildGeneralOptions(Options opts) {
      return opts;
    }
  }
}
