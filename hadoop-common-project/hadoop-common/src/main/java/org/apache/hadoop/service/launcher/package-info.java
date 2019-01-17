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

/**

 This package contains classes, interfaces and exceptions to launch
 YARN services from the command line.

 <h2>Key Features</h2>

 <p>
 <b>General purpose YARN service launcher</b>:<p>
 The {@link org.apache.hadoop.service.launcher.ServiceLauncher} class parses
 a command line, then instantiates and launches the specified YARN service. It
 then waits for the service to finish, converting any exceptions raised or
 exit codes returned into an exit code for the (then exited) process. 
 <p>
 This class is designed be invokable from the static 
 {@link org.apache.hadoop.service.launcher.ServiceLauncher#main(String[])}
 method, or from {@code main(String[])} methods implemented by
 other classes which provide their own entry points.
  

 <p>
 <b>Extended YARN Service Interface</b>:<p>
 The {@link org.apache.hadoop.service.launcher.LaunchableService} interface
 extends {@link org.apache.hadoop.service.Service} with methods to pass
 down the CLI arguments and to execute an operation without having to
 spawn a thread in the  {@link org.apache.hadoop.service.Service#start()} phase.
  

 <p>
 <b>Standard Exit codes</b>:<p>
 {@link org.apache.hadoop.service.launcher.LauncherExitCodes}
 defines a set of exit codes that can be used by services to standardize
 exit causes.

 <p>
 <b>Escalated shutdown</b>:<p>
 The {@link org.apache.hadoop.service.launcher.ServiceShutdownHook}
 shuts down any service via the hadoop shutdown mechanism.
 The {@link org.apache.hadoop.service.launcher.InterruptEscalator} can be
 registered to catch interrupts, triggering the shutdown -and forcing a JVM
 exit if it times out or a second interrupt is received.

 <p><b>Tests:</b><p> test cases include interrupt handling and
 lifecycle failures.

 <h2>Launching a YARN Service</h2>

 The Service Launcher can launch <i>any YARN service</i>.
 It will instantiate the service classname provided, using the no-args
 constructor, or if no such constructor is available, it will fall back
 to a constructor with a single {@code String} parameter,
 passing the service name as the parameter value.
 <p>

 The launcher will initialize the service via
 {@link org.apache.hadoop.service.Service#init(Configuration)},
 then start it via its {@link org.apache.hadoop.service.Service#start()} method.
 It then waits indefinitely for the service to stop.
 <p> 
 After the service has stopped, a non-null value  of
 {@link org.apache.hadoop.service.Service#getFailureCause()} is interpreted
 as a failure, and, if it didn't happen during the stop phase (i.e. when
 {@link org.apache.hadoop.service.Service#getFailureState()} is not
 {@code STATE.STOPPED}, escalated into a non-zero return code).
 <p>
 
 To view the workflow in sequence, it is:
 <ol>
 <li>(prepare configuration files &mdash;covered later)</li>
 <li>instantiate service via its empty or string constructor</li>
 <li>call {@link org.apache.hadoop.service.Service#init(Configuration)}</li>
 <li>call {@link org.apache.hadoop.service.Service#start()}</li>
 <li>call
   {@link org.apache.hadoop.service.Service#waitForServiceToStop(long)}</li>
 <li>If an exception was raised: propagate it</li>
 <li>If an exception was recorded in
 {@link org.apache.hadoop.service.Service#getFailureCause()}
 while the service was running: propagate it.</li>
 </ol>

 For a service to be fully compatible with this launch model, it must
 <ul>
 <li>Start worker threads, processes and executors in its
 {@link org.apache.hadoop.service.Service#start()} method</li>
 <li>Terminate itself via a call to
 {@link org.apache.hadoop.service.Service#stop()}
 in one of these asynchronous methods.</li>
 </ul>

 If a service does not stop itself, <i>ever</i>, then it can be launched
 as a long-lived daemon.
 The service launcher will never terminate, but neither will the service.
 The service launcher does register signal handlers to catch {@code kill}
 and control-C signals &mdash;calling {@code stop()} on the service when
 signaled.
 This means that a daemon service <i>may</i> get a warning and time to shut
 down.

 <p>
 To summarize: provided a service launches its long-lived threads in its Service
 {@code start()} method, the service launcher can create it, configure it
 and start it, triggering shutdown when signaled.

 What these services can not do is get at the command line parameters or easily
 propagate exit codes (there is a way covered later). These features require
 some extensions to the base {@code Service} interface: <i>the Launchable
 Service</i>.

 <h2>Launching a Launchable YARN Service</h2>

 A Launchable YARN Service is a YARN service which implements the interface
 {@link org.apache.hadoop.service.launcher.LaunchableService}. 
 <p>
 It adds two methods to the service interface &mdash;and hence two new features:

 <ol>
 <li>Access to the command line passed to the service launcher </li>
 <li>A blocking {@code int execute()} method which can return the exit
 code for the application.</li>
 </ol>

 This design is ideal for implementing services which parse the command line,
 and which execute short-lived applications. For example, end user 
 commands can be implemented as such services, thus integrating with YARN's
 workflow and {@code YarnClient} client-side code.  

 <p>
 It can just as easily be used for implementing long-lived services that
 parse the command line -it just becomes the responsibility of the
 service to decide when to return from the {@code execute()} method.
 It doesn't even need to {@code stop()} itself; the launcher will handle
 that if necessary.
 <p>
 The {@link org.apache.hadoop.service.launcher.LaunchableService} interface
 extends {@link org.apache.hadoop.service.Service} with two new methods.

 <p>
 {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)}
 provides the {@code main(String args[])} arguments as a list, after any
 processing by the Service Launcher to extract configuration file references.
 This method <i>is called before
 {@link org.apache.hadoop.service.Service#init(Configuration)}.</i>
 This is by design: it allows the arguments to be parsed before the service is
 initialized, thus allowing services to tune their configuration data before
 passing it to any superclass in that {@code init()} method.
 To make this operation even simpler, the
 {@link org.apache.hadoop.conf.Configuration} that is to be passed in
 is provided as an argument.
 This reference passed in is the initial configuration for this service;
 the one that will be passed to the init operation.

 In
 {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)},
 a Launchable Service may manipulate this configuration by setting or removing
 properties. It may also create a new {@code Configuration} instance
 which may be needed to trigger the injection of HDFS or YARN resources
 into the default resources of all Configurations.
 If the return value of the method call is a configuration
 reference (as opposed to a null value), the returned value becomes that
 passed in to the {@code init()} method.
 <p>
 After the {@code bindArgs()} processing, the service's {@code init()}
 and {@code start()} methods are called, as usual.
 <p>
 At this point, rather than block waiting for the service to terminate (as
 is done for a basic service), the method
 {@link org.apache.hadoop.service.launcher.LaunchableService#execute()}
 is called.
 This is a method expected to block until completed, returning the intended 
 application exit code of the process when it does so. 
 <p> 
 After this {@code execute()} operation completes, the
 service is stopped and exit codes generated. Any exception raised
 during the {@code execute()} method takes priority over any exit codes
 returned by the method. This allows services to signal failures simply
 by raising exceptions with exit codes.
 <p>

 <p>
 To view the workflow in sequence, it is:
 <ol>
 <li>(prepare configuration files &mdash;covered later)</li>
 <li>instantiate service via its empty or string constructor</li>
 <li>call {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)}</li>
 <li>call {@link org.apache.hadoop.service.Service#init(Configuration)} with the existing config,
  or any new one returned by
  {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)}</li>
 <li>call {@link org.apache.hadoop.service.Service#start()}</li>
 <li>call {@link org.apache.hadoop.service.launcher.LaunchableService#execute()}</li>
 <li>call {@link org.apache.hadoop.service.Service#stop()}</li>
 <li>The return code from
  {@link org.apache.hadoop.service.launcher.LaunchableService#execute()}
  becomes the exit code of the process, unless overridden by an exception.</li>
 <li>If an exception was raised in this workflow: propagate it</li>
 <li>If an exception was recorded in
  {@link org.apache.hadoop.service.Service#getFailureCause()}
  while the service was running: propagate it.</li>
 </ol>


 <h2>Exit Codes and Exceptions</h2>

 <p>
 For a basic service, the return code is 0 unless an exception
 was raised. 
 <p>
 For a {@link org.apache.hadoop.service.launcher.LaunchableService}, the return
 code is the number returned from the
 {@link org.apache.hadoop.service.launcher.LaunchableService#execute()}
 operation, again, unless overridden an exception was raised.

 <p>
 Exceptions are converted into exit codes -but rather than simply
 have a "something went wrong" exit code, exceptions <i>may</i>
 provide exit codes which will be extracted and used as the return code.
 This enables Launchable Services to use exceptions as a way
 of returning error codes to signal failures and for
 normal Services to return any error code at all.

 <p>
 Any exception which implements the
 {@link org.apache.hadoop.util.ExitCodeProvider}
 interface is considered be a provider of the exit code: the method
 {@link org.apache.hadoop.util.ExitCodeProvider#getExitCode()}
 will be called on the caught exception to generate the return code.
 This return code and the message in the exception will be used to
 generate an instance of
 {@link org.apache.hadoop.util.ExitUtil.ExitException}
 which can be passed down to
 {@link org.apache.hadoop.util.ExitUtil#terminate(ExitUtil.ExitException)}
 to trigger a JVM exit. The initial exception will be used as the cause
 of the {@link org.apache.hadoop.util.ExitUtil.ExitException}.

 <p>
 If the exception is already an instance or subclass of 
 {@link org.apache.hadoop.util.ExitUtil.ExitException}, it is passed
 directly to
 {@link org.apache.hadoop.util.ExitUtil#terminate(ExitUtil.ExitException)}
 without any conversion.
 One such subclass,
 {@link org.apache.hadoop.service.launcher.ServiceLaunchException}
 may be useful: it includes formatted exception message generation. 
 It also declares that it extends the
 {@link org.apache.hadoop.service.launcher.LauncherExitCodes}
 interface listing common exception codes. These are exception codes
 that can be raised by the {@link org.apache.hadoop.service.launcher.ServiceLauncher}
 itself to indicate problems during parsing the command line, creating
 the service instance and the like. There are also some common exit codes
 for Hadoop/YARN service failures, such as
 {@link org.apache.hadoop.service.launcher.LauncherExitCodes#EXIT_UNAUTHORIZED}.
 Note that {@link org.apache.hadoop.util.ExitUtil.ExitException} itself
 implements {@link org.apache.hadoop.util.ExitCodeProvider#getExitCode()}

 <p>
 If an exception does not implement
 {@link org.apache.hadoop.util.ExitCodeProvider#getExitCode()},
 it will be wrapped in an {@link org.apache.hadoop.util.ExitUtil.ExitException}
 with the exit code
 {@link org.apache.hadoop.service.launcher.LauncherExitCodes#EXIT_EXCEPTION_THROWN}.

 <p>
 To view the exit code extraction in sequence, it is:
 <ol>
 <li>If no exception was triggered by a basic service, a
 {@link org.apache.hadoop.service.launcher.ServiceLaunchException} with an
 exit code of 0 is created.</li>

 <li>For a LaunchableService, the exit code is the result of {@code execute()}
 Again, a {@link org.apache.hadoop.service.launcher.ServiceLaunchException}
 with a return code of 0 is created.
 </li>

 <li>Otherwise, if the exception is an instance of {@code ExitException},
 it is returned as the service terminating exception.</li>

 <li>If the exception implements {@link org.apache.hadoop.util.ExitCodeProvider},
 its exit code and {@code getMessage()} value become the exit exception.</li>

 <li>Otherwise, it is wrapped as a
 {@link org.apache.hadoop.service.launcher.ServiceLaunchException}
 with the exit code
 {@link org.apache.hadoop.service.launcher.LauncherExitCodes#EXIT_EXCEPTION_THROWN}
 to indicate that an exception was thrown.</li>

 <li>This is finally passed to
 {@link org.apache.hadoop.util.ExitUtil#terminate(ExitUtil.ExitException)},
 by way of
 {@link org.apache.hadoop.service.launcher.ServiceLauncher#exit(ExitUtil.ExitException)};
 a method designed to allow subclasses to override for testing.</li>

 <li>The {@link org.apache.hadoop.util.ExitUtil} class then terminates the JVM
 with the specified exit code, printing the {@code toString()} value
 of the exception if the return code is non-zero.</li>
 </ol>

 This process may seem convoluted, but it is designed to allow any exception
 in the Hadoop exception hierarchy to generate exit codes,
 and to minimize the amount of exception wrapping which takes place.

 <h2>Interrupt Handling</h2>

 The Service Launcher has a helper class,
 {@link org.apache.hadoop.service.launcher.InterruptEscalator}
 to handle the standard SIGKILL signal and control-C signals.
 This class registers for signal callbacks from these signals, and,
 when received, attempts to stop the service in a limited period of time.
 It then triggers a JVM shutdown by way of
 {@link org.apache.hadoop.util.ExitUtil#terminate(int, String)}
 <p>
 If a second signal is received, the
 {@link org.apache.hadoop.service.launcher.InterruptEscalator}
 reacts by triggering an immediate JVM halt, invoking 
 {@link org.apache.hadoop.util.ExitUtil#halt(int, String)}. 
 This escalation process is designed to address the situation in which
 a shutdown-hook can block, yet the caller (such as an init.d daemon)
 wishes to kill the process.
 The shutdown script should repeat the kill signal after a chosen time period,
 to trigger the more aggressive process halt. The exit code will always be
 {@link org.apache.hadoop.service.launcher.LauncherExitCodes#EXIT_INTERRUPTED}.
 <p>
 The {@link org.apache.hadoop.service.launcher.ServiceLauncher} also registers
 a {@link org.apache.hadoop.service.launcher.ServiceShutdownHook} with the
 Hadoop shutdown hook manager, unregistering it afterwards. This hook will
 stop the service if a shutdown request is received, so ensuring that
 if the JVM is exited by any thread, an attempt to shut down the service
 will be made.
 

 <h2>Configuration class creation</h2>

 The Configuration class used to initialize a service is a basic
 {@link org.apache.hadoop.conf.Configuration} instance. As the launcher is
 the entry point for an application, this implies that the HDFS, YARN or other
 default configurations will not have been forced in through the constructors
 of {@code HdfsConfiguration} or {@code YarnConfiguration}.
 <p>
 What the launcher does do is use reflection to try and create instances of
 these classes simply to force in the common resources. If the classes are
 not on the classpath this fact will be logged.
 <p>
 Applications may consider it essential to either force load in the relevant
 configuration, or pass it down to the service being created. In which
 case further measures may be needed.
 
 <p><b>1: Creation in an extended {@code ServiceLauncher}</b>
 
 <p>
 Subclass the Service launcher and override its
 {@link org.apache.hadoop.service.launcher.ServiceLauncher#createConfiguration()}
 method with one that creates the right configuration.
 This is good if a single
 launcher can be created for all services launched by a module, such as
 HDFS or YARN. It does imply a dedicated script to invoke the custom
 {@code main()} method.

 <p><b>2: Creation in {@code bindArgs()}</b>

 <p>
 In
 {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)},
 a new configuration is created:

 <pre>
 public Configuration bindArgs(Configuration config, List&lt;String&gt; args)
    throws Exception {
   Configuration newConf = new YarnConfiguration(config);
   return newConf;
 }
 </pre>

 This guarantees a configuration of the right type is generated for all
 instances created via the service launcher. It does imply that this is
 expected to be only way that services will be launched.

 <p><b>3: Creation in {@code serviceInit()}</b>

 <pre>
 protected void serviceInit(Configuration conf) throws Exception {
   super.serviceInit(new YarnConfiguration(conf));
 }
 </pre>

 <p>
 This is a strategy used by many existing YARN services, and is ideal for
 services which do not implement the LaunchableService interface. Its one
 weakness is that the configuration is now private to that instance. Some
 YARN services use a single shared configuration instance as a way of
 propagating information between peer services in a
 {@link org.apache.hadoop.service.CompositeService}.
 While a dangerous practice, it does happen.


 <b>Summary</b>: the ServiceLauncher makes a best-effort attempt to load the
 standard Configuration subclasses, but does not fail if they are not present.
 Services which require a specific subclasses should follow one of the
 strategies listed;
 creation in {@code serviceInit()} is the recommended policy.
 
 <h2>Configuration file loading</h2>

 Before the service is bound to the CLI, the ServiceLauncher scans through
 all the arguments after the first one, looking for instances of the argument
 {@link org.apache.hadoop.service.launcher.ServiceLauncher#ARG_CONF}
 argument pair: {@code --conf &lt;file&gt;}. This must refer to a file
 in the local filesystem which exists.
 <p>
 It will be loaded into the Hadoop configuration
 class (the one created by the
 {@link org.apache.hadoop.service.launcher.ServiceLauncher#createConfiguration()}
 method.
 If this argument is repeated multiple times, all configuration
 files are merged with the latest file on the command line being the
 last one to be applied.
 <p>
 All the {@code --conf &lt;file&gt;} argument pairs are stripped off
 the argument list provided to the instantiated service; they get the
 merged configuration, but not the commands used to create it.

 <h2>Utility Classes</h2>

 <ul>

 <li>
 {@link org.apache.hadoop.service.launcher.IrqHandler}: registers interrupt
 handlers using {@code sun.misc} APIs.
 </li>

 <li>
 {@link org.apache.hadoop.service.launcher.ServiceLaunchException}: a
 subclass of {@link org.apache.hadoop.util.ExitUtil.ExitException} which
 takes a String-formatted format string and a list of arguments to create
 the exception text.
 </li>

 </ul>
 */


package org.apache.hadoop.service.launcher;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitUtil;
