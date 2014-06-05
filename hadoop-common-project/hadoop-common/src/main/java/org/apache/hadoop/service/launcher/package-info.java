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

package org.apache.hadoop.service.launcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitUtil;

import java.util.List;

/**

 This package contains classes, interfaces and exceptions to launch
 YARN services from the command line.

 <h2>Key Features</h2>

 <p>
 <b>Generic service launcher</b>:
 The {@link org.apache.hadoop.service.launcher.ServiceLauncher} class parses
 a command line, then instantiates and launches the specified service. It
 then waits for the service to finish, converting any exceptions raised or
 exit codes returned into an exit code for the (then exited) process. 
 This class is designed be invokable from the static 
 {@link org.apache.hadoop.service.launcher.ServiceLauncher#main(String[])} method,
 or from <code>main(String[])</code> methods implemented by other classes
 that which to provide their own endpoints.
 </p> 
 
 <p>
 <b>Extended {@link org.apache.hadoop.service.Service} Interface</b>:
 The {@link org.apache.hadoop.service.launcher.LaunchableService} interface
 adds methods to pass down the CLI arguments, to execute a command line
 action without having to spawn a thread in the
 {@link org.apache.hadoop.service.Service#start()} phase.
 </p> 
 
 <p>
 <b>Escalated shutdown</b>: the {@link org.apache.hadoop.service.launcher.ServiceShutdownHook}
 shuts down any service via the hadoop shutdown mechanism.
 The {@link org.apache.hadoop.service.launcher.InterruptEscalator} can be
 registered to catch interrupts, triggering the shutdown -and forcing a JVM
 exit if it times our or a second interrupt is received.
 </p> 

 <p>
 <b>Standard Exit codes</b>: {@link org.apache.hadoop.service.launcher.LauncherExitCodes}
 Defines a set of exit codes that can be used by services to standardize exit causes.
 </p> 

 <p><b>Tests:</b> test cases include interrupt handling and
 lifecycle failures.</p>

 <h2>Utility Classes</h2>
 
 <ul>

 <li>
 {@link org.apache.hadoop.service.launcher.IrqHandler}: registers interrupt
 handlers using <code>sun.misc</code> APIs.
 </li>
 
 <li>
 {@link org.apache.hadoop.service.launcher.ServiceLaunchException}: a
 subclass of {@link org.apache.hadoop.util.ExitUtil.ExitException} which
 takes a String-formatted format string and a list of arguments to create
 the exception text.
 </li>
  
 </ul>
 
 <h2>Launching a YARN Service</h2>
 
 
 The Service Launcher can launch <i>any YARN service</i>. It will instantiate
 the service classname provided, by zero-args constructor. Or, if none
 is available, falling back to a constructor that takes a <code>String</code>
 as its parameter, on the assumption that the parameter is the service name.
 <p>
 
 The launcher will initialize the service via {@link org.apache.hadoop.service.Service#init(Configuration)},
 then start it via its {@link org.apache.hadoop.service.Service#start()} method.
 <p>
 It then proceeds to wait for the service to stop.
 <p> 
 The return code of the service is 0 if it worked, or a non-zero return code
 if a failure happened during service instantiation, init or launch. 
 After the service has stopped, a non-null value  of
 {@link org.apache.hadoop.service.Service#getFailureCause()} is interpreted
 as a failure, and, if it didn't happen during the stop phase (i.e. when
 {@link org.apache.hadoop.service.Service#getFailureState()} is not
 <code>STATE.STOPPED</code>, escalated into a non-zero return code.
 <p>
 To view the worklow in sequence, it is:
 <ol>
 <li>(prepare configuration files -covered later)</li>
 <li>instantiate service via its empty or string constructor</li>
 <li>call {@link org.apache.hadoop.service.Service#init(Configuration)}</li>
 <li>call {@link org.apache.hadoop.service.Service#start()}</li>
 <li>call {@link org.apache.hadoop.service.Service#waitForServiceToStop(long)}</li>
 <li>If an exception was raised in this workflow: propagate it</li>
 <li>If an exception was recorded in {@link org.apache.hadoop.service.Service#getFailureCause()}
 while the service was running: propagate it.</li>
 </ol>

 For a service to be fully compatible with this launch model, it must
 <ul>
 <li>start worker threads, processes and executors in its <code>start()</code>
 method</li>
 <li>Terminate itself via a call to <code>stop()</code> in one of these
 asynchronous methods.</li>
 </ul>
 
 If a service does not stop itself, <i>ever</i>, then it can be launched as a long-lived
 daemon. The service launcher will never terminate, but neither will the service.
 The service launcher does register signal handlers to catch <code>kill</code>
 and control-C signals -calling <code>stop()</code> on the service when signalled.
 This means that a daemon service <i>may</i> get some warning and time to shut
 down.
 
 <p>
 To summarize: provided a service launches its long-lived processes in it's
 <code>start()</code> method, the service launcher can create it, configure it
 and start it -triggering shutdown when signalled.</b>
 
 What these services can not do is get at the command line parameters or easily
 propagate exit codes (there is way covered later). These features require
 some extensions to the base <code>Service</code> interface: <i>the Launchable
 Service</i>.
 
 <h2>Launching a Launchable YARN Service</h2>

 A Launchable YARN Service is a YARN service which implements the interface
 {@link org.apache.hadoop.service.launcher.LaunchableService}. 
 <p>
 It adds two methods to the service interface -and hence two new features: 
 
 <ol>
 <li>Access to the command line passed to the service launcher </li>
 <li>A blocking <code>int execute()</code> method which can return the exit
 code for the application.<li>
 </ol>
 
 This design is ideal for implementing services which parse the command line,
 and which execute short-lived applications. For example, end user 
 commands can be implemented as such services, so integrating with YARN's
 workflow and <code>YarnClient</code> client-side code.  
 
 <p>
 It can just as easily be used for implementing long-lived services that
 parse the command line -it just becomes the responsibility of the
 service to decide when to return from the <code>execute()</code> method.
 It doesn't even need to <code>stop()</code> itself; the launcher will handle
 that if necessary.
 <p>
 The {@link org.apache.hadoop.service.launcher.LaunchableService} interface
 extends {@link org.apache.hadoop.service.Service} with two new methods.
 
 <p>
 {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)}
 provides the <code>main(String args[])</code> arguments as a list, after any
 processing by the Service Launcher to extract configuration file references.
 This method <i>is called before {@link org.apache.hadoop.service.Service#init(Configuration)}.</i>
 This is by design: it allows the arguments to be parsed before the service is initialized,
 so allowing services to tune their configuration data before passing it to 
 any superclass in that <code>init()</code> method. 
 To make this operation even simpler, the {@link org.apache.hadoop.conf.Configuration}
 that is to be passed in is provided as an argument. This reference passed in is the initial
 configuration for this service; the one that will be passed to the init operation.
 In {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)},
 a Launchable Service may manipulate this configuration by setting or removing
 properties. It may also create a new Configuration instance -which may be needed
 to trigger the injection of HDFS or YARN resources into the default resources
 of all Configurations. If the return value of the method call is a configuration
 reference (as opposed to a null value), the returned value becomes that
 passed in to the <code>init()</code> method.
 <p>
 After the <code>bindArgs()</code> processing, the service's <code>init()</code>
 and <code>start()</code> methods are called, as usual. It is after this point
 that the lifecycle of a launched service diverges from a classic service.
 In a Launchable Service, the method {@link org.apache.hadoop.service.launcher.LaunchableService#execute()}
 is called, a method expected to block until completed, returning the exit code
 of the process when it does so. This method will be invoked after the
 service is started -and provided the service hasn't already stopped.
 <p> 
 Finally, after this <code>execute()</code> operation completes, the
 service is stopped and exit codes generated. Any exception raised
 during the execute method takes priority over any exit codes returned
 by the method, so services may signal failures simply by returning
 exceptions with exit codes.
 <p>

 <p>
 To view the worklow in sequence, it is:
 <ol>
 <li>(prepare configuration files -covered later)</li>
 <li>instantiate service via its empty or string constructor</li>
 <li>call {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)}</li>
 <li>call {@link org.apache.hadoop.service.Service#init(Configuration)} with the existing config,
 or any new one returned by {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)}</li>
 <li>call {@link org.apache.hadoop.service.Service#start()}</li>
 <li>call {@link org.apache.hadoop.service.launcher.LaunchableService#execute()}</li>
 <li>call {@link org.apache.hadoop.service.Service#stop()}</li>
 <li>The return code from the {@link org.apache.hadoop.service.launcher.LaunchableService#execute()} 
 becomes the exit code of the process, unless overridden by an exception.</li>
 <li>If an exception was raised in this workflow: propagate it</li>
 <li>If an exception was recorded in {@link org.apache.hadoop.service.Service#getFailureCause()}
 while the service was running: propagate it.</li>
 </ol>


 <h2>Exit codes and Exceptions</h2>

 As stated, the exit code from run is, for a LaunchableService, the return
 value from the {@link org.apache.hadoop.service.launcher.LaunchableService#execute()}
 operation -unless overridden by an exception.
 <p>
 For a normal service, the return code is 0 unless an exception
 was raised. 
 </p>
 Exceptions are converted into exit codes -but rather than simply
 have an exit code "something went wrong", exceptions may
 provide exit codes which will be extracted and used as the return code.
 This enables LaunchableServices to use exceptions as a way
 of returning error codes to signal failures -and for 
 normal Services to return any error code at all.
 
 <p>
 Any exception that implements the {@link org.apache.hadoop.util.ExitCodeProvider}
 interface will be a provider of the exit code: the method
 {@link org.apache.hadoop.util.ExitCodeProvider#getExitCode()}
 will be called on the caught exception to generate the return code.
 This return code and the message in the exception will be used to
 generate an instance of {@link org.apache.hadoop.util.ExitUtil.ExitException}
 which can be passed down to {@link org.apache.hadoop.util.ExitUtil#terminate(ExitUtil.ExitException)}
 to trigger a JVM exit. The initial exception will be used as the cause
 of the {@link org.apache.hadoop.util.ExitUtil.ExitException}.
 </p>

 <p>
 If the exception is already an instance or subclass of 
 {@link org.apache.hadoop.util.ExitUtil.ExitException}, it is passed
 directly to {@link org.apache.hadoop.util.ExitUtil.ExitException} without
 any conversion. One such subclass, {@link org.apache.hadoop.service.launcher.ServiceLaunchException}</p>
 may be useful: it includes formatted exception message generation. 
 It also declares that it extends {@link org.apache.hadoop.service.launcher.LauncherExitCodes}
 interface listing common exception codes. These are exception codes
 that can be raised by the {@link org.apache.hadoop.service.launcher.ServiceLauncher}
 itself to indicate problems during parsing the command line, creating
 the service instance &c. There are also some common exit codes
 for Hadoop/YARN service failures, such as {@link org.apache.hadoop.service.launcher.LauncherExitCodes#EXIT_CONNECTIVITY_PROBLEM}.
 Note that {@link org.apache.hadoop.util.ExitUtil.ExitException} itself
 implements {@link org.apache.hadoop.util.ExitCodeProvider#getExitCode()}

 <p>
 If an exception does not implement  {@link org.apache.hadoop.util.ExitCodeProvider#getExitCode()},
 it is is wrapped in an {@link org.apache.hadoop.util.ExitUtil.ExitException} with
 the exit code {@link org.apache.hadoop.service.launcher.LauncherExitCodes#EXIT_EXCEPTION_THROWN}.
 
 
 </p>

 <h2>Interrupt Handling</h2>

 The Service Launcher has a helper class, the {@link org.apache.hadoop.service.launcher.InterruptEscalator}
 to handle the standard SIGKILL signal and control-C signals. This class
 Registers for signal callbacks from these signals, and, when received, attempts
 to stop the service in a limited period of time, then triggers a JVM shutdown
 by way of {@link org.apache.hadoop.util.ExitUtil#terminate(int, String)}
 <p>
 If a second signal is received, the {@link org.apache.hadoop.service.launcher.InterruptEscalator}
 reacts by triggering an immediate JVM halt, invoking 
 {@link org.apache.hadoop.util.ExitUtil#halt(int, String)}. This escalation
 process is designed to address the situation in which a shutdown-hook can
 block, yet the caller (such as an init.d daemon) wishes to kill the process.
 The shutdown script should repeat the kill signal after a chosen time period,
 to trigger the more aggressive process halt. The exit code will always be
 {@link LauncherExitCodes#EXIT_INTERRUPTED}.
 </p>
 <p>
 The {@link org.apache.hadoop.service.launcher.ServiceLauncher} also registers
 a {@link org.apache.hadoop.service.launcher.ServiceShutdownHook} with the
 Hadoop shutdown hook manager, unregistering it afterwards. This hook will
 stop the service if a shutdown request is received -so ensuring that 
 if the JVM is exited by any thread, an attempt to shut down the service
 will be made.
 </p>

 <h2>Configuration class creation</h2>
 
 The Configuration class used to initialize a service is a basic
 {@link org.apache.hadoop.conf.Configuration} instance. As the launcher is
 the entry point for an application, this implies that the HDFS, YARN or other
 default configurations will not have been forced in through the constructors
 of <code>HdfsConfiguration</code> or <code>YARNConfiguration</code>.
 <p>
 There are three strategies for dealing with this
 <p>
 Subclass the Service launcher and override its {@link org.apache.hadoop.service.launcher.ServiceLauncher#createConfiguration()}
 method with one that creates the right configuration. This is good if a single
 launcher can be created for all services launched by a module, such as
 HDFS or YARN. It does imply a dedicated script to invoke the custom main() method.
 <p>
 In the {@link org.apache.hadoop.service.launcher.LaunchableService#bindArgs(Configuration, List)},
 a new configuration is created:
 
 <pre>
 public Configuration bindArgs(Configuration config, List<String> args) throws
 Exception {
   Configuration newConf = new YARNConfiguration(config);
   return newConf;
 }
 </pre>
 
 This guarantees a configuration of the right type is generated for all
 instances created via the service launcher. It does imply that this is
 expected to be only way that services will be launched.
 
 <pre>
 protected void serviceInit(Configuration conf) throws Exception {
   super.serviceInit(new YARNConfiguration(conf));
 }
 </pre>
 <p>
 This is a strategy used by many existing YARN services, and is ideal for
 services which to not implement the LaunchableService interface. Its one
 weakness is that the configuration is now private to that instance. Some
 YARN services use a single shared configuration instance as a way of propagating
 information between peer services in a {@link org.apache.hadoop.service.CompositeService}.
 While a dangerous practice, it does happen.
 
 </p>

 <h2>Configuration file loading</h2>
 
 Before the service is bound to the CLI, the ServiceLauncher scans through
 all the arguments after the first one, looking for instances of the argument
 {@link org.apache.hadoop.service.launcher.ServiceLauncher#ARG_CONF}
 argument pair: <code>--conf &lt;file&gt;</code>. This must refer to a file
 in the local filesystem which exists. It will be loaded into the Hadoop configuration
 class (the one created by the {@link org.apache.hadoop.service.launcher.ServiceLauncher#createConfiguration()}
 method. If this argument is repeated multiple times, all configuration
 files are merged -with the latest file on the command line being the 
 last one to be applied.
 
 All the <code>--conf &lt;file&gt;</code> argument pairs are stripped off
 the argument list provided to the instantiated service; they get the
 merged configuration, but not the commands used to create it.

 */

