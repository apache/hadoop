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

/**

 This package contains classes, interfaces and exceptions to launch
 services from the command line and tests

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
 The {@link org.apache.hadoop.service.launcher.LaunchedService} interface
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
 
 <h2>Implementing a Launchable Service</h2>
 
 
 The Service Launcher can launch <i>any YARN service</i>. It will instantiate
 the service classname provided, by zero-args constructor -preferably- falling
 back to a constructor that takes a <code>String</code> as its parameter.
 
 It will initialize the service via {@link org.apache.hadoop.service.Service#init(Configuration)},
 then start it via the {@link org.apache.hadoop.service.Service#start()} method.
 
 
 A Launchable service is one that 
 
 */


import org.apache.hadoop.conf.Configuration;