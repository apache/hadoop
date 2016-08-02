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

package org.apache.slider.server.services.workflow;

/**

<p>
 This package contains classes which can be aggregated to build up
 complex workflows of services: sequences of operations, callbacks
 and composite services with a shared lifespan.
 </p>

<h2>
 Core concepts:
</h2>


<p>
The Workflow Services are set of Hadoop YARN services, all implementing
the {@link org.apache.hadoop.service.Service} API.
They are designed to be aggregated, to be composed to produce larger
composite services which than perform ordered operations, notify other services
when work has completed, and to propagate failure up the service hierarchy.
</p>
<p>
Service instances may a limited lifespan, and may self-terminate when
they consider it appropriate.</p>
<p>
Workflow Services that have children implement the
{@link org.apache.slider.server.services.workflow.ServiceParent}
class, which provides (thread-safe) access to the children -allowing new children
to be added, and existing children to be ennumerated. The implement policies
on how to react to the termination of children -so can sequence operations
which terminate themselves when complete.
</p>

<p>
Workflow Services may be subclassed to extend their behavior, or to use them
in specific applications. Just as the standard
{@link org.apache.hadoop.service.CompositeService}
is often subclassed to aggregate child services, the
{@link org.apache.slider.server.services.workflow.WorkflowCompositeService}
can be used instead -adding the feature that failing services trigger automatic
parent shutdown. If that is the desired operational mode of a class,
swapping the composite service implementation may be sufficient to adopt it.
</p>


<h2> How do the workflow services differ from the standard YARN services? </h2>

 <p>
 
 There is exactly one standard YARN service for managing children, the
 {@link org.apache.hadoop.service.CompositeService}.
 </p><p>
 The {@link org.apache.slider.server.services.workflow.WorkflowCompositeService}
 shares the same model of "child services, all inited and started together".
 Where it differs is that if any child service stops -either due to a failure
 or to an action which invokes that service's
 {@link org.apache.hadoop.service.Service#stop()} method.
 </p>
 <p>

In contrast, the original <code>CompositeService</code> class starts its children
in its{@link org.apache.hadoop.service.Service#start()}  method, but does not
listen or react to any child service halting. As a result, changes in child 
state are not automatically detected or propagated, other than failures in
the actual init() and start() methods.
</p>

<p>
If a child service runs until completed -that is it will not be stopped until
instructed to do so, and if it is only the parent service that attempts to
stop the child, then this difference is unimportant. 
</p>
<p>

However, if any service that depends upon all it child services running -
and if those child services are written so as to stop when they fail, using
the <code>WorkflowCompositeService</code> as a base class will enable the 
parent service to be automatically notified of a child stopping.

</p>
<p>
The {@link org.apache.slider.server.services.workflow.WorkflowSequenceService}
resembles the composite service in API, but its workflow is different. It
initializes and starts its children one-by-one, only starting the second after
the first one succeeds, the third after the second, etc. If any service in
the sequence fails, the parent <code>WorkflowSequenceService</code> stops, 
reporting the same exception. 
</p>

<p>
The {@link org.apache.slider.server.services.workflow.ForkedProcessService}:
Executes a process when started, and binds to the life of that process. When the
process terminates, so does the service -and vice versa. This service enables
external processes to be executed as part of a sequence of operations -or,
using the {@link org.apache.slider.server.services.workflow.WorkflowCompositeService}
in parallel with other services, terminating the process when the other services
stop -and vice versa.
</p>

<p>
The {@link org.apache.slider.server.services.workflow.WorkflowCallbackService}
executes a {@link java.util.concurrent.Callable} callback a specified delay
after the service is started, then potentially terminates itself.
This is useful for callbacks when a workflow  reaches a specific point
-or simply for executing arbitrary code in the workflow.

 </p>


<h2>
Other Workflow Services
</h2>

There are some minor services that have proven useful within aggregate workflows,
and simply in applications which are built from composite YARN services.

 <ul>
 <li>{@link org.apache.slider.server.services.workflow.WorkflowRpcService }:
 Maintains a reference to an RPC {@link org.apache.hadoop.ipc.Server} instance.
 When the service is started, so is the RPC server. Similarly, when the service
 is stopped, so is the RPC server instance. 
 </li>

 <li>{@link org.apache.slider.server.services.workflow.ClosingService}: Closes
 an instance of {@link java.io.Closeable} when the service is stopped. This
 is purely a housekeeping class.
 </li>

 </ul>

 Lower-level classes 
 <ul>
 <li>{@link org.apache.slider.server.services.workflow.ServiceTerminatingRunnable }:
 A {@link java.lang.Runnable} which runs the runnable supplied in its constructor
 then signals its owning service to stop once that runnable is completed. 
 Any exception raised in the run is stored.
 </li>
 <li>{@link org.apache.slider.server.services.workflow.WorkflowExecutorService}:
 A base class for services that wish to have a {@link java.util.concurrent.ExecutorService}
 with a lifespan mapped to that of a service. When the service is stopped, the
 {@link java.util.concurrent.ExecutorService#shutdownNow()} method is called to
 attempt to shut down all running tasks.
 </li>
 <li>{@link org.apache.slider.server.services.workflow.ServiceThreadFactory}:
 This is a simple {@link java.util.concurrent.ThreadFactory} which generates
 meaningful thread names. It can be used as a parameter to constructors of 
 {@link java.util.concurrent.ExecutorService} instances, to ensure that
 log information can tie back text to the related services</li>
 </ul>



 */
