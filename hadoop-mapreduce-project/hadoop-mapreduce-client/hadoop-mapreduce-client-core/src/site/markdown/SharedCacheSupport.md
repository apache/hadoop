<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

MR Support for YARN Shared Cache
==================

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

Overview
-------

MapReduce support for the YARN shared cache allows MapReduce jobs to take advantage
of additional resource caching. This saves network bandwidth between the job
submission client as well as within the YARN cluster itself. This will reduce job
submission time and overall job runtime.


Enabling/Disabling the shared cache
-------

First, your YARN cluster must have the shared cache service running. Please see YARN documentation
for information on how to setup the shared cache service.

A MapReduce user can specify what resources are eligible to be uploaded to the shared cache
based on resource type. This is done using a configuration parameter in mapred-site.xml:

```
<property>
    <name>mapreduce.job.sharedcache.mode</name>
    <value>disabled</value>
    <description>
       A comma delimited list of resource categories to submit to the
       shared cache. The valid categories are: jobjar, libjars, files,
       archives. If "disabled" is specified then the job submission code
       will not use the shared cache.
    </description>
</property>
```

If a resource type is listed, it will check the shared cache to see if the resource is already in the
cache. If so, it will use the cached resource, if not, it will specify that the resource needs to be
uploaded asynchronously.

Specifying resources for the cache
-------

A MapReduce user has 3 ways to specify resources for a MapReduce job:

1. **The command line via the generic options parser (i.e. -files, -archives, -libjars):** If a
resource is specified via the command line and the resource type is enabled for the
shared cache, that resource will use the shared cache.
2. **The distributed cache api:** If a resource is specified via the distributed cache the
resource will not use the shared cache regardless of if the resource type is enabled for
the shared cache.
3. **The shared cache api:** This is a new set of methods added to the
org.apache.hadoop.mapreduce.Job api. It allows users to add a file to the shared cache,
add it to the shared cache and the classpath and add an archive to the shared cache.
These resources will be placed in the distributed cache and, if their resource type is
enabled the client will use the shared cache as well.

Resource naming
-------

It is important to ensure that each resource for a MapReduce job has a unique file name.
This prevents symlink clobbering when YARN containers running MapReduce tasks are localized
during container launch. A user can specify their own resource name by using the fragment
portion of a URI. For example, for file resources specified on the command line, it could look
like this:
```
-files /local/path/file1.txt#foo.txt,/local/path2/file1.txt#bar.txt
```
In the above example two files, named file1.txt, will be localized with two different names: foo.txt
and bar.txt.

Resource Visibility
-------

All resources in the shared cache have a PUBLIC visibility.


MapReduce client behavior while the shared cache is unavailable
-------

In the event that the shared cache manager is unavailable, the MapReduce client uses a fail-fast
mechanism. If the MapReduce client fails to contact the shared cache manager, the client will
no longer use the shared cache for the rest of that job submission. This
prevents the MapReduce client from timing out each time it tries to check for a resource
in the shared cache. The MapReduce client quickly reverts to the default behavior and submits a
Job as if the shared cache was never enabled in the first place.