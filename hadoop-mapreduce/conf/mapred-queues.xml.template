<?xml version="1.0"?>
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->
<!-- This is the template for queue configuration. The format supports nesting of
     queues within queues - a feature called hierarchical queues. All queues are
     defined within the 'queues' tag which is the top level element for this
     XML document. The queue acls configured here for different queues are
     checked for authorization only if the configuration property
     mapreduce.cluster.acls.enabled is set to true. -->
<queues>

  <!-- Configuration for a queue is specified by defining a 'queue' element. -->
  <queue>

    <!-- Name of a queue. Queue name cannot contain a ':'  -->
    <name>default</name>

    <!-- properties for a queue, typically used by schedulers,
    can be defined here -->
    <properties>
    </properties>

	<!-- State of the queue. If running, the queue will accept new jobs.
         If stopped, the queue will not accept new jobs. -->
    <state>running</state>

    <!-- Specifies the ACLs to check for submitting jobs to this queue.
         If set to '*', it allows all users to submit jobs to the queue.
         If set to ' '(i.e. space), no user will be allowed to do this
         operation. The default value for any queue acl is ' '.
         For specifying a list of users and groups the format to use is
         user1,user2 group1,group2

         It is only used if authorization is enabled in Map/Reduce by setting
         the configuration property mapreduce.cluster.acls.enabled to true.

         Irrespective of this ACL configuration, the user who started the
         cluster and cluster administrators configured via
         mapreduce.cluster.administrators can do this operation. -->
    <acl-submit-job> </acl-submit-job>

    <!-- Specifies the ACLs to check for viewing and modifying jobs in this
         queue. Modifications include killing jobs, tasks of jobs or changing
         priorities.
         If set to '*', it allows all users to view, modify jobs of the queue.
         If set to ' '(i.e. space), no user will be allowed to do this
         operation.
         For specifying a list of users and groups the format to use is
         user1,user2 group1,group2

         It is only used if authorization is enabled in Map/Reduce by setting
         the configuration property mapreduce.cluster.acls.enabled to true.

         Irrespective of this ACL configuration, the user who started the
         cluster  and cluster administrators configured via
         mapreduce.cluster.administrators can do the above operations on all
         the jobs in all the queues. The job owner can do all the above
         operations on his/her job irrespective of this ACL configuration. -->
    <acl-administer-jobs> </acl-administer-jobs>
  </queue>

  <!-- Here is a sample of a hierarchical queue configuration
       where q2 is a child of q1. In this example, q2 is a leaf level
       queue as it has no queues configured within it. Currently, ACLs
       and state are only supported for the leaf level queues.
       Note also the usage of properties for the queue q2.
  <queue>
    <name>q1</name>
    <queue>
      <name>q2</name>
      <properties>
        <property key="capacity" value="20"/>
        <property key="user-limit" value="30"/>
      </properties>
    </queue>
  </queue>
 -->
</queues>
