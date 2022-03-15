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

Proxy user - Superusers Acting On Behalf Of Other Users
=======================================================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Introduction
------------

This document describes how a superuser can submit jobs or access hdfs on behalf of another user.

Use Case
--------

The code example described in the next section is applicable for the following use case.

A superuser with username 'super' wants to submit job and access hdfs on behalf of a user joe. The superuser has kerberos credentials but user joe doesn't have any. The tasks are required to run as user joe and any file accesses on namenode are required to be done as user joe. It is required that user joe can connect to the namenode or job tracker on a connection authenticated with super's kerberos credentials. In other words super is impersonating the user joe.

Some products such as Apache Oozie need this.

Code example
------------

In this example super's credentials are used for login and a proxy user ugi object is created for joe. The operations are performed within the doAs method of this proxy user ugi object.

        ...
        //Create ugi for joe. The login user is 'super'.
        UserGroupInformation ugi =
                UserGroupInformation.createProxyUser("joe", UserGroupInformation.getLoginUser());
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          public Void run() throws Exception {
            //Submit a job
            JobClient jc = new JobClient(conf);
            jc.submitJob(conf);
            //OR access hdfs
            FileSystem fs = FileSystem.get(conf);
            fs.mkdir(someFilePath);
          }
        }

Configurations
--------------

You can configure proxy user using properties `hadoop.proxyuser.$superuser.hosts` along with either or both of `hadoop.proxyuser.$superuser.groups` and `hadoop.proxyuser.$superuser.users`.

By specifying as below in core-site.xml, the superuser named `super` can connect only from `host1` and `host2` to impersonate a user belonging to `group1` and `group2`.

       <property>
         <name>hadoop.proxyuser.super.hosts</name>
         <value>host1,host2</value>
       </property>
       <property>
         <name>hadoop.proxyuser.super.groups</name>
         <value>group1,group2</value>
       </property>

If these configurations are not present, impersonation will not be allowed and connection will fail.

If more lax security is preferred, the wildcard value \* may be used to allow impersonation from any host or of any user. For example, by specifying as below in core-site.xml, user named `oozie` accessing from any host can impersonate any user belonging to any group.

      <property>
        <name>hadoop.proxyuser.oozie.hosts</name>
        <value>*</value>
      </property>
      <property>
        <name>hadoop.proxyuser.oozie.groups</name>
        <value>*</value>
      </property>

The `hadoop.proxyuser.$superuser.hosts` accepts list of ip addresses, ip address ranges in CIDR format and/or host names. For example, by specifying as below, user named `super` accessing from hosts in the range `10.222.0.0-10.222.255.255` and `10.113.221.221` can impersonate `user1` and `user2`.

       <property>
         <name>hadoop.proxyuser.super.hosts</name>
         <value>10.222.0.0/16,10.113.221.221</value>
       </property>
       <property>
         <name>hadoop.proxyuser.super.users</name>
         <value>user1,user2</value>
       </property>

Caveats
-------

If the cluster is running in [Secure Mode](./SecureMode.html), the superuser must have kerberos credentials to be able to impersonate another user.

It cannot use delegation tokens for this feature. It would be wrong if superuser adds its own delegation token to the proxy user ugi, as it will allow the proxy user to connect to the service with the privileges of the superuser.

However, if the superuser does want to give a delegation token to joe, it must first impersonate joe and get a delegation token for joe, in the same way as the code example above, and add it to the ugi of joe. In this way the delegation token will have the owner as joe.
