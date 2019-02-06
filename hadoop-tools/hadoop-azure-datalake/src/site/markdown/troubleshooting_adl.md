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

# Troubleshooting ADL

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->


## Error messages


### Error fetching access token:

You aren't authenticated.

### Error fetching access token:  JsonParseException

This means a problem talking to the oauth endpoint.


```
Operation null failed with exception com.fasterxml.jackson.core.JsonParseException : Unexpected character ('<' (code 60)): expected a valid value (number, String, array, object, 'true', 'false' or 'null')
  at [Source: sun.net.www.protocol.http.HttpURLConnection$HttpInputStream@211d30ed; line: 3, column: 2]
  Last encountered exception thrown after 5 tries. [com.fasterxml.jackson.core.JsonParseException,com.fasterxml.jackson.core.JsonParseException,com.fasterxml.jackson.core.JsonParseException,com.fasterxml.jackson.core.JsonParseException,com.fasterxml.jackson.core.JsonParseException]
  [ServerRequestId:null]
  at com.microsoft.azure.datalake.store.ADLStoreClient.getExceptionFromResponse(ADLStoreClient.java:1147)
  at com.microsoft.azure.datalake.store.ADLStoreClient.getDirectoryEntry(ADLStoreClient.java:725)
  at org.apache.hadoop.fs.adl.AdlFileSystem.getFileStatus(AdlFileSystem.java:476)
  at org.apache.hadoop.fs.FileSystem.exists(FileSystem.java:1713)
  at org.apache.hadoop.fs.contract.ContractTestUtils.rm(ContractTestUtils.java:397)
  at org.apache.hadoop.fs.contract.ContractTestUtils.cleanup(ContractTestUtils.java:374)
  at org.apache.hadoop.fs.contract.AbstractFSContractTestBase.deleteTestDirInTeardown(AbstractFSContractTestBase.java:213)
  at org.apache.hadoop.fs.contract.AbstractFSContractTestBase.teardown(AbstractFSContractTestBase.java:204)
  at org.apache.hadoop.fs.contract.AbstractContractOpenTest.teardown(AbstractContractOpenTest.java:64)
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:498)
  at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:47)
  at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
  at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:44)
  at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:33)
  at org.junit.rules.TestWatcher$1.evaluate(TestWatcher.java:55)
  at org.junit.internal.runners.statements.FailOnTimeout$StatementThread.run(FailOnTimeout.java:74)
  ```

The endpoint for token refresh is wrong; the web site at the far end is returning HTML, which breaks the JSON parser.
Fix: get the right endpoint from the web UI; make sure it ends in `oauth2/token`.

If there is a proxy betwen the application and ADL, make sure that the JVM proxy
settings are correct.

### `UnknownHostException : yourcontainer.azuredatalakestore.net`

The name of the ADL container is wrong, and does not resolve to any known container.


```
Operation MKDIRS failed with exception java.net.UnknownHostException : yourcontainer.azuredatalakestore.net
Last encountered exception thrown after 5 tries. [java.net.UnknownHostException,java.net.UnknownHostException,java.net.UnknownHostException,java.net.UnknownHostException,java.net.UnknownHostException]
  [ServerRequestId:null]
  at com.microsoft.azure.datalake.store.ADLStoreClient.getExceptionFromResponse(ADLStoreClient.java:1147)
  at com.microsoft.azure.datalake.store.ADLStoreClient.createDirectory(ADLStoreClient.java:582)
  at org.apache.hadoop.fs.adl.AdlFileSystem.mkdirs(AdlFileSystem.java:598)
  at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:2305)
  at org.apache.hadoop.fs.contract.AbstractFSContractTestBase.mkdirs(AbstractFSContractTestBase.java:338)
  at org.apache.hadoop.fs.contract.AbstractFSContractTestBase.setup(AbstractFSContractTestBase.java:193)
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:498)
  at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:47)
  at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
  at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:44)
  at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:24)
  at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)
  at org.junit.rules.TestWatcher$1.evaluate(TestWatcher.java:55)
  at org.junit.internal.runners.statements.FailOnTimeout$StatementThread.run(FailOnTimeout.java:74)
Caused by: java.net.UnknownHostException: yourcontainer.azuredatalakestore.net
  at java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:184)
  at java.net.SocksSocketImpl.connect(SocksSocketImpl.java:392)
  at java.net.Socket.connect(Socket.java:589)
  at sun.security.ssl.SSLSocketImpl.connect(SSLSocketImpl.java:668)
  at sun.net.NetworkClient.doConnect(NetworkClient.java:175)
  at sun.net.www.http.HttpClient.openServer(HttpClient.java:432)
  at sun.net.www.http.HttpClient.openServer(HttpClient.java:527)
  at sun.net.www.protocol.https.HttpsClient.<init>(HttpsClient.java:264)
  at sun.net.www.protocol.https.HttpsClient.New(HttpsClient.java:367)
  at sun.net.www.protocol.https.AbstractDelegateHttpsURLConnection.getNewHttpClient(AbstractDelegateHttpsURLConnection.java:191)
  at sun.net.www.protocol.http.HttpURLConnection.plainConnect0(HttpURLConnection.java:1138)
  at sun.net.www.protocol.http.HttpURLConnection.plainConnect(HttpURLConnection.java:1032)
  at sun.net.www.protocol.https.AbstractDelegateHttpsURLConnection.connect(AbstractDelegateHttpsURLConnection.java:177)
  at sun.net.www.protocol.http.HttpURLConnection.getOutputStream0(HttpURLConnection.java:1316)
  at sun.net.www.protocol.http.HttpURLConnection.getOutputStream(HttpURLConnection.java:1291)
  at sun.net.www.protocol.https.HttpsURLConnectionImpl.getOutputStream(HttpsURLConnectionImpl.java:250)
  at com.microsoft.azure.datalake.store.HttpTransport.makeSingleCall(HttpTransport.java:273)
  at com.microsoft.azure.datalake.store.HttpTransport.makeCall(HttpTransport.java:91)
  at com.microsoft.azure.datalake.store.Core.mkdirs(Core.java:399)
  at com.microsoft.azure.datalake.store.ADLStoreClient.createDirectory(ADLStoreClient.java:580)
  ... 15 more
```

### ACL verification failed


You are logged in but have no access to the ADL container.

```
[ERROR] testOpenReadZeroByteFile(org.apache.hadoop.fs.adl.live.TestAdlContractOpenLive)  Time elapsed: 3.392 s  <<< ERROR!
org.apache.hadoop.security.AccessControlException: MKDIRS failed with error 0x83090aa2 (Forbidden. ACL verification failed. Either the resource does not exist or the user is not authorized to perform the requested operation.). [709ad9f6-725f-45a8-8231-e9327c52e79f][2017-11-28T07:06:30.3068084-08:00] [ServerRequestId:709ad9f6-725f-45a8-8231-e9327c52e79f]
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at com.microsoft.azure.datalake.store.ADLStoreClient.getRemoteException(ADLStoreClient.java:1167)
  at com.microsoft.azure.datalake.store.ADLStoreClient.getExceptionFromResponse(ADLStoreClient.java:1132)
  at com.microsoft.azure.datalake.store.ADLStoreClient.createDirectory(ADLStoreClient.java:582)
  at org.apache.hadoop.fs.adl.AdlFileSystem.mkdirs(AdlFileSystem.java:598)
  at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:2305)
  at org.apache.hadoop.fs.contract.AbstractFSContractTestBase.mkdirs(AbstractFSContractTestBase.java:338)
  at org.apache.hadoop.fs.contract.AbstractFSContractTestBase.setup(AbstractFSContractTestBase.java:193)
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:498)
  at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:47)
  at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
  at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:44)
  at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:24)
  at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)
  at org.junit.rules.TestWatcher$1.evaluate(TestWatcher.java:55)
  at org.junit.internal.runners.statements.FailOnTimeout$StatementThread.run(FailOnTimeout.java:74)
```

See "Adding the service principal to your ADL Account".

## Timeouts

The timeout used by the ADL SDK can be overridden with the hadoop property
`adl.http.timeout`.  Some timeouts in compute frameworks may need to be
addressed by lowering the timeout used by the SDK.  A lower timeout at the
storage layer may allow more retries to be attempted and actually increase
the likelihood of success before hitting the framework's timeout, as attempts
that may ultimately fail will fail faster.
