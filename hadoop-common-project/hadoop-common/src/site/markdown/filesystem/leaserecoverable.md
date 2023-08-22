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

# <a name="LeaseRecoverable"></a> interface `LeaseRecoverable`

The `LeaseRecoverable` interface tells whether a given path of current filesystem can perform lease
recovery for open file that a lease is not explicitly renewed or the client holding it goes away.

This interface should be implemented accordingly when necessary to any Filesystem that supports
lease recovery, e.g. `DistributedFileSystem` (HDFS) and `ViewDistributedFileSystem`.

```java
public interface LeaseRecoverable {
  boolean recoverLease(Path file) throws IOException;
  boolean isFileClosed(Path file) throws IOException;
}
```

There are two main functions of this interface, one performs lease recovery and another one
verifies if a file has been closed.

### boolean recoverLease(Path file)

This function performs the lease recovery for the given file path, and it does not support
directory path recovery.
1. Return `true`, if the file has already closed, or does not require lease recovery.
1. Return `false`, if the lease recovery is yet completed.
1. Throw `IOException` if a directory path is given as input.

### boolean isFileClosed(Path file)

This function only checks if the give file path has been closed, and it does not support directory
verification.
1. Return `true`, if the file has been closed.
1. Return `false`, if the file is still open.
1. Throw `IOException` if a directory path is given as input.

### Path Capabilities SHOULD BE declared

If a filesystem supports `LeaseRecoverable`, it should return `true` to
`PathCapabilities.hasPathCapability(path, "fs.capability.lease.recoverable")` for a given path.