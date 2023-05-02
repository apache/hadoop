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

# <a name="SafeMode"></a> interface `SafeMode`

The `SafeMode` interface provides a way to perform safe mode actions and obtain the
status after such actions performed to the `FileSystem`.

This is admin only interface, should be implemented accordingly when necessary to
Filesystem that support safe mode, e.g. `DistributedFileSystem` (HDFS) and
`ViewDistributedFileSystem`.

```java
public interface SafeMode {
  default boolean setSafeMode(SafeModeAction action) throws IOException {
    return setSafeMode(action, false);
  }
  boolean setSafeMode(SafeModeAction action, boolean isChecked) throws IOException;
}
```

The goals of this interface is allow any file system implementation to share the
same concept of safe mode with the following actions and states

### Safe mode actions
1. `GET`, get the safe mode status of the file system.
1. `ENTER`, enter the safe mode for the file system.
1. `LEAVE`, exit safe mode for the file system gracefully.
1. `FORCE_EXIT`, exit safe mode for the file system even if there is any ongoing data process.

### Safe mode states
1. return true, when safe mode is on.
1. return false, when safe mode is off, usually it's the result of safe mode actions
with `GET`, `LEAVE`, `FORCE_EXIT`.