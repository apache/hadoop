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


<!--  ============================================================= -->
<!--  CLASS: FileSystem -->
<!--  ============================================================= -->

# interface `org.apache.hadoop.fs.Abortable`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

Abort the active operation such that the output does not become
manifest.

Specifically, if supported on an [output stream](outputstream.html),
a successful `abort()` MUST guarantee that the stream will not be made visible in the `close()`
operation.

```java

@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface Abortable {

  /**
   * Abort the active operation without the output becoming visible.
   *
   * This is to provide ability to cancel the write on stream; once
   * a stream is aborted, the write MUST NOT become visible.
   *
   * @throws UnsupportedOperationException if the operation is not supported.
   * @return the result.
   */
  AbortableResult abort();

  /**
   * Interface for the result of aborts; allows subclasses to extend
   * (IOStatistics etc) or for future enhancements if ever needed.
   */
  interface AbortableResult {

    /**
     * Was the stream already closed/aborted?
     * @return true if a close/abort operation had already
     * taken place.
     */
    boolean alreadyClosed();

    /**
     * Any exception caught during cleanup operations,
     * exceptions whose raising/catching does not change
     * the semantics of the abort.
     * @return an exception or null.
     */
    IOException anyCleanupException();
  }
}
```

## Method `abort()`

Aborts the ongoing operation such that no output SHALL become visible
when the operation is completed.

Unless and until other File System classes implement `Abortable`, the
interface is specified purely for output streams.

## Method `abort()` on an output stream

`Abortable.abort()` MUST only be supported on output streams
whose output is only made visible when `close()` is called,
for example. output streams returned by the S3A FileSystem.

## Preconditions

The stream MUST implement `Abortable` and `StreamCapabilities`.

```python
 if unsupported:
  throw UnsupportedException

if not isOpen(stream):
  no-op

StreamCapabilities.hasCapability("fs.capability.outputstream.abortable") == True

```


## Postconditions

After `abort()` returns, the filesystem MUST be unchanged:

```
FS' = FS
```

A successful `abort()` operation MUST guarantee that
when the stream` close()` is invoked no output shall be manifest.

* The stream MUST retry any remote calls needed to force the abort outcome.
* If any file was present at the destination path, it MUST remain unchanged.

Strictly then:

> if `Abortable.abort()` does not raise `UnsupportedOperationException`
> then returns, then it guarantees that the write SHALL NOT become visible
> and that any existing data in the filesystem at the destination path SHALL
> continue to be available.


1. Calls to `write()` methods MUST fail.
1. Calls to `flush()` MUST be no-ops (applications sometimes call this on closed streams)
1. Subsequent calls to `abort()` MUST be no-ops.
1. `close()` MUST NOT manifest the file, and MUST NOT raise an exception

That is, the postconditions of `close()` becomes:

```
FS' = FS
```

### Cleanup

* If temporary data is stored in the local filesystem or in the store's upload
  infrastructure then this MAY be cleaned up; best-effort is expected here.

* The stream SHOULD NOT retry cleanup operations; any failure there MUST be
  caught and added to `AbortResult`

#### Returned `AbortResult`

The `AbortResult` value returned is primarily for testing and logging.

`alreadyClosed()`: MUST return `true` if the write had already been aborted or closed;

`anyCleanupException();`: SHOULD return any IOException raised during any optional
cleanup operations.


### Thread safety and atomicity

Output streams themselves aren't formally required to  be thread safe,
but as applications do sometimes assume they are, this call MUST be thread safe.

## Path/Stream capability "fs.capability.outputstream.abortable"


An application MUST be able to verify that a stream supports the `Abortable.abort()`
operation without actually calling it. This is done through the `StreamCapabilities`
interface.

1. If a stream instance supports `Abortable` then it MUST return `true`
in the probe `hasCapability("fs.capability.outputstream.abortable")`

1. If a stream instance does not support `Abortable` then it MUST return `false`
in the probe `hasCapability("fs.capability.outputstream.abortable")`

That is: if a stream declares its support for the feature, a call to `abort()`
SHALL meet the defined semantics of the operation.

FileSystem/FileContext implementations SHOULD declare support similarly, to
allow for applications to probe for the feature in the destination directory/path.

If a filesystem supports `Abortable` under a path `P` then it SHOULD return `true` to
`PathCababilities.hasPathCapability(path, "fs.capability.outputstream.abortable")`
This is to allow applications to verify that the store supports the feature.

If a filesystem does not support `Abortable` under a path `P` then it MUST
return `false` to
`PathCababilities.hasPathCapability(path, "fs.capability.outputstream.abortable")`



