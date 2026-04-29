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

# <a name="TrashPolicy"></a> class `org.apache.hadoop.fs.TrashPolicy`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

`TrashPolicy` is the public API for implementing filesystem trash behavior.
It defines how paths are moved into trash, where the current trash directory
is located, how trash checkpoints are created and deleted, and how an emptier
process removes expired checkpoints.

The `Trash` class delegates trash operations to a `TrashPolicy` instance.
Applications and filesystems can supply different policy implementations when
the default policy does not match the filesystem's layout or deletion semantics.

```java
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TrashPolicy extends Configured {
  public abstract void initialize(Configuration conf, FileSystem fs, Path home);
  public void initialize(Configuration conf, FileSystem fs);
  public abstract boolean isEnabled();
  public abstract boolean moveToTrash(Path path) throws IOException;
  public abstract void createCheckpoint() throws IOException;
  public abstract void deleteCheckpoint() throws IOException;
  public abstract void deleteCheckpointsImmediately() throws IOException;
  public abstract Path getCurrentTrashDir();
  public Path getCurrentTrashDir(Path path) throws IOException;
  public abstract Runnable getEmptier() throws IOException;
  public final long getDeletionInterval();
}
```

## Policy Selection

A `TrashPolicy` is associated with a `FileSystem` instance. The preferred
factory path is:

```java
TrashPolicy policy = TrashPolicy.getInstance(conf, fs);
```

This delegates policy selection to `FileSystem#getTrashPolicy(Path, Configuration)`.
The default `FileSystem` implementation reads the configuration key
`fs.trash.classname`, instantiates the configured class, initializes it with
the supplied `Configuration` and `FileSystem`, and returns the initialized
policy. Filesystems MAY override `getTrashPolicy(Path, Configuration)` to use
filesystem-specific configuration keys or default policy classes.

Filesystem implementations with multiple child filesystems, such as
`ViewFileSystem`, SHOULD NOT choose a policy directly. The trash operation
SHOULD resolve to the underlying filesystem before policy selection.

## Initialization

The policy returned by `TrashPolicy.getInstance(Configuration, FileSystem)` or
`FileSystem#getTrashPolicy(Path, Configuration)` MUST be initialized and ready
for use.

Implementations MUST implement `initialize(Configuration, FileSystem)`.
This method initializes the policy's filesystem, deletion interval, checkpoint
interval, and any implementation-specific state required by later operations.
The deprecated `initialize(Configuration, FileSystem, Path)` method is retained
for compatibility with older callers.

The deletion interval is exposed through `getDeletionInterval()`.
A deletion interval of zero means trash is disabled.
The deletion interval MUST NOT be negative after initialization.

## `boolean isEnabled()`

Returns whether trash is enabled for this policy and filesystem instance.

#### Postconditions

```python
if getDeletionInterval() > 0:
  return True
else:
  return False
```

## `boolean moveToTrash(Path path)`

Move a file or directory to the current trash directory.

#### Preconditions

The path MUST exist.

#### Postconditions

If `isEnabled()` is false, return false without side effects.

If the path is already under `FileSystem#getTrashRoot(Path)`, return false
without side effects.

Otherwise, move the path under `getCurrentTrashDir(Path)` and return true.
After a successful move:

```python
exists(FS, path) == False
exists(FS, Path.mergePaths(getCurrentTrashDir(path), path)) == True
```

The move may fail with an `IOException` if the filesystem cannot create the
trash directory or cannot rename the path into trash.

## Trash Directories

`getCurrentTrashDir()` returns the current trash directory used by older
callers.

`getCurrentTrashDir(Path path)` returns the current trash directory for the
specific path being deleted. New callers SHOULD use the path-specific method,
because the correct trash directory may depend on the path. For example, HDFS
encryption zones and filesystems with multiple trash roots may need the path
to choose a valid trash location.

## Checkpoints

Trash checkpoints are previous current-trash directories retained until they
expire. A policy implementation controls the naming and layout of checkpoints.

### `createCheckpoint()`

Create a checkpoint from the current trash directory.

If there is no current trash directory, this operation is a no-op.

If a checkpoint is created, it MUST NOT be equal to `getCurrentTrashDir(Path)`.

### `deleteCheckpoint()`

Delete expired checkpoint directories under the trash roots returned by
`FileSystem#getTrashRoots(boolean)`.

The expiration age is determined by `getDeletionInterval()`.
The current trash directory MUST NOT be deleted by this operation.

### `deleteCheckpointsImmediately()`

Delete checkpoint directories under the trash roots returned by
`FileSystem#getTrashRoots(boolean)`, regardless of checkpoint timestamp.

The current trash directory MUST NOT be deleted by this operation.

## Emptier

`getEmptier()` returns a `Runnable` that periodically empties trash.
It is intended to be run by a process with permission to enumerate and clean
trash roots, such as the filesystem superuser.

The effective emptier interval SHOULD be in the range
`[0, getDeletionInterval()]`.

If the interval is zero, the runnable is a no-op and returns immediately.
If the interval is non-zero, the runnable SHOULD repeat until interrupted.
For each interval it:

1. Checks trash root directories through `FileSystem#getTrashRoots(boolean)`.
1. Deletes checkpoints older than `getDeletionInterval()`.
1. Creates a new checkpoint from the current trash directory.
1. Leaves unexpired checkpoints unchanged.

## Default Policy

`TrashPolicyDefault` is the default implementation. It is selected by the
configuration key `fs.trash.classname` when no filesystem-specific override is
provided.

It uses `fs.trash.interval` as the deletion interval. A positive value enables
trash; zero disables trash.

It uses `fs.trash.checkpoint.interval` as the emptier checkpoint interval. If
that interval is zero or negative, the default policy uses the deletion interval
as the checkpoint interval.
