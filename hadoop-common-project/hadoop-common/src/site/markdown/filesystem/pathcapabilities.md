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

# <a name="PathCapabilities"></a> interface `PathCapabilities`

The `PathCapabilities` interface provides a way to programmatically query the
operations offered under a given path by an instance of `FileSystem`, `FileContext`
or other implementing class.

```java
public interface PathCapabilities {
  boolean hasPathCapability(Path path, String capability)
      throws IOException;
}
```

There are a number of goals here:

1. Allow callers to probe for optional filesystem operations without actually
having to invoke them.
1. Allow filesystems with their own optional per-instance features to declare
whether or not they are active for the specific instance.
1. Allow for filesystem connectors which work with object stores to expose the
fundamental difference in semantics of these stores (e.g: files not visible
until closed, file rename being `O(data)`), directory rename being non-atomic,
etc.

### Available Capabilities

Capabilities are defined as strings and split into "Common Capabilites"
and non-standard ones for a specific store.

The common capabilities are all defined under the prefix `fs.capability.`

Consult the javadocs for `org.apache.hadoop.fs.CommonPathCapabilities` for these.


Individual filesystems MAY offer their own set of capabilities which
can be probed for. These MUST begin with `fs.` + the filesystem scheme +
 `.capability`. For example `fs.s3a.capability.select.sql`;

### `boolean hasPathCapability(path, capability)`

Probe for the instance offering a specific capability under the
given path.

#### Postconditions

```python
if fs_supports_the_feature(path, capability):
  return True
else:
  return False
```

Return: `True`, iff the specific capability is available.

A filesystem instance *MUST NOT* return `True` for any capability unless it is
known to be supported by that specific instance. As a result, if a caller
probes for a capability then it can assume that the specific feature/semantics
are available.

If the probe returns `False` then it can mean one of:

1. The capability is unknown.
1. The capability is known, and known to be unavailable on this instance.
1. The capability is known but this local class does not know if it is supported
   under the supplied path.

This predicate is intended to be low cost. If it requires remote calls other
than path/link resolution, it SHOULD conclude that the availability
of the feature is unknown and return `False`.

The predicate MUST also be side-effect free.

*Validity of paths*
There is no requirement that the existence of the path must be checked;
the parameter exists so that any filesystem which relays operations to other
filesystems (e.g `viewfs`) can resolve and relay it to the nested filesystem.
Consider the call to be *relatively* lightweight.

Because of this, it may be that while the filesystem declares that
it supports a capability under a path, the actual invocation of the operation
may fail for other reasons.

As an example, while a filesystem may support `append()` under a path,
if invoked on a directory, the call may fail.

That is for a path `root = new Path("/")`: the capabilities call may succeed

```java
fs.hasCapabilities(root, "fs.capability.append") == true
```

But a subsequent call to the operation on that specific path may fail,
because the root path is a directory:

```java
fs.append(root)
```


Similarly, there is no checking that the caller has the permission to
perform a specific operation: just because a feature is available on that
path does not mean that the caller can execute the operation.

The `hasCapabilities(path, capability)` probe is therefore declaring that
the operation will not be rejected as unsupported, not that a specific invocation
will be permitted on that path by the caller.

*Duration of availability*

As the state of a remote store changes,so may path capabilities. This
may be due to changes in the local state of the filesystem (e.g. symbolic links
or mount points changing), or changes in its functionality (e.g. a feature
becoming availaible/unavailable due to operational changes, system upgrades, etc.)

*Capabilities which must be invoked to determine availablity*

Some operations may be known by the client connector, and believed to be available,
but may actually fail when invoked due to the state and permissons of the remote
store —state which is cannot be determined except by attempting
side-effecting operations.

A key example of this is symbolic links and the local filesystem.
The filesystem declares that it supports this unless symbolic links are explicitly
disabled —when invoked they may actually fail.

### Implementors Notes

Implementors *MUST NOT* return `true` for any capability which is not guaranteed
to be supported. To return `true` indicates that the implementation/deployment
of the filesystem does, to the best of the knowledge of the filesystem client,
offer the desired operations *and semantics* queried for.

For performance reasons, implementations *SHOULD NOT* check the path for
existence, unless it needs to resolve symbolic links in parts of the path
to determine whether a feature is present. This is required of `FileContext`
and `viewfs`.

Individual filesystems *MUST NOT* unilaterally define new `fs.capability`-prefixed
capabilities. Instead they *MUST* do one of the following:

* Define and stabilize new cross-filesystem capability flags (preferred),
and so formally add a new `fs.capability` value.
* Use the scheme of the filesystem to as a prefix for their own options,
e.g `fs.hdfs.`
