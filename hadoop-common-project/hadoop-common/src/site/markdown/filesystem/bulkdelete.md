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

# <a name="BulkDelete"></a> interface `BulkDelete`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

The `BulkDelete` interface provides an API to perform bulk delete of files/objects
in an object store or filesystem.

## Key Features

* An API for submitting a list of paths to delete.
* This list must be no larger than the "page size" supported by the client; This size is also exposed as a method.
* This list must not have any path outside the base path.
* Triggers a request to delete files at the specific paths.
* Returns a list of which paths were reported as delete failures by the store.
* Does not consider a nonexistent file to be a failure.
* Does not offer any atomicity guarantees.
* Idempotency guarantees are weak: retries may delete files newly created by other clients.
* Provides no guarantees as to the outcome if a path references a directory.
* Provides no guarantees that parent directories will exist after the call.


The API is designed to match the semantics of the AWS S3 [Bulk Delete](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html) REST API call, but it is not
exclusively restricted to this store. This is why the "provides no guarantees"
restrictions do not state what the outcome will be when executed on other stores.

### Interface `org.apache.hadoop.fs.BulkDeleteSource`

The interface `BulkDeleteSource` is offered by a FileSystem/FileContext class if
it supports the API. The default implementation is implemented in base FileSystem
class that returns an instance of `org.apache.hadoop.fs.impl.DefaultBulkDeleteOperation`.
The default implementation details are provided in below sections.


```java
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BulkDeleteSource {
  BulkDelete createBulkDelete(Path path)
      throws UnsupportedOperationException, IllegalArgumentException, IOException;

}

```

### Interface `org.apache.hadoop.fs.BulkDelete`

This is the bulk delete implementation returned by the `createBulkDelete()` call.

```java
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BulkDelete extends IOStatisticsSource, Closeable {
  int pageSize();
  Path basePath();
  List<Map.Entry<Path, String>> bulkDelete(List<Path> paths)
      throws IOException, IllegalArgumentException;

}

```

### `bulkDelete(paths)`

#### Preconditions

```python
if length(paths) > pageSize: throw IllegalArgumentException
```

#### Postconditions

All paths which refer to files are removed from the set of files.
```python
FS'Files = FS.Files - [paths]
```

No other restrictions are placed upon the outcome.


### Availability

The `BulkDeleteSource` interface is exported by `FileSystem` and `FileContext` storage clients
which is available for all FS via `org.apache.hadoop.fs.impl.DefaultBulkDeleteSource`. For
integration in applications like Apache Iceberg to work seamlessly, all implementations
of this interface MUST NOT reject the request but instead return a BulkDelete instance
of size >= 1.

Use the `PathCapabilities` probe `fs.capability.bulk.delete`.

```java
store.hasPathCapability(path, "fs.capability.bulk.delete")
```

### Invocation through Reflection.

The need for many libraries to compile against very old versions of Hadoop
means that most of the cloud-first Filesystem API calls cannot be used except
through reflection -And the more complicated The API and its data types are,
The harder that reflection is to implement.

To assist this, the class `org.apache.hadoop.io.wrappedio.WrappedIO` has few methods
which are intended to provide simple access to the API, especially
through reflection.

```java

  public static int bulkDeletePageSize(FileSystem fs, Path path) throws IOException;

  public static int bulkDeletePageSize(FileSystem fs, Path path) throws IOException;

  public static List<Map.Entry<Path, String>> bulkDelete(FileSystem fs, Path base, Collection<Path> paths);
```

### Implementations

#### Default Implementation

The default implementation which will be used by all implementation of `FileSystem` of the
`BulkDelete` interface is `org.apache.hadoop.fs.impl.DefaultBulkDeleteOperation` which fixes the page
size to be 1 and calls `FileSystem.delete(path, false)` on the single path in the list.


#### S3A Implementation
The S3A implementation is `org.apache.hadoop.fs.s3a.impl.BulkDeleteOperation` which implements the
multi object delete semantics of the AWS S3 API [Bulk Delete](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
For more details please refer to the S3A Performance documentation.