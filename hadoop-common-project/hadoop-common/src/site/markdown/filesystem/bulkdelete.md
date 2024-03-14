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
it supports the API.

```java
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BulkDeleteSource {

  /**
   * Create a bulk delete operation.
   * There is no network IO at this point, simply the creation of
   * a bulk delete object.
   * A path must be supplied to assist in link resolution.
   * @param path path to delete under.
   * @return the bulk delete.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException path not valid.
   * @throws IOException problems resolving paths
   */
  default BulkDelete createBulkDelete(Path path)
      throws UnsupportedOperationException, IllegalArgumentException, IOException;

}

```

### Interface `org.apache.hadoop.fs.BulkDelete`

This is the bulk delete implementation returned by the `createBulkDelete()` call.

```java
/**
 * API for bulk deletion of objects/files,
 * <i>but not directories</i>.
 * After use, call {@code close()} to release any resources and
 * to guarantee store IOStatistics are updated.
 * <p>
 * Callers MUST have no expectation that parent directories will exist after the
 * operation completes; if an object store needs to explicitly look for and create
 * directory markers, that step will be omitted.
 * <p>
 * Be aware that on some stores (AWS S3) each object listed in a bulk delete counts
 * against the write IOPS limit; large page sizes are counterproductive here, as
 * are attempts at parallel submissions across multiple threads.
 * @see <a href="https://issues.apache.org/jira/browse/HADOOP-16823">HADOOP-16823.
 *  Large DeleteObject requests are their own Thundering Herd</a>
 * <p>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BulkDelete extends IOStatisticsSource, Closeable {

  /**
   * The maximum number of objects/files to delete in a single request.
   * @return a number greater than or equal to zero.
   */
  int pageSize();

  /**
   * Base path of a bulk delete operation.
   * All paths submitted in {@link #bulkDelete(List)} must be under this path.
   */
  Path basePath();

  /**
   * Delete a list of files/objects.
   * <ul>
   *   <li>Files must be under the path provided in {@link #basePath()}.</li>
   *   <li>The size of the list must be equal to or less than the page size
   *       declared in {@link #pageSize()}.</li>
   *   <li>Directories are not supported; the outcome of attempting to delete
   *       directories is undefined (ignored; undetected, listed as failures...).</li>
   *   <li>The operation is not atomic.</li>
   *   <li>The operation is treated as idempotent: network failures may
   *        trigger resubmission of the request -any new objects created under a
   *        path in the list may then be deleted.</li>
   *    <li>There is no guarantee that any parent directories exist after this call.
   *    </li>
   * </ul>
   * @param paths list of paths which must be absolute and under the base path.
   * provided in {@link #basePath()}.
   * @throws IOException IO problems including networking, authentication and more.
   * @throws IllegalArgumentException if a path argument is invalid.
   */
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
which MAY support the API; it may still be unsupported by the
specific instance.

Use the `PathCapabilities` probe `fs.capability.bulk.delete`.

```java
store.hasPathCapability(path, "fs.capability.bulk.delete")
```

### Invocation through Reflection.

The need for many Libraries to compile against very old versions of Hadoop
means that most of the cloud-first Filesystem API calls cannot be used except
through reflection -And the more complicated The API and its data types are,
The harder that reflection is to implement.

To assist this, the class `org.apache.hadoop.fs.FileUtil` has two methods
which are intended to provide simple access to the API, especially
through reflection.

```java
  /**
   * Get the maximum number of objects/files to delete in a single request.
   * @param fs filesystem
   * @param path path to delete under.
   * @return a number greater than or equal to zero.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException path not valid.
   * @throws IOException problems resolving paths
   */
  public static int bulkDeletePageSize(FileSystem fs, Path path) throws IOException;
  
  /**
   * Delete a list of files/objects.
   * <ul>
   *   <li>Files must be under the path provided in {@code base}.</li>
   *   <li>The size of the list must be equal to or less than the page size.</li>
   *   <li>Directories are not supported; the outcome of attempting to delete
   *       directories is undefined (ignored; undetected, listed as failures...).</li>
   *   <li>The operation is not atomic.</li>
   *   <li>The operation is treated as idempotent: network failures may
   *        trigger resubmission of the request -any new objects created under a
   *        path in the list may then be deleted.</li>
   *    <li>There is no guarantee that any parent directories exist after this call.
   *    </li>
   * </ul>
   * @param fs filesystem
   * @param base path to delete under.
   * @param paths list of paths which must be absolute and under the base path.
   * @return a list of all the paths which couldn't be deleted for a reason other than "not found" and any associated error message.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IOException IO problems including networking, authentication and more.
   * @throws IllegalArgumentException if a path argument is invalid.
   */
  public static List<Map.Entry<Path, String>> bulkDelete(FileSystem fs, Path base, List<Path> paths)
```

## S3A Implementation

The S3A client exports this API.

If multi-object delete is enabled (`fs.s3a.multiobjectdelete.enable` = true), as
it is by default, then the page size is limited to that defined in
`fs.s3a.bulk.delete.page.size`, which MUST be less than or equal to1000.
* The entire list of paths to delete is aggregated into a single bulk delete request,
issued to the store.
* Provided the caller has the correct permissions, every entry in the list
  will, if the path references an object, cause that object to be deleted.
* If the path does not reference an object: the path will not be deleted
  "This is for deleting objects, not directories"
* No probes for the existence of parent directories will take place; no
  parent directory markers will be created.
  "If you need parent directories, call mkdir() yourself"
* The list of failed keys listed in the `DeleteObjectsResponse` response
  are converted into paths and returned along with their error messages.
* Network and other IO errors are raised as exceptions.

If multi-object delete is disabled (or the list of size 1)
* A single `DELETE` call is issued
* Any `AccessDeniedException` raised is converted to a result in the error list.
* Any 404 response from a (non-AWS) store will be ignored.
* Network and other IO errors are raised as exceptions.

Because there are no probes to ensure the call does not overwrite a directory,
or to see if a parentDirectory marker needs to be created,
this API is still faster than issuing a normal `FileSystem.delete(path)` call.

That is: all the overhead normally undertaken to preserve the Posix System model are omitted.


### S3 Scalability and Performance

Every entry in a bulk delete request counts as one write operation
against AWS S3 storage.
With the default write rate under a prefix on AWS S3 Standard storage
restricted to 3,500 writes/second, it is very easy to overload
the store by issuing a few bulk delete requests simultaneously.

* If throttling is triggered then all clients interacting with
  the store may observe performance issues.
* The write quota applies even for paths which do not exist.
* The S3A client *may* perform rate throttling as well as page size limiting.

What does that mean? it means that attempting to issue multiple
bulk delete calls in parallel can be counterproductive.

When overloaded, the S3 store returns a 403 throttle response.
This will trigger it back off and retry of posting the request.
However, the repeated request will still include the same number of objects and
*so generate the same load*.

This can lead to a pathological situation where the repeated requests will
never be satisfied because the request itself is sufficient to overload the store.
See [HADOOP-16823.Large DeleteObject requests are their own Thundering Herd](https://issues.apache.org/jira/browse/HADOOP-16823)
for an example of where this did actually surface in production.

This is why the default page size of S3A clients is 250 paths, not the store limit of 1000 entries.
It is also why the S3A delete/rename Operations do not attempt to do massive parallel deletions,
Instead bulk delete requests are queued for a single blocking thread to issue.
Consider a similar design.


When working with versioned S3 buckets, every path deleted will add a tombstone marker
to the store at that location, even if there was no object at that path.
While this has no negative performance impact on the bulk delete call,
it will slow down list requests subsequently made against that path.
That is: bulk delete requests of paths which do not exist will hurt future queries.

Avoid this. Note also that TPC-DS Benchmark do not create the right load to make the 
performance problems observable -but they can surface in production.
* Configure buckets to have a limited number of days for tombstones to be preserved.
* Do not delete which you know do not contain objects.
