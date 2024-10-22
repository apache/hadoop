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

# class `org.apache.hadoop.fs.FileSystem`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

The abstract `FileSystem` class is the original class to access Hadoop filesystems;
non-abstract subclasses exist for all Hadoop-supported filesystems.

All operations that take a Path to this interface MUST support relative paths.
In such a case, they must be resolved relative to the working directory
defined by `setWorkingDirectory()`.

For all clients, therefore, we also add the notion of a state component PWD:
this represents the present working directory of the client. Changes to this
state are not reflected in the filesystem itself: they are unique to the instance
of the client.

**Implementation Note**: the static `FileSystem get(URI uri, Configuration conf) ` method MAY return
a pre-existing instance of a filesystem client class&mdash;a class that may also be in use in other threads.
The implementations of `FileSystem` shipped with Apache Hadoop
*do not make any attempt to synchronize access to the working directory field*.

## Invariants

All the requirements of a valid FileSystem are considered implicit preconditions and postconditions:
all operations on a valid FileSystem MUST result in a new FileSystem that is also valid.

## Feasible features

### <a name="ProtectedDirectories"></a>Protected directories

HDFS has the notion of *Protected Directories*, which are declared in
the option `fs.protected.directories`. Any attempt to delete or rename
such a directory or a parent thereof raises an `AccessControlException`.
Accordingly, any attempt to delete the root directory SHALL, if there is
a protected directory, result in such an exception being raised.

## Predicates and other state access operations


### `boolean exists(Path p)`


    def exists(FS, p) = p in paths(FS)


### `boolean isDirectory(Path p)`

    def isDir(FS, p) = p in directories(FS)


### `boolean isFile(Path p)`


    def isFile(FS, p) = p in filenames(FS)


### `FileStatus getFileStatus(Path p)`

Get the status of a path

#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException

#### Postconditions

    result = stat: FileStatus where:
        if isFile(FS, p) :
            stat.length = len(FS.Files[p])
            stat.isdir = False
            stat.blockSize > 0
        elif isDirectory(FS, p) :
            stat.length = 0
            stat.isdir = True
        elif isSymlink(FS, p) :
            stat.length = 0
            stat.isdir = False
            stat.symlink = FS.Symlinks[p]
        stat.hasAcl = hasACL(FS, p)
        stat.isEncrypted = inEncryptionZone(FS, p)
        stat.isErasureCoded = isErasureCoded(FS, p)

The returned `FileStatus` status of the path additionally carries details on
ACL, encryption and erasure coding information. `getFileStatus(Path p).hasAcl()`
can be queried to find if the path has an ACL. `getFileStatus(Path p).isEncrypted()`
can be queried to find if the path is encrypted. `getFileStatus(Path p).isErasureCoded()`
will tell if the path is erasure coded or not.

YARN's distributed cache lets applications add paths to be cached across
containers and applications via `Job.addCacheFile()` and `Job.addCacheArchive()`.
The cache treats world-readable resources paths added as shareable across
applications, and downloads them differently, unless they are declared as encrypted.

To avoid failures during container launching, especially when delegation tokens
are used, filesystems and object stores which not implement POSIX access permissions
for both files and directories, MUST always return `true` to the `isEncrypted()`
predicate. This can be done by setting the `encrypted` flag to true when creating
the `FileStatus` instance.


### `msync()`

Synchronize metadata state of the client with the latest state of the metadata
service of the FileSystem.

In highly available FileSystems standby service can be used as a read-only
metadata replica. This call is essential to guarantee consistency of
reads from the standby replica and to avoid stale reads.

It is currently only implemented for HDFS and others will just throw
`UnsupportedOperationException`.

#### Preconditions


#### Postconditions

This call internally records the state of the metadata service at the time of
the call. This guarantees consistency of subsequent reads from any metadata
replica. It assures the client will never access the state of the metadata that
preceded the recorded state.

#### HDFS implementation notes

HDFS supports `msync()` in HA mode by calling the Active NameNode and requesting
its latest journal transaction ID. For more details see HDFS documentation
[Consistent Reads from HDFS Observer NameNode](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/ObserverNameNode.html)


### `Path getHomeDirectory()`

The function `getHomeDirectory` returns the home directory for the FileSystem
and the current user account.

For some FileSystems, the path is `["/", "users", System.getProperty("user-name")]`.

However, for HDFS, the username is derived from the credentials used to authenticate the client with HDFS. This
may differ from the local user account name.

**It is the responsibility of the FileSystem to determine the actual home directory
of the caller.**


#### Preconditions


#### Postconditions

    result = p where valid-path(FS, p)

There is no requirement that the path exists at the time the method was called,
or, if it exists, that it points to a directory. However, code tends to assume
that `not isFile(FS, getHomeDirectory())` holds to the extent that follow-on
code may fail.

#### Implementation Notes

* The `FTPFileSystem` queries this value from the remote filesystem and may
fail with a `RuntimeException` or subclass thereof if there is a connectivity
problem. The time to execute the operation is not bounded.

### `FileStatus[] listStatus(Path path, PathFilter filter)`

Lists entries under a path, `path`.

If `path` refers to a file and the filter accepts it,
then that file's `FileStatus` entry is returned in a single-element array.

If the path refers to a directory, the call returns a list of all its immediate
child paths which are accepted by the filter —and does not include the directory
itself.

A `PathFilter` `filter` is a class whose `accept(path)` returns true iff the path
`path` meets the filter's conditions.

#### Preconditions

Path `path` must exist:

    if not exists(FS, path) : raise FileNotFoundException

#### Postconditions


    if isFile(FS, path) and filter.accept(path) :
      result = [ getFileStatus(path) ]

    elif isFile(FS, path) and not filter.accept(P) :
      result = []

    elif isDirectory(FS, path):
      result = [
        getFileStatus(c) for c in children(FS, path) if filter.accepts(c)
      ]


**Implicit invariant**: the contents of a `FileStatus` of a child retrieved
via `listStatus()` are equal to those from a call of `getFileStatus()`
to the same path:

    forall fs in listStatus(path) :
      fs == getFileStatus(fs.path)

**Ordering of results**: there is no guarantee of ordering of the listed entries.
While HDFS currently returns an alphanumerically sorted list, neither the Posix `readdir()`
nor Java's `File.listFiles()` API calls define any ordering of returned values. Applications
which require a uniform sort order on the results must perform the sorting themselves.

**Null return**: Local filesystems prior to 3.0.0 returned null upon access
error. It is considered erroneous. Expect IOException upon access error.

#### Atomicity and Consistency

By the time the `listStatus()` operation returns to the caller, there
is no guarantee that the information contained in the response is current.
The details MAY be out of date, including the contents of any directory, the
attributes of any files, and the existence of the path supplied.

The state of a directory MAY change during the evaluation
process.

* After an entry at path `P` is created, and before any other
 changes are made to the filesystem, `listStatus(P)` MUST
 find the file and return its status.

* After an entry at path `P` is deleted, and before any other
 changes are made to the filesystem, `listStatus(P)` MUST
 raise a `FileNotFoundException`.

* After an entry at path `P` is created, and before any other
 changes are made to the filesystem, the result of `listStatus(parent(P))` SHOULD
 include the value of `getFileStatus(P)`.

* After an entry at path `P` is deleted, and before any other
 changes are made to the filesystem, the result of `listStatus(parent(P))` SHOULD
 NOT include the value of `getFileStatus(P)`.

This is not a theoretical possibility, it is observable in HDFS when a
directory contains many thousands of files.

Consider a directory `"/d"` with the contents:

    a
    part-0000001
    part-0000002
    ...
    part-9999999


If the number of files is such that HDFS returns a partial listing in each
response, then, if a listing `listStatus("/d")` takes place concurrently with the operation
`rename("/d/a","/d/z"))`, the result may be one of:

    [a, part-0000001, ... , part-9999999]
    [part-0000001, ... , part-9999999, z]
    [a, part-0000001, ... , part-9999999, z]
    [part-0000001, ... , part-9999999]

While this situation is likely to be a rare occurrence, it MAY happen. In HDFS
these inconsistent views are only likely when listing a directory with many children.

Other filesystems may have stronger consistency guarantees, or return inconsistent
data more readily.

### `FileStatus[] listStatus(Path path)`

This is exactly equivalent to `listStatus(Path, DEFAULT_FILTER)` where
`DEFAULT_FILTER.accept(path) = True` for all paths.

The atomicity and consistency constraints are as for
`listStatus(Path, DEFAULT_FILTER)`.

### `FileStatus[] listStatus(Path[] paths, PathFilter filter)`

Enumerate all files found in the list of directories passed in,
calling `listStatus(path, filter)` on each one.

As with `listStatus(path, filter)`, the results may be inconsistent.
That is: the state of the filesystem changed during the operation.

There are no guarantees as to whether paths are listed in a specific order, only
that they must all be listed, and, at the time of listing, exist.

#### Preconditions

All paths must exist. There is no requirement for uniqueness.

    forall p in paths :
      exists(FS, p) else raise FileNotFoundException

#### Postconditions

The result is an array whose entries contain every status element
found in the path listings, and no others.

    result = [listStatus(p, filter) for p in paths]

Implementations MAY merge duplicate entries; and/or optimize the
operation by recoginizing duplicate paths and only listing the entries
once.

The default implementation iterates through the list; it does not perform
any optimizations.

The atomicity and consistency constraints are as for
`listStatus(Path, PathFilter)`.

### `RemoteIterator<FileStatus> listStatusIterator(Path p)`

Return an iterator enumerating the `FileStatus` entries under
a path. This is similar to `listStatus(Path)` except the fact that
rather than returning an entire list, an iterator is returned.
The result is exactly the same as `listStatus(Path)`, provided no other
caller updates the directory during the listing. Having said that, this does
not guarantee atomicity if other callers are adding/deleting the files
inside the directory while listing is being performed. Different filesystems
may provide a more efficient implementation, for example S3A does the
listing in pages and fetches the next pages asynchronously while a
page is getting processed.

Note that now since the initial listing is async, bucket/path existence
exception may show up later during next() call.

Callers should prefer using listStatusIterator over listStatus as it
is incremental in nature.

### `FileStatus[] listStatus(Path[] paths)`

Enumerate all files found in the list of directories passed in,
calling `listStatus(path, DEFAULT_FILTER)` on each one, where
the `DEFAULT_FILTER` accepts all path names.

### `RemoteIterator[LocatedFileStatus] listLocatedStatus(Path path, PathFilter filter)`

Return an iterator enumerating the `LocatedFileStatus` entries under
a path. This is similar to `listStatus(Path)` except that the return
value is an instance of the `LocatedFileStatus` subclass of a `FileStatus`,
and that rather than return an entire list, an iterator is returned.

This is actually a `protected` method, directly invoked by
`listLocatedStatus(Path path)`. Calls to it may be delegated through
layered filesystems, such as `FilterFileSystem`, so its implementation MUST
be considered mandatory, even if `listLocatedStatus(Path path)` has been
implemented in a different manner. There are open JIRAs proposing
making this method public; it may happen in future.

There is no requirement for the iterator to provide a consistent view
of the child entries of a path. The default implementation does use
`listStatus(Path)` to list its children, with its consistency constraints
already documented. Other implementations may perform the enumeration even
more dynamically. For example fetching a windowed subset of child entries,
so avoiding building up large data structures and the
transmission of large messages.
In such situations, changes to the filesystem are more likely to become
visible.

Callers MUST assume that the iteration operation MAY fail if changes
to the filesystem take place between this call returning and the iteration
being completely performed.

#### Preconditions

Path `path` must exist:

    if not exists(FS, path) : raise FileNotFoundException

#### Postconditions

The operation generates a set of results, `resultset`, equal to the result of
`listStatus(path, filter)`:

    if isFile(FS, path) and filter.accept(path) :
      resultset =  [ getLocatedFileStatus(FS, path) ]

    elif isFile(FS, path) and not filter.accept(path) :
      resultset = []

    elif isDirectory(FS, path) :
      resultset = [
        getLocatedFileStatus(FS, c)
         for c in children(FS, path) where filter.accept(c)
      ]

The operation `getLocatedFileStatus(FS, path: Path): LocatedFileStatus`
is defined as a generator of a `LocatedFileStatus` instance `ls`
where:

    fileStatus = getFileStatus(FS, path)

    bl = getFileBlockLocations(FS, path, 0, fileStatus.len)

    locatedFileStatus = new LocatedFileStatus(fileStatus, bl)

The ordering in which the elements of `resultset` are returned in the iterator
is undefined.

The atomicity and consistency constraints are as for
`listStatus(Path, PathFilter)`.

### `RemoteIterator[LocatedFileStatus] listLocatedStatus(Path path)`

The equivalent to `listLocatedStatus(path, DEFAULT_FILTER)`,
where `DEFAULT_FILTER` accepts all path names.

### `RemoteIterator[LocatedFileStatus] listFiles(Path path, boolean recursive)`

Create an iterator over all files in/under a directory, potentially
recursing into child directories.

The goal of this operation is to permit large recursive directory scans
to be handled more efficiently by filesystems, by reducing the amount
of data which must be collected in a single RPC call.

#### Preconditions

    if not exists(FS, path) : raise FileNotFoundException

### Postconditions

The outcome is an iterator, whose output from the sequence of
`iterator.next()` calls can be defined as the set `iteratorset`:

    if not recursive:
      iteratorset == listStatus(path)
    else:
      iteratorset = [
        getLocatedFileStatus(FS, d)
          for d in descendants(FS, path)
      ]

The function `getLocatedFileStatus(FS, d)` is as defined in
`listLocatedStatus(Path, PathFilter)`.

The atomicity and consistency constraints are as for
`listStatus(Path, PathFilter)`.


### `ContentSummary getContentSummary(Path path)`

Given a path return its content summary.

`getContentSummary()` first checks if the given path is a file and if yes, it returns 0 for directory count
and 1 for file count.

#### Preconditions

    if not exists(FS, path) : raise FileNotFoundException

#### Postconditions

Returns a `ContentSummary` object with information such as directory count
and file count for a given path.

The atomicity and consistency constraints are as for
`listStatus(Path, PathFilter)`.

### `BlockLocation[] getFileBlockLocations(FileStatus f, int s, int l)`

#### Preconditions

    if s < 0 or l < 0 : raise {HadoopIllegalArgumentException, InvalidArgumentException}

* HDFS throws `HadoopIllegalArgumentException` for an invalid offset
or length; this extends `IllegalArgumentException`.

#### Postconditions

If the filesystem is location aware, it must return the list
of block locations where the data in the range `[s:s+l]` can be found.


    if f == null :
        result = null
    elif f.getLen() <= s:
        result = []
    else result = [ locations(FS, b) for b in blocks(FS, p, s, s+l)]

Where

      def locations(FS, b) = a list of all locations of a block in the filesystem

      def blocks(FS, p, s, s +  l)  = a list of the blocks containing data(FS, path)[s:s+l]


Note that as `length(FS, f) ` is defined as `0` if `isDir(FS, f)`, the result
of `getFileBlockLocations()` on a directory is `[]`


If the filesystem is not location aware, it SHOULD return

      [
        BlockLocation(["localhost:9866"] ,
                  ["localhost"],
                  ["/default/localhost"]
                   0, f.getLen())
       ] ;


*A bug in Hadoop 1.0.3 means that a topology path of the same number
of elements as the cluster topology MUST be provided, hence Filesystems SHOULD
return that `"/default/localhost"` path. While this is no longer an issue,
the convention is generally retained.


###  `BlockLocation[] getFileBlockLocations(Path P, int S, int L)`

#### Preconditions


    if p == null : raise NullPointerException
    if not exists(FS, p) : raise FileNotFoundException


#### Postconditions

    result = getFileBlockLocations(getFileStatus(FS, P), S, L)


###  `long getDefaultBlockSize()`

Get the "default" block size for a filesystem. This is often used during
split calculations to divide work optimally across a set of worker processes.

#### Preconditions

#### Postconditions

    result = integer > 0

Although there is no defined minimum value for this result, as it
is used to partition work during job submission, a block size
that is too small will result in badly partitioned workload,
or even the `JobSubmissionClient` and equivalent
running out of memory as it calculates the partitions.

Any FileSystem that does not actually break files into blocks SHOULD
return a number for this that results in efficient processing.
A FileSystem MAY make this user-configurable (the object store connectors usually do this).

###  `long getDefaultBlockSize(Path p)`

Get the "default" block size for a path --that is, the block size to be used
when writing objects to a path in the filesystem.

#### Preconditions


#### Postconditions


    result = integer >= 0

The outcome of this operation is usually identical to `getDefaultBlockSize()`,
with no checks for the existence of the given path.

Filesystems that support mount points may have different default values for
different paths, in which case the specific default value for the destination path
SHOULD be returned.

It is not an error if the path does not exist: the default/recommended value
for that part of the filesystem MUST be returned.

###  `long getBlockSize(Path p)`

This method is exactly equivalent to querying the block size
of the `FileStatus` structure returned in `getFileStatus(p)`.
It is deprecated in order to encourage users to make a single call to
`getFileStatus(p)` and then use the result to examine multiple attributes
of the file (e.g. length, type, block size). If more than one attribute is queried,
This can become a significant performance optimization —and reduce load
on the filesystem.

#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException


#### Postconditions

    if len(FS, P) > 0 :  getFileStatus(P).getBlockSize() > 0
    result == getFileStatus(P).getBlockSize()

1. The outcome of this operation MUST be identical to the value of
   `getFileStatus(P).getBlockSize()`.
2. By inference, it MUST be > 0 for any file of length > 0.

###  `Path getEnclosingRoot(Path p)`

This method is used to find a root directory for a path given. This is useful for creating
staging and temp directories in the same enclosing root directory. There are constraints around how
renames are allowed to atomically occur (ex. across hdfs volumes or across encryption zones).

For any two paths p1 and p2 that do not have the same enclosing root, `rename(p1, p2)` is expected to fail or will not
be atomic.

For object stores, even with the same enclosing root, there is no guarantee file or directory rename is atomic

The following statement is always true:
`getEnclosingRoot(p) == getEnclosingRoot(getEnclosingRoot(p))`


```python
path in ancestors(FS, p) or path == p:
isDir(FS, p)
```

#### Preconditions

The path does not have to exist, but the path does need to be valid and reconcilable by the filesystem
* if a linkfallback is used all paths are reconcilable
* if a linkfallback is not used there must be a mount point covering the path


#### Postconditions

* The path returned will not be null, if there is no deeper enclosing root, the root path ("/") will be returned.
* The path returned is a directory


## <a name="state_changing_operations"></a> State Changing Operations

### `boolean mkdirs(Path p, FsPermission permission)`

Create a directory and all its parents.

#### Preconditions


The path must either be a directory or not exist
 
     if exists(FS, p) and not isDirectory(FS, p) :
         raise [ParentNotDirectoryException, FileAlreadyExistsException, IOException]

No ancestor may be a file

    forall d = ancestors(FS, p) : 
        if exists(FS, d) and not isDir(FS, d) :
            raise {ParentNotDirectoryException, FileAlreadyExistsException, IOException}

#### Postconditions


    FS' where FS'.Directories = FS.Directories + [p] + ancestors(FS, p)
    result = True


The condition exclusivity requirement of a FileSystem's directories,
files and symbolic links must hold.

The probe for the existence and type of a path and directory creation MUST be
atomic. The combined operation, including `mkdirs(parent(F))` MAY be atomic.

The return value is always true&mdash;even if a new directory is not created
 (this is defined in HDFS).

### <a name='FileSystem.create'></a> `FSDataOutputStream create(Path, ...)`


    FSDataOutputStream create(Path p,
          FsPermission permission,
          boolean overwrite,
          int bufferSize,
          short replication,
          long blockSize,
          Progressable progress) throws IOException;


#### Preconditions

The file must not exist for a no-overwrite create:

    if not overwrite and isFile(FS, p) : raise FileAlreadyExistsException

Writing to or overwriting a directory must fail.

    if isDirectory(FS, p) : raise {FileAlreadyExistsException, FileNotFoundException, IOException}

No ancestor may be a file

    forall d = ancestors(FS, p) : 
        if exists(FS, d) and not isDir(FS, d) :
            raise {ParentNotDirectoryException, FileAlreadyExistsException, IOException}

FileSystems may reject the request for other
reasons, such as the FS being read-only  (HDFS),
the block size being below the minimum permitted (HDFS),
the replication count being out of range (HDFS),
quotas on namespace or filesystem being exceeded, reserved
names, etc. All rejections SHOULD be `IOException` or a subclass thereof
and MAY be a `RuntimeException` or subclass.
For instance, HDFS may raise an `InvalidPathException`.

#### Postconditions

    FS' where :
       FS'.Files[p] == []
       ancestors(p) subset-of FS'.Directories

    result = FSDataOutputStream

A zero byte file MUST exist at the end of the specified path, visible to all.

The updated (valid) FileSystem MUST contain all the parent directories of the path, as created by `mkdirs(parent(p))`.

The result is `FSDataOutputStream`, which through its operations may generate new filesystem states with updated values of
`FS.Files[p]`

The behavior of the returned stream is covered in [Output](outputstream.html).

#### Implementation Notes

* Some implementations split the create into a check for the file existing
 from the
 actual creation. This means the operation is NOT atomic: it is possible for
 clients creating files with `overwrite==true` to fail if the file is created
 by another client between the two tests.

* The S3A and potentially other Object Stores connectors currently don't change the `FS` state
until the output stream `close()` operation is completed.
This is a significant difference between the behavior of object stores
and that of filesystems, as it allows &gt;1 client to create a file with `overwrite=false`,
and potentially confuse file/directory logic. In particular, using `create()` to acquire
an exclusive lock on a file (whoever creates the file without an error is considered
the holder of the lock) may not be a safe algorithm to use when working with object stores.

* Object stores may create an empty file as a marker when a file is created.
However, object stores with `overwrite=true` semantics may not implement this atomically,
so creating files with `overwrite=false` cannot be used as an implicit exclusion
mechanism between processes.

* The Local FileSystem raises a `FileNotFoundException` when trying to create a file over
a directory, hence it is listed as an exception that MAY be raised when
this precondition fails.

* Not covered: symlinks. The resolved path of the symlink is used as the final path argument to the `create()` operation

### `FSDataOutputStreamBuilder createFile(Path p)`

Make a `FSDataOutputStreamBuilder` to specify the parameters to create a file.

The behavior of the returned stream is covered in [Output](outputstream.html).

#### Implementation Notes

`createFile(p)` returns a `FSDataOutputStreamBuilder` only and does not make
changes on the filesystem immediately. When `build()` is invoked on the `FSDataOutputStreamBuilder`,
the builder parameters are verified and [`create(Path p)`](#FileSystem.create)
is invoked on the underlying filesystem. `build()` has the same preconditions
and postconditions as [`create(Path p)`](#FileSystem.create).

* Similar to [`create(Path p)`](#FileSystem.create), files are overwritten
by default, unless specified by `builder.overwrite(false)`.
* Unlike [`create(Path p)`](#FileSystem.create), missing parent directories are
not created by default, unless specified by `builder.recursive()`.

### <a name='FileSystem.append'></a> `FSDataOutputStream append(Path p, int bufferSize, Progressable progress)`

Implementations without a compliant call SHOULD throw `UnsupportedOperationException`.

#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException

    if not isFile(FS, p) : raise {FileAlreadyExistsException, FileNotFoundException, IOException}

#### Postconditions

    FS' = FS
    result = FSDataOutputStream

Return: `FSDataOutputStream`, which can update the entry `FS'.Files[p]`
by appending data to the existing list.

The behavior of the returned stream is covered in [Output](outputstream.html).

### `FSDataOutputStreamBuilder appendFile(Path p)`

Make a `FSDataOutputStreamBuilder` to specify the parameters to append to an
existing file.

The behavior of the returned stream is covered in [Output](outputstream.html).

#### Implementation Notes

`appendFile(p)` returns a `FSDataOutputStreamBuilder` only and does not make
change on filesystem immediately. When `build()` is invoked on the `FSDataOutputStreamBuilder`,
the builder parameters are verified and [`append()`](#FileSystem.append) is
invoked on the underlying filesystem. `build()` has the same preconditions and
postconditions as [`append()`](#FileSystem.append).

### `FSDataInputStream open(Path f, int bufferSize)`

Implementations without a compliant call SHOULD throw `UnsupportedOperationException`.

#### Preconditions

    if not isFile(FS, p)) : raise {FileNotFoundException, IOException}

This is a critical precondition. Implementations of some FileSystems (e.g.
Object stores) could shortcut one round trip by postponing their HTTP GET
operation until the first `read()` on the returned `FSDataInputStream`.
However, much client code does depend on the existence check being performed
at the time of the `open()` operation. Implementations MUST check for the
presence of the file at the time of creation. This does not imply that
the file and its data is still at the time of the following `read()` or
any successors.

#### Postconditions

    result = FSDataInputStream(0, FS.Files[p])

The result provides access to the byte array defined by `FS.Files[p]`; whether that
access is to the contents at the time the `open()` operation was invoked,
or whether and how it may pick up changes to that data in later states of FS is
an implementation detail.

The result MUST be the same for local and remote callers of the operation.


#### HDFS implementation notes

1. HDFS MAY throw `UnresolvedPathException` when attempting to traverse
symbolic links

1. HDFS throws `IOException("Cannot open filename " + src)` if the path
exists in the metadata, but no copies of its blocks can be located;
-`FileNotFoundException` would seem more accurate and useful.

### `FSDataInputStreamBuilder openFile(Path path)`

See [openFile()](openfile.html).

### `FSDataInputStreamBuilder openFile(PathHandle)`

See [openFile()](openfile.html).

### `PathHandle getPathHandle(FileStatus stat, HandleOpt... options)`

Implementations without a compliant call MUST throw `UnsupportedOperationException`

#### Preconditions

    let stat = getFileStatus(Path p)
    let FS' where:
      (FS'.Directories, FS.Files', FS'.Symlinks)
      p' in paths(FS') where:
        exists(FS, stat.path) implies exists(FS', p')

The referent of a `FileStatus` instance, at the time it was resolved, is the
same referent as the result of `getPathHandle(FileStatus)`. The `PathHandle`
may be used in subsequent operations to ensure invariants hold between
calls.

The `options` parameter specifies whether a subsequent call e.g.,
`open(PathHandle)` will succeed if the referent data or location changed. By
default, any modification results in an error. The caller MAY specify
relaxations that allow operations to succeed even if the referent exists at
a different path and/or its data are changed.

An implementation MUST throw `UnsupportedOperationException` if it cannot
support the semantics specified by the caller. The default set of options
are as follows.

|            | Unmoved  | Moved     |
|-----------:|:--------:|:---------:|
| Unchanged  | EXACT    | CONTENT   |
| Changed    | PATH     | REFERENCE |

Changes to ownership, extended attributes, and other metadata are not
required to match the `PathHandle`. Implementations can extend the set of
`HandleOpt` parameters with custom constraints.

##### Examples

A client specifies that the `PathHandle` should track the entity across
renames using `REFERENCE`. The implementation MUST throw an
`UnsupportedOperationException` when creating the `PathHandle` unless
failure to resolve the reference implies the entity no longer exists.

A client specifies that the `PathHandle` should resolve only if the entity
is unchanged using `PATH`. The implementation MUST throw an
`UnsupportedOperationException` when creating the `PathHandle` unless it can
distinguish between an identical entity located subsequently at the same
path.

#### Postconditions

    result = PathHandle(p')

#### Implementation notes

The referent of a `PathHandle` is the namespace when the `FileStatus`
instance was created, _not_ its state when the `PathHandle` is created. An
implementation MAY reject attempts to create or resolve `PathHandle`
instances that are valid, but expensive to service.

Object stores that implement rename by copying objects MUST NOT claim to
support `CONTENT` and `REFERENCE` unless the lineage of the object is
resolved.

It MUST be possible to serialize a `PathHandle` instance and reinstantiate
it in one or more processes, on another machine, and arbitrarily far into
the future without changing its semantics. The implementation MUST refuse to
resolve instances if it can no longer guarantee its invariants.

#### HDFS implementation notes

HDFS does not support `PathHandle` references to directories or symlinks.
Support for `CONTENT` and `REFERENCE` looks up files by INode. INodes are
not unique across NameNodes, so federated clusters SHOULD include enough
metadata in the `PathHandle` to detect references from other namespaces.

### `FSDataInputStream open(PathHandle handle, int bufferSize)`

Implementations without a compliant call MUST throw `UnsupportedOperationException`

#### Preconditions

    let fd = getPathHandle(FileStatus stat)
    if stat.isdir : raise IOException
    let FS' where:
      (FS'.Directories, FS.Files', FS'.Symlinks)
      p' in FS'.Files where:
        FS'.Files[p'] = fd
    if not exists(FS', p') : raise InvalidPathHandleException

The implementation MUST resolve the referent of the `PathHandle` following
the constraints specified at its creation by `getPathHandle(FileStatus)`.

Metadata necessary for the `FileSystem` to satisfy this contract MAY be
encoded in the `PathHandle`.

#### Postconditions

    result = FSDataInputStream(0, FS'.Files[p'])

The stream returned is subject to the constraints of a stream returned by
`open(Path)`. Constraints checked on open MAY hold to hold for the stream, but
this is not guaranteed.

For example, a `PathHandle` created with `CONTENT` constraints MAY return a
stream that ignores updates to the file after it is opened, if it was
unmodified when `open(PathHandle)` was resolved.

#### Implementation notes

An implementation MAY check invariants either at the server or before
returning the stream to the client. For example, an implementation may open
the file, then verify the invariants in the `PathHandle` using
`getFileStatus(Path)` to implement `CONTENT`. This could yield false
positives and it requires additional RPC traffic.

### `boolean delete(Path p, boolean recursive)`

Delete a path, be it a file, symbolic link or directory. The
`recursive` flag indicates whether a recursive delete should take place —if
unset then a non-empty directory cannot be deleted.

Except in the special case of the root directory, if this API call
completed successfully then there is nothing at the end of the path.
That is: the outcome is desired. The return flag simply tells the caller
whether or not any change was made to the state of the filesystem.

*Note*: many uses of this method surround it with checks for the return value being
false, raising exception if so. For example

```java
if (!fs.delete(path, true)) throw new IOException("Could not delete " + path);
```

This pattern is not needed. Code SHOULD just call `delete(path, recursive)` and
assume the destination is no longer present —except in the special case of root
directories, which will always remain (see below for special coverage of root directories).

#### Preconditions

A directory with children and `recursive == False` cannot be deleted

    if isDirectory(FS, p) and not recursive and (children(FS, p) != {}) : raise IOException

(HDFS raises `PathIsNotEmptyDirectoryException` here.)

#### Postconditions


##### Nonexistent path

If the file does not exist the filesystem state does not change

    if not exists(FS, p) :
        FS' = FS
        result = False

The result SHOULD be `False`, indicating that no file was deleted.


##### Simple File


A path referring to a file is removed, return value: `True`

    if isFile(FS, p) :
        FS' = (FS.Directories, FS.Files - [p], FS.Symlinks)
        result = True


##### Empty root directory, `recursive == False`

Deleting an empty root does not change the filesystem state
and may return true or false.

    if isRoot(p) and children(FS, p) == {} :
        FS ' = FS
        result = (undetermined)

There is no consistent return code from an attempt to delete the root directory.

Implementations SHOULD return true; this avoids code which checks for a false
return value from overreacting.

*Object Stores*: see [Object Stores: root directory deletion](#object-stores-rm-root).


##### Empty (non-root) directory `recursive == False`

Deleting an empty directory that is not root will remove the path from the FS and
return true.

    if isDirectory(FS, p) and not isRoot(p) and children(FS, p) == {} :
        FS' = (FS.Directories - [p], FS.Files, FS.Symlinks)
        result = True


##### Recursive delete of non-empty root directory

Deleting a root path with children and `recursive==True`
can generally have three outcomes:

1. The POSIX model assumes that if the user has
the correct permissions to delete everything,
they are free to do so (resulting in an empty filesystem).

        if isDirectory(FS, p) and isRoot(p) and recursive :
            FS' = ({["/"]}, {}, {}, {})
            result = True

1. HDFS never permits the deletion of the root of a filesystem; the
filesystem must be taken offline and reformatted if an empty
filesystem is desired.

        if isDirectory(FS, p) and isRoot(p) and recursive :
            FS' = FS
            result = False

1. Object Stores: see [Object Stores: root directory deletion](#object-stores-rm-root).

This specification does not recommend any specific action. Do note, however,
that the POSIX model assumes that there is a permissions model such that normal
users do not have the permission to delete that root directory; it is an action
which only system administrators should be able to perform.

Any filesystem client which interacts with a remote filesystem which lacks
such a security model, MAY reject calls to `delete("/", true)` on the basis
that it makes it too easy to lose data.


### <a name="object-stores-rm-root"></a> Object Stores: root directory deletion

Some of the object store based filesystem implementations always return
false when deleting the root, leaving the state of the store unchanged.

    if isRoot(p) :
        FS' = FS
        result = False

This is irrespective of the recursive flag status or the state of the directory.

This is a simplification which avoids the inevitably non-atomic scan and delete
of the contents of the store. It also avoids any confusion about whether
the operation actually deletes that specific store/container itself, and
adverse consequences of the simpler permissions models of stores.

##### Recursive delete of non-root directory

Deleting a non-root path with children `recursive==true`
removes the path and all descendants

    if isDirectory(FS, p) and not isRoot(p) and recursive :
        FS' where:
            not isDirectory(FS', p)
            and forall d in descendants(FS, p):
                not isDirectory(FS', d)
                not isFile(FS', d)
                not isSymlink(FS', d)
        result = True

#### Atomicity

* Deleting a file MUST be an atomic action.

* Deleting an empty directory MUST be an atomic action.

* A recursive delete of a directory tree MUST be atomic.

#### Implementation Notes

* Object Stores and other non-traditional filesystems onto which a directory
 tree is emulated, tend to implement `delete()` as recursive listing and
entry-by-entry delete operation.
This can break the expectations of client applications for O(1) atomic directory
deletion, preventing the stores' use as drop-in replacements for HDFS.

### `boolean rename(Path src, Path d)`

In terms of its specification, `rename()` is one of the most complex operations within a filesystem.

In terms of its implementation, it is the one with the most ambiguity regarding when to return false
versus raising an exception.

Rename includes the calculation of the destination path.
If the destination exists and is a directory, the final destination
of the rename becomes the destination + the filename of the source path.

    let dest = if (isDir(FS, d) and d != src) :
            d + [filename(src)]
        else :
            d

#### Preconditions

All checks on the destination path MUST take place after the final `dest` path
has been calculated.

Source `src` must exist:

    if not exists(FS, src) : raise FileNotFoundException

`dest` cannot be a descendant of `src`:

    if isDescendant(FS, src, dest) : raise IOException

This implicitly covers the special case of `isRoot(FS, src)`.

`dest` must be root, or have a parent that exists:

    if not (isRoot(FS, dest) or exists(FS, parent(dest))) : raise IOException

The parent path of a destination must not be a file:

    if isFile(FS, parent(dest)) : raise IOException

This implicitly covers all the ancestors of the parent.

There must not be an existing file at the end of the destination path:

    if isFile(FS, dest) : raise FileAlreadyExistsException, IOException


#### Postconditions


##### Renaming a directory onto itself

Renaming a directory onto itself is no-op; return value is not specified.

In POSIX the result is `False`;  in HDFS the result is `True`.

    if isDir(FS, src) and src == dest :
        FS' = FS
        result = (undefined)


##### Renaming a file to self

Renaming a file to itself is a no-op; the result is `True`.

     if isFile(FS, src) and src == dest :
         FS' = FS
         result = True


##### Renaming a file onto a nonexistent path

Renaming a file where the destination is a directory moves the file as a child
 of the destination directory, retaining the filename element of the source path.

    if isFile(FS, src) and src != dest:
        FS' where:
            not exists(FS', src)
            and exists(FS', dest)
            and data(FS', dest) == data (FS, source)
        result = True



##### Renaming a directory onto a directory

If `src` is a directory then all its children will then exist under `dest`, while the path
`src` and its descendants will no longer exist. The names of the paths under
`dest` will match those under `src`, as will the contents:

    if isDir(FS, src) and isDir(FS, dest) and src != dest :
        FS' where:
            not exists(FS', src)
            and dest in FS'.Directories
            and forall c in descendants(FS, src) :
                not exists(FS', c))
            and forall c in descendants(FS, src) where isDir(FS, c):
                isDir(FS', dest + childElements(src, c)
            and forall c in descendants(FS, src) where not isDir(FS, c):
                    data(FS', dest + childElements(s, c)) == data(FS, c)
        result = True

##### Renaming into a path where the parent path does not exist

      not exists(FS, parent(dest))

There is no consistent behavior here.

*HDFS*

The outcome is no change to FileSystem state, with a return value of false.

    FS' = FS
    result = False

*Local Filesystem*

The outcome is as a normal rename, with the additional (implicit) feature
that the parent directories of the destination also exist.

    exists(FS', parent(dest))

*S3A FileSystem*

The outcome is as a normal rename, with the additional (implicit) feature that
the parent directories of the destination then exist:
`exists(FS', parent(dest))`

There is a check for and rejection if the `parent(dest)` is a file, but
no checks for any other ancestors.

*Other Filesystems*

Other filesystems strictly reject the operation, raising a `FileNotFoundException`

##### Concurrency requirements

* The core operation of `rename()`&mdash;moving one entry in the filesystem to
another&mdash;MUST be atomic. Some applications rely on this as a way to coordinate access to data.

* Some FileSystem implementations perform checks on the destination
FileSystem before and after the rename. One example of this is `ChecksumFileSystem`, which
provides checksummed access to local data. The entire sequence MAY NOT be atomic.

##### Implementation Notes

**Files open for reading, writing or appending**

The behavior of `rename()` on an open file is unspecified: whether it is
allowed, what happens to later attempts to read from or write to the open stream

**Renaming a directory onto itself**

The return code of renaming a directory onto itself is unspecified.

**Destination exists and is a file**

Renaming a file atop an existing file is specified as failing, raising an exception.

* Local FileSystem : the rename succeeds; the destination file is replaced by the source file.

* HDFS : The rename fails, no exception is raised. Instead the method call simply returns false.

**Missing source file**

If the source file `src` does not exist,  `FileNotFoundException` should be raised.

HDFS fails without raising an exception; `rename()` merely returns false.

    FS' = FS
    result = false

The behavior of HDFS here should not be considered a feature to replicate.
`FileContext` explicitly changed the behavior to raise an exception, and the retrofitting of that action
to the `DFSFileSystem` implementation is an ongoing matter for debate.

See  [Renaming File and Directories](renaming.html).

### `void concat(Path p, Path sources[])`

Joins multiple blocks together to create a single file. This
is a little-used operation currently implemented only by HDFS.

Implementations without a compliant call SHOULD throw `UnsupportedOperationException`.

#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException

    if sources==[] : raise IllegalArgumentException

All sources MUST be in the same directory:

    for s in sources:
        if parent(s) != parent(p) : raise IllegalArgumentException

All block sizes must match that of the target:

    for s in sources:
        getBlockSize(FS, s) == getBlockSize(FS, p)

No duplicate paths:

    let input = sources + [p]
    not (exists i, j: i != j and input[i] == input[j])

HDFS: All source files except the final one MUST be a complete block:

    for s in (sources[0:length(sources)-1] + [p]):
        (length(FS, s) mod getBlockSize(FS, p)) == 0


#### Postconditions


    FS' where:
        (data(FS', p) = data(FS, p) + data(FS, sources[0]) + ... + data(FS, sources[length(sources)-1]))
        for s in sources: not exists(FS', s)


HDFS's restrictions may be an implementation detail of how it implements
`concat` by changing the inode references to join them together in
a sequence. As no other filesystem in the Hadoop core codebase
implements this method, there is no way to distinguish implementation detail
from specification.


### `boolean truncate(Path p, long newLength)`

Truncate file `p` to the specified `newLength`.

Implementations without a compliant call SHOULD throw `UnsupportedOperationException`.

#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException

    if isDir(FS, p) : raise {FileNotFoundException, IOException}

    if newLength < 0 || newLength > len(FS.Files[p]) : raise HadoopIllegalArgumentException

HDFS: The source file MUST be closed.
Truncate cannot be performed on a file, which is open for writing or appending.

#### Postconditions

    len(FS'.Files[p]) = newLength

Return: `true`, if truncation is finished and the file can be immediately
opened for appending, or `false` otherwise.

HDFS: HDFS returns `false` to indicate that a background process of adjusting
the length of the last block has been started, and clients should wait for it
to complete before they can proceed with further file updates.

#### Concurrency

If an input stream is open when truncate() occurs, the outcome of read
operations related to the part of the file being truncated is undefined.



### `boolean copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)`

The source file or directory at `src` is on the local disk and is copied into the file system at
destination `dst`. If the source must be deleted after the move then `delSrc` flag must be
set to TRUE. If destination already exists, and the destination contents must be overwritten
then `overwrite` flag must be set to TRUE.

#### Preconditions
Source and destination must be different
```python
if src = dest : raise FileExistsException
```

Destination and source must not be descendants of one another
```python
if isDescendant(src, dest) or isDescendant(dest, src) : raise IOException
```

The source file or directory must exist locally:
```python
if not exists(LocalFS, src) : raise FileNotFoundException
```

Directories cannot be copied into files regardless to what the overwrite flag is set to:

```python
if isDir(LocalFS, src) and isFile(FS, dst) : raise PathExistsException
```

For all cases, except the one for which the above precondition throws, the overwrite flag must be
set to TRUE for the operation to succeed if destination exists. This will also overwrite any files
 / directories at the destination:

```python
if exists(FS, dst) and not overwrite : raise PathExistsException
```

#### Determining the final name of the copy
Given a base path on the source `base` and a child path `child` where `base` is in
`ancestors(child) + child`:

```python
def final_name(base, child, dest):
    if base == child:
        return dest
    else:
        return dest + childElements(base, child)
```

#### Outcome where source is a file `isFile(LocalFS, src)`
For a file, data at destination becomes that of the source. All ancestors are directories.
```python
if isFile(LocalFS, src) and (not exists(FS, dest) or (exists(FS, dest) and overwrite)):
    FS' = FS where:
        FS'.Files[dest] = LocalFS.Files[src]
        FS'.Directories = FS.Directories + ancestors(FS, dest)
    LocalFS' = LocalFS where
        not delSrc or (delSrc = true and delete(LocalFS, src, false))
else if isFile(LocalFS, src) and isDir(FS, dest):
    FS' = FS where:
        let d = final_name(src, dest)
        FS'.Files[d] = LocalFS.Files[src]
    LocalFS' = LocalFS where:
        not delSrc or (delSrc = true and delete(LocalFS, src, false))
```
There are no expectations that the file changes are atomic for both local `LocalFS` and remote `FS`.

#### Outcome where source is a directory `isDir(LocalFS, src)`
```python
if isDir(LocalFS, src) and (isFile(FS, dest) or isFile(FS, dest + childElements(src))):
    raise FileAlreadyExistsException
else if isDir(LocalFS, src):
    if exists(FS, dest):
        dest' = dest + childElements(src)
        if exists(FS, dest') and not overwrite:
            raise PathExistsException
    else:
        dest' = dest

    FS' = FS where:
        forall c in descendants(LocalFS, src):
            not exists(FS', final_name(c)) or overwrite
        and forall c in descendants(LocalFS, src) where isDir(LocalFS, c):
            FS'.Directories = FS'.Directories + (dest' + childElements(src, c))
        and forall c in descendants(LocalFS, src) where isFile(LocalFS, c):
            FS'.Files[final_name(c, dest')] = LocalFS.Files[c]
    LocalFS' = LocalFS where
        not delSrc or (delSrc = true and delete(LocalFS, src, true))
```
There are no expectations of operation isolation / atomicity.
This means files can change in source or destination while the operation is executing.
No guarantees are made for the final state of the file or directory after a copy other than it is
best effort. E.g.: when copying a directory, one file can be moved from source to destination but
there's nothing stopping the new file at destination being updated while the copy operation is still
in place.

#### Implementation

The default HDFS implementation, is to recurse through each file and folder, found at `src`, and
copy them sequentially to their final destination (relative to `dst`).

Object store based file systems should be mindful of what limitations arise from the above
implementation and could take advantage of parallel uploads and possible re-ordering of files copied
into the store to maximize throughput.


## <a name="RemoteIterator"></a> interface `RemoteIterator`

The `RemoteIterator` interface is used as a remote-access equivalent
to `java.util.Iterator`, allowing the caller to iterate through a finite sequence
of remote data elements.

The core differences are

1. `Iterator`'s optional `void remove()` method is not supported.
2. For those methods which are supported, `IOException` exceptions
may be raised.

```java
public interface RemoteIterator<E> {
  boolean hasNext() throws IOException;
  E next() throws IOException;
}
```

The basic view of the interface is that `hasNext()` being true implies
that `next()` will successfully return the next entry in the list:

```
while hasNext(): next()
```

Equally, a successful call to `next()` implies that had `hasNext()` been invoked
prior to the call to `next()`, it would have been true.

```java
boolean elementAvailable = hasNext();
try {
  next();
  assert elementAvailable;
} catch (NoSuchElementException e) {
  assert !elementAvailable
}
```

The `next()` operator MUST iterate through the list of available
results, *even if no calls to `hasNext()` are made*.

That is, it is possible to enumerate the results through a loop which
only terminates when a `NoSuchElementException` exception is raised.

```java
try {
  while (true) {
    process(iterator.next());
  }
} catch (NoSuchElementException ignored) {
  // the end of the list has been reached
}
```

The output of the iteration is equivalent to the loop

```java
while (iterator.hasNext()) {
  process(iterator.next());
}
```

As raising exceptions is an expensive operation in JVMs, the `while(hasNext())`
loop option is more efficient. (see also [Concurrency and the Remote Iterator](#RemoteIteratorConcurrency)
for a discussion on this topic).

Implementors of the interface MUST support both forms of iterations; authors
of tests SHOULD verify that both iteration mechanisms work.

The iteration is required to return a finite sequence; both forms
of loop MUST ultimately terminate. All implementations of the interface in the
Hadoop codebase meet this requirement; all consumers assume that it holds.

### `boolean hasNext()`

Returns true if-and-only-if a subsequent single call to `next()` would
return an element rather than raise an exception.

#### Preconditions

#### Postconditions

    result = True ==> next() will succeed.
    result = False ==> next() will raise an exception

Multiple calls to `hasNext()`, without any intervening `next()` calls, MUST
return the same value.

```java
boolean has1 = iterator.hasNext();
boolean has2 = iterator.hasNext();
assert has1 == has2;
```

### `E next()`

Return the next element in the iteration.

#### Preconditions

    hasNext() else raise java.util.NoSuchElementException

#### Postconditions

    result = the next element in the iteration

Repeated calls to `next()` return
subsequent elements in the sequence, until the entire sequence has been returned.


### <a name="RemoteIteratorConcurrency"></a>Concurrency and the Remote Iterator

The primary use of `RemoteIterator` in the filesystem APIs is to list files
on (possibly remote) filesystems. These filesystems are invariably accessed
concurrently; the state of the filesystem MAY change between a `hasNext()`
probe and the invocation of the `next()` call.

During iteration through a `RemoteIterator`, if the directory is deleted on
remote filesystem, then `hasNext()` or `next()` call may throw
`FileNotFoundException`.

Accordingly, a robust iteration through a `RemoteIterator` would catch and
discard `NoSuchElementException` exceptions raised during the process, which
could be done through the `while(true)` iteration example above, or
through a `hasNext()/next()` sequence with an outer `try/catch` clause to
catch a `NoSuchElementException` alongside other exceptions which may be
raised during a failure (for example, a `FileNotFoundException`)


```java
try {
  while (iterator.hasNext()) {
    process(iterator.next());
  }
} catch (NoSuchElementException ignored) {
  // the end of the list has been reached
}
```

It is notable that this is *not* done in the Hadoop codebase. This does not imply
that robust loops are not recommended —more that the concurrency
problems were not considered during the implementation of these loops.


## <a name="StreamCapability"></a> interface `StreamCapabilities`

The `StreamCapabilities` provides a way to programmatically query the
capabilities that `OutputStream`, `InputStream`, or other FileSystem class
supports.

```java
public interface StreamCapabilities {
  boolean hasCapability(String capability);
}
```

### `boolean hasCapability(capability)`

Return true iff the `OutputStream`, `InputStream`, or other FileSystem class
has the desired capability.

The caller can query the capabilities of a stream using a string value.
Here is a table of possible string values:

String       | Constant   | Implements       | Description
-------------|------------|------------------|-------------------------------
hflush       | HFLUSH     | Syncable         | Flush out the data in client's user buffer. After the return of this call, new readers will see the data.
hsync        | HSYNC      | Syncable         | Flush out the data in client's user buffer all the way to the disk device (but the disk may have it in its cache). Similar to POSIX fsync.
in:readahead | READAHEAD  | CanSetReadahead  | Set the readahead on the input stream.
dropbehind   | DROPBEHIND | CanSetDropBehind | Drop the cache.
in:unbuffer  | UNBUFFER   | CanUnbuffer      | Reduce the buffering on the input stream.

## <a name="etagsource"></a> Etag probes through the interface `EtagSource`

FileSystem implementations MAY support querying HTTP etags from `FileStatus`
entries. If so, the requirements are as follows

### Etag support MUST BE across all list/`getFileStatus()` calls.

That is: when adding etag support, all operations which return `FileStatus` or `ListLocatedStatus`
entries MUST return subclasses which are instances of `EtagSource`.

### FileStatus instances MUST have etags whenever the remote store provides them.

To support etags, they MUST BE to be provided in both `getFileStatus()`
and list calls.

Implementors note: the core APIs which MUST BE overridden to achieve this are as follows:

```java
FileStatus getFileStatus(Path)
FileStatus[] listStatus(Path)
RemoteIterator<FileStatus> listStatusIterator(Path)
RemoteIterator<LocatedFileStatus> listFiles([Path, boolean)
```


### Etags of files MUST BE Consistent across all list/getFileStatus operations.

The value of `EtagSource.getEtag()` MUST be the same for list* queries which return etags for calls of `getFileStatus()` for the specific object.

```java
((EtagSource)getFileStatus(path)).getEtag() == ((EtagSource)listStatus(path)[0]).getEtag()
```

Similarly, the same value MUST BE returned for `listFiles()`, `listStatusIncremental()` of the path and
when listing the parent path, of all files in the listing.

### Etags MUST BE different for different file contents.

Two different arrays of data written to the same path MUST have different etag values when probed.
This is a requirement of the HTTP specification.

### Etags of files SHOULD BE preserved across rename operations

After a file is renamed, the value of `((EtagSource)getFileStatus(dest)).getEtag()`
SHOULD be the same as the value of `((EtagSource)getFileStatus(source)).getEtag()`
was before the rename took place.

This is an implementation detail of the store; it does not hold for AWS S3.

If and only if the store consistently meets this requirement, the filesystem SHOULD
declare in `hasPathCapability()` that it supports
`fs.capability.etags.preserved.in.rename`

### Directories MAY have etags

Directory entries MAY return etags in listing/probe operations; these entries MAY be preserved across renames.

Equally, directory entries MAY NOT provide such entries, MAY NOT preserve them acrosss renames,
and MAY NOT guarantee consistency over time.

Note: special mention of the root path "/".
As that isn't a real "directory", nobody should expect it to have an etag.

### All etag-aware `FileStatus` subclass MUST BE `Serializable`; MAY BE `Writable`

The base `FileStatus` class implements `Serializable` and  `Writable` and marshalls its fields appropriately.

Subclasses MUST support java serialization (Some Apache Spark applications use it), preserving the etag.
This is a matter of making the etag field non-static and adding a `serialVersionUID`.

The `Writable` support was used for marshalling status data over Hadoop IPC calls;
in Hadoop 3 that is implemented through `org/apache/hadoop/fs/protocolPB/PBHelper.java`and the methods deprecated.
Subclasses MAY override the deprecated methods to add etag marshalling.
However -but there is no expectation of this and such marshalling is unlikely to ever take place.

### Appropriate etag Path Capabilities SHOULD BE declared

1. `hasPathCapability(path, "fs.capability.etags.available")` MUST return true iff
    the filesystem returns valid (non-empty etags) on file status/listing operations.
2. `hasPathCapability(path, "fs.capability.etags.consistent.across.rename")` MUST return
   true if and only if etags are preserved across renames.

### Non-requirements of etag support

* There is no requirement/expectation that `FileSystem.getFileChecksum(Path)` returns
  a checksum value related to the etag of an object, if any value is returned.
* If the same data is uploaded to the twice to the same or a different path,
  the etag of the second upload MAY NOT match that of the first upload.

