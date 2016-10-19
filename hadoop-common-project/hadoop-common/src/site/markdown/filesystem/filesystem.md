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


## Predicates and other state access operations


### `boolean exists(Path p)`


    def exists(FS, p) = p in paths(FS)


### `boolean isDirectory(Path p)`

    def isDirectory(FS, p)= p in directories(FS)


### `boolean isFile(Path p)`


    def isFile(FS, p) = p in files(FS)


### `FileStatus getFileStatus(Path p)`

Get the status of a path

#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException

#### Postconditions

    result = stat: FileStatus where:
        if isFile(FS, p) :
            stat.length = len(FS.Files[p])
            stat.isdir = False
        elif isDir(FS, p) :
            stat.length = 0
            stat.isdir = True
        elif isSymlink(FS, p) :
            stat.length = 0
            stat.isdir = False
            stat.symlink = FS.Symlinks[p]
        if inEncryptionZone(FS, p) :
            stat.isEncrypted = True
        else
            stat.isEncrypted = False


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

    elif isDir(FS, path):
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

* After an entry at path `P` is created, and before any other
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
      exists(fs, p) else raise FileNotFoundException

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

    exists(FS, path) : raise FileNotFoundException

#### Postconditions

The operation generates a set of results, `resultset`, equal to the result of
`listStatus(path, filter)`:

    if isFile(FS, path) and filter.accept(path) :
      resultset =  [ getLocatedFileStatus(FS, path) ]

    elif isFile(FS, path) and not filter.accept(path) :
      resultset = []

    elif isDir(FS, path) :
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

    exists(FS, path) else raise FileNotFoundException

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


Note that that as `length(FS, f) ` is defined as `0` if `isDir(FS, f)`, the result
of `getFileBlockLocations()` on a directory is `[]`


If the filesystem is not location aware, it SHOULD return

      [
        BlockLocation(["localhost:9866"] ,
                  ["localhost"],
                  ["/default/localhost"]
                   0, F.getLen())
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

    result = integer >= 0

Although there is no defined minimum value for this result, as it
is used to partition work during job submission, a block size
that is too small will result in either too many jobs being submitted
for efficient work, or the `JobSubmissionClient` running out of memory.


Any FileSystem that does not actually break files into blocks SHOULD
return a number for this that results in efficient processing.
A FileSystem MAY make this user-configurable (the S3 and Swift filesystem clients do this).

###  `long getDefaultBlockSize(Path p)`

Get the "default" block size for a path —that is, the block size to be used
when writing objects to a path in the filesystem.

#### Preconditions


#### Postconditions


    result = integer  >= 0

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

    if not exists(FS, p) :  raise FileNotFoundException


#### Postconditions


    result == getFileStatus(P).getBlockSize()

The outcome of this operation MUST be identical to that contained in
the `FileStatus` returned from `getFileStatus(P)`.


## State Changing Operations

### `boolean mkdirs(Path p, FsPermission permission)`

Create a directory and all its parents

#### Preconditions


     if exists(FS, p) and not isDir(FS, p) :
         raise [ParentNotDirectoryException, FileAlreadyExistsException, IOException]


#### Postconditions


    FS' where FS'.Directories' = FS.Directories + [p] + ancestors(FS, p)
    result = True


The condition exclusivity requirement of a FileSystem's directories,
files and symbolic links must hold.

The probe for the existence and type of a path and directory creation MUST be
atomic. The combined operation, including `mkdirs(parent(F))` MAY be atomic.

The return value is always true&mdash;even if a new directory is not created
 (this is defined in HDFS).

#### Implementation Notes: Local FileSystem

The local FileSystem does not raise an exception if `mkdirs(p)` is invoked
on a path that exists and is a file. Instead the operation returns false.

    if isFile(FS, p):
       FS' = FS
       result = False

### `FSDataOutputStream create(Path, ...)`


    FSDataOutputStream create(Path p,
          FsPermission permission,
          boolean overwrite,
          int bufferSize,
          short replication,
          long blockSize,
          Progressable progress) throws IOException;


#### Preconditions

The file must not exist for a no-overwrite create:

    if not overwrite and isFile(FS, p)  : raise FileAlreadyExistsException

Writing to or overwriting a directory must fail.

    if isDir(FS, p) : raise {FileAlreadyExistsException, FileNotFoundException, IOException}


FileSystems may reject the request for other
reasons, such as the FS being read-only  (HDFS),
the block size being below the minimum permitted (HDFS),
the replication count being out of range (HDFS),
quotas on namespace or filesystem being exceeded, reserved
names, etc. All rejections SHOULD be `IOException` or a subclass thereof
and MAY be a `RuntimeException` or subclass. For instance, HDFS may raise a `InvalidPathException`.

#### Postconditions

    FS' where :
       FS'.Files'[p] == []
       ancestors(p) is-subset-of FS'.Directories'

    result = FSDataOutputStream

The updated (valid) FileSystem must contains all the parent directories of the path, as created by `mkdirs(parent(p))`.

The result is `FSDataOutputStream`, which through its operations may generate new filesystem states with updated values of
`FS.Files[p]`

#### Implementation Notes

* Some implementations split the create into a check for the file existing
 from the
 actual creation. This means the operation is NOT atomic: it is possible for
 clients creating files with `overwrite==true` to fail if the file is created
 by another client between the two tests.

* S3N, S3A, Swift and potentially other Object Stores do not currently change the FS state
until the output stream `close()` operation is completed.
This MAY be a bug, as it allows >1 client to create a file with `overwrite==false`,
 and potentially confuse file/directory logic

* The Local FileSystem raises a `FileNotFoundException` when trying to create a file over
a directory, hence it is listed as an exception that MAY be raised when
this precondition fails.

* Not covered: symlinks. The resolved path of the symlink is used as the final path argument to the `create()` operation

### `FSDataOutputStream append(Path p, int bufferSize, Progressable progress)`

Implementations without a compliant call SHOULD throw `UnsupportedOperationException`.

#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException

    if not isFile(FS, p) : raise [FileNotFoundException, IOException]

#### Postconditions

    FS
    result = FSDataOutputStream

Return: `FSDataOutputStream`, which can update the entry `FS.Files[p]`
by appending data to the existing list.


### `FSDataInputStream open(Path f, int bufferSize)`

Implementations without a compliant call SHOULD throw `UnsupportedOperationException`.

#### Preconditions

    if not isFile(FS, p)) : raise [FileNotFoundException, IOException]

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
exists in the metadata, but no copies of any its blocks can be located;
-`FileNotFoundException` would seem more accurate and useful.


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

    if isDir(FS, p) and not recursive and (children(FS, p) != {}) : raise IOException

(HDFS raises `PathIsNotEmptyDirectoryException` here.)

#### Postconditions


##### Nonexistent path

If the file does not exist the filesystem state does not change

    if not exists(FS, p):
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

    if isDir(FS, p) and isRoot(p) and children(FS, p) == {} :
        FS ' = FS
        result = (undetermined)

There is no consistent return code from an attempt to delete the root directory.

Implementations SHOULD return true; this avoids code which checks for a false
return value from overreacting.

##### Empty (non-root) directory `recursive == False`

Deleting an empty directory that is not root will remove the path from the FS and
return true.

    if isDir(FS, p) and not isRoot(p) and children(FS, p) == {} :
        FS' = (FS.Directories - [p], FS.Files, FS.Symlinks)
        result = True


##### Recursive delete of non-empty root directory

Deleting a root path with children and `recursive==True`
 can do one of two things.

1. The POSIX model assumes that if the user has
the correct permissions to delete everything,
they are free to do so (resulting in an empty filesystem).

        if isDir(FS, p) and isRoot(p) and recursive :
            FS' = ({["/"]}, {}, {}, {})
            result = True

1. HDFS never permits the deletion of the root of a filesystem; the
filesystem must be taken offline and reformatted if an empty
filesystem is desired.

        if isDir(FS, p) and isRoot(p) and recursive :
            FS' = FS
            result = False

HDFS has the notion of *Protected Directories*, which are declared in
the option `fs.protected.directories`. Any attempt to delete such a directory
or a parent thereof raises an `AccessControlException`. Accordingly, any
attempt to delete the root directory SHALL, if there is a protected directory,
result in such an exception being raised.

This specification does not recommend any specific action. Do note, however,
that the POSIX model assumes that there is a permissions model such that normal
users do not have the permission to delete that root directory; it is an action
which only system administrators should be able to perform.

Any filesystem client which interacts with a remote filesystem which lacks
such a security model, MAY reject calls to `delete("/", true)` on the basis
that it makes it too easy to lose data.

##### Recursive delete of non-root directory

Deleting a non-root path with children `recursive==true`
removes the path and all descendants

    if isDir(FS, p) and not isRoot(p) and recursive :
        FS' where:
            not isDir(FS', p)
            and forall d in descendants(FS, p):
                not isDir(FS', d)
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

In terms of its specification, `rename()` is one of the most complex operations within a filesystem .

In terms of its implementation, it is the one with the most ambiguity regarding when to return false
versus raising an exception.

Rename includes the calculation of the destination path.
If the destination exists and is a directory, the final destination
of the rename becomes the destination + the filename of the source path.

    let dest = if (isDir(FS, src) and d != src) :
            d + [filename(src)]
        else :
            d

#### Preconditions

All checks on the destination path MUST take place after the final `dest` path
has been calculated.

Source `src` must exist:

    exists(FS, src) else raise FileNotFoundException


`dest` cannot be a descendant of `src`:

    if isDescendant(FS, src, dest) : raise IOException

This implicitly covers the special case of `isRoot(FS, src)`.

`dest` must be root, or have a parent that exists:

    isRoot(FS, dest) or exists(FS, parent(dest)) else raise IOException

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
            and data(FS', dest) == data (FS, dest)
        result = True



##### Renaming a directory onto a directory

If `src` is a directory then all its children will then exist under `dest`, while the path
`src` and its descendants will no longer exist. The names of the paths under
`dest` will match those under `src`, as will the contents:

    if isDir(FS, src) isDir(FS, dest) and src != dest :
        FS' where:
            not exists(FS', src)
            and dest in FS'.Directories]
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

    FS' = FS; result = False

*Local Filesystem, S3N*

The outcome is as a normal rename, with the additional (implicit) feature
that the parent directories of the destination also exist.

    exists(FS', parent(dest))

*Other Filesystems (including Swift) *

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


### `void concat(Path p, Path sources[])`

Joins multiple blocks together to create a single file. This
is a little-used operation currently implemented only by HDFS.

Implementations without a compliant call SHOULD throw `UnsupportedOperationException`.

#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException

    if sources==[] : raise IllegalArgumentException

All sources MUST be in the same directory:

    for s in sources: if parent(S) != parent(p) raise IllegalArgumentException

All block sizes must match that of the target:

    for s in sources: getBlockSize(FS, S) == getBlockSize(FS, p)

No duplicate paths:

    not (exists p1, p2 in (sources + [p]) where p1 == p2)

HDFS: All source files except the final one MUST be a complete block:

    for s in (sources[0:length(sources)-1] + [p]):
      (length(FS, s) mod getBlockSize(FS, p)) == 0


#### Postconditions


    FS' where:
     (data(FS', T) = data(FS, T) + data(FS, sources[0]) + ... + data(FS, srcs[length(srcs)-1]))
     and for s in srcs: not exists(FS', S)


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

    if isDir(FS, p) : raise [FileNotFoundException, IOException]

    if newLength < 0 || newLength > len(FS.Files[p]) : raise HadoopIllegalArgumentException

HDFS: The source file MUST be closed.
Truncate cannot be performed on a file, which is open for writing or appending.

#### Postconditions

    FS' where:
        len(FS.Files[p]) = newLength

Return: `true`, if truncation is finished and the file can be immediately
opened for appending, or `false` otherwise.

HDFS: HDFS returns `false` to indicate that a background process of adjusting
the length of the last block has been started, and clients should wait for it
to complete before they can proceed with further file updates.

#### Concurrency

If an input stream is open when truncate() occurs, the outcome of read
operations related to the part of the file being truncated is undefined.



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
for a dicussion on this topic).

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
