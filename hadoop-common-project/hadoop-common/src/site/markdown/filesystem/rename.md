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

# Renaming File and Directories

The `rename()` operation is the one which causes the most problems for anyone implementing
a filesystem or Hadoop filesystem client.


It has a number of key problems

* It is a critical part of algorithms used to commit work in MapReduce, Spark, Hive, etc.
* It is an operation which spans multiple directories yet is expected to be a atomic, so all
  stores have to implement some lock mechanism at a larger granularity than just a 
  single directory.
* Any file system with mount points and multiple volumes is going to
  fail when any attempt is made to rename eight file or directory to a
  different mount point/volume etc.
  There is no way to probe for `rename()` being permitted without actually trying.
* Encryption zones can have similar issues.
* In those object stores where the notion of "directory" does not exist,
  a lot of effort has to go into making rename pretend to work,
  yet the atomicity requirements of commit algorithms are unlikely to be satisfied.
* Nobody really understand all the nuances of the semantics of rename.
* The original `FileSystem.rename()` has behaviors which do not match that
  of POSIX and which are unwelcome.
* When the local filesystem's rename operations are invoked they are
  (a) platform dependent and (b) accessed via Java 1.0 File APIs' rename(), which itself has
  similar issues.  Indeed, the `java.io.FileSystem.rename()` call is clearly
  something that Hadoop's `rename/2` call is based on.

## Notation; rename/2 and rename/3. 

This document uses the term s

* *rename/2* : the `FileSystm.rename(source, dest)` call with two arguments ("arity of two").
* *rename/3* : the `rename(source, dest, options)` call with three arguments ("arity of three") which 
is implemented in both `FileContext` and `FileSystem` *and is implemented consistently in both places.

Of the two, rename/3 is better because it eliminates the "helpful" destination path recalculation,
resolves ambiguity about what renaming a directory into an empty directory does, *and* fails
meaningfully.

### <a name="rename2"></a> `boolean rename(Path source, Path dest, Rename...options))` , "rename/3"

### `void rename(Path source, Path dest, Rename...options)`



This is a better implementation of path rename than its predecessor
`rename(src, dest)` (rename/2).


1. Does not redefine the destination path based on whether or not the supplied
destination is a directory and the source a file.
1. Defines the policy on overwriting paths.
1. Defines the policy on nonexistent source paths.
1. MUST raise an exception on all failure conditions.
1. Has no return code. If the method does not raise an exception, it has
succeeded.

The semantics of this method are simpler and more clearly defined.

- The final path of the rename is always that of the `dest` argument; there
is no attempt to generate a new path underneath a target directory. 
- If that destination exists, unless the `Rename.OVERWRITE` is set, an exception
is raised.
- The parent directory MUST exist.

This avoids the "quirks" of `rename(source, dest)` where the behaviour
of the rename depends on the type and state of the destination.


The `Rename` enumeration has three values:

* `NONE`: This is a no-op entry.
* `OVERWRITE`: overwrite any destination file or empty destination directory.
* `TRASH`: flag to indicate that the destination is the trash portion of
  the filesystem. This flag is used in HDFS to verify that the caller has
  the appropriate delete permission as well as the rename permission.

As multiple entries can be supplied, all probes for features are done by
checking for the specific flag in the options list. There is no requirement of
ordering, and it is not an error if there are duplicate entries.

```python
let overwrite = Rename.OVERWRITE in options
```

#### Preconditions

Source `source` MUST exist:

```python
exists(FS, source) else raise FileNotFoundException
```


`source` MUST NOT be root:

```python
if isRoot(FS, source)): raise IOException
```

`dest` MUST NOT equal `source`

```python
source != dest else raise FileAlreadyExistsException
```

`dest` MUST NOT be a descendant of `source`:

```python
if isDescendant(FS, source, dest) : raise IOException
```

`dest` MUST NOT be root:

```python
if isRoot(FS, dest)): raise IOException
```


The parent directory of `dest` MUST exist:

```python
exists(FS, parent(dest)) else raise FileNotFoundException
```


The parent path of a destination MUST be a directory:

```python
isDirectory(FS, parent(dest)) else raise ParentNotDirectoryException, IOException
```

This implicitly covers all the ancestors of the parent.

If `dest` exists the type of the entry MUST match
that of the source.

```python
if exists(FS, dest) and isFile(FS, dest) and not isFile(FS, src): raise IOException

if exists(FS, dest) and isDirectory(FS, dest) and not isDirectory(FS, src): raise IOException
```

There must be not be an existing file at the end of the destination path unless
`Rename.OVERWRITE` was set.

```python
if isFile(FS, dest) and not overwrite: raise FileAlreadyExistsException
```


If the source is a directory, there must be not be an existing directory at the end of
the destination path unless `Rename.OVERWRITE` was set.

```python
if isDirectory(FS, dest) and not overwrite and isDirectory(FS, source): raise FileAlreadyExistsException
```


If there is a directory at the end of the path, it must be empty, irrespective of
the overwrite flag:

```python
if isDirectory(FS, source) and isDirectory(FS, dest) and len(listStatus(FS, dest)) > 0:
    raise IOException
```

Note how this is different from `rename(src, dest)`.


#### Postconditions

##### Renaming a file onto an empty path or an existing file

Renaming a file to a path where there is no entry moves the file to the
destination path. If the destination exists, the operation MUST fail unless the
`overwrite` option is not set. If `overwrite` is set the semantics are the same
as renaming onto an empty path.

```python
if isFile(FS, source) and not exists(dest) or (isFile(dest) and overwrite):
    FS' where:
        exists(FS', dest)
        and data(FS', dest) == data(FS, source)
        and not exists(FS', source)
```

##### Renaming a directory

If `source` is a directory then all its children will then exist under `dest` in `FS'`, while the path
`source` and its descendants will no longer exist. The names of the paths under
`dest` will match those under `source`, as will the contents:

```python
if isDirectory(FS, source) :
    FS' where:
        isDirectory(FS, dest)
        and forall c in descendants(FS, source) where isDirectory(FS, c):
            isDirectory(FS', dest + childElements(source, c))
        and forall c in descendants(FS, source) where not isDirectory(FS, c):
            data(FS', dest + childElements(s, c)) == data(FS, c)
        and not exists(FS', source)
        and forall c in descendants(FS, source):
            not exists(FS', c))
```

##### Concurrency requirements

* The core operation of rename/3, moving one entry in the filesystem to
another, SHOULD be atomic. Many applications rely on this as a way to commit operations.

* However, the base implementation is *not* atomic; some of the precondition checks
are performed separately. 

HDFS's rename/3 operation *is* atomic; other filesystems SHOULD follow its example.

##### Implementation Notes

**Files open for reading, writing or appending**

The behavior of rename/3 on an open file is unspecified: whether it is
allowed, what happens to later attempts to read from or write to the open stream


### <a name="rename2"></a> `boolean rename(Path source, Path dest)` , "rename/2"

In terms of its specification, `rename(Path source, Path dest)` is one of the most
complex APIs operations within a filesystem.

In terms of its implementation, it is the one with the most ambiguity regarding when to return false
versus raising an exception. This makes less useful to use than
others: in production code it is usually wrapped by code to raise an 
exception when `false` is returned.

```java
if (!fs.rename(source, dest)) {
  throw new IOException("rename failed!!!");
}
```

The weakness of this approach is not just that all uses need to be so
guarded, the actual cause of the failure is lost.

Rename/2 includes the calculation of the destination path.
If the source is a file, and the destination is an (existing)
directory, the final destination
of the rename becomes the destination + the filename of the source path.

```python
let dest' = if (isFile(FS, source) and isDirectory(FS, dest)) :
        path(dest, filename(source))
    else :
        dest
```

This is a needless complication whose origin is believed to have been
based on experience with the unix `mv` command, rather than the posix API.



#### Preconditions

All checks on the destination path MUST take place after the final `dest` path
has been calculated.

Source `source` must exist:

```python
exists(FS, source) else raise FileNotFoundException
```

`dest` cannot be a descendant of `source`:

```python
if isDescendant(FS, source, dest) : raise IOException
```

This implicitly covers the special case of `isRoot(FS, source)`.

`dest` must be root, or have a parent that exists:

```python
isRoot(FS, dest) or exists(FS, parent(dest)) else raise IOException
```

The parent path of a destination must not be a file:

```python
if isFile(FS, parent(dest)) : raise IOException
```

This implicitly covers all the ancestors of the parent.

There must not be an existing file at the end of the destination path:

```python
if isFile(FS, dest) : raise FileAlreadyExistsException, IOException
```


#### Postconditions


##### Renaming a directory onto itself

Renaming a directory onto itself is no-op; return value is not specified.

In POSIX the result is `False`;  in HDFS the result is `True`.

```python
if isDirectory(FS, source) and source == dest :
    FS' = FS
    result = (undefined)
```


##### Renaming a file to self

Renaming a file to itself is a no-op; the result is `True`.

```python
if isFile(FS, source) and source == dest :
    FS' = FS
    result = True
```



##### Renaming a file onto a nonexistent path

Renaming a file where the destination is a directory moves the file as a child
 of the destination directory, retaining the filename element of the source path.

```python
if isFile(FS, source) and source != dest:
    FS' where:
        not exists(FS', source)
        and exists(FS', dest)
        and data(FS', dest) == data (FS, dest)
    result = True
```



##### Renaming a directory under a directory

If `source` is a directory then all its children will then exist under `dest`, while the path
`source` and its descendants will no longer exist. The names of the paths under
`dest` will match those under `source`, as will the contents:

```python
if isDirectory(FS, source) and source != dest :
    FS' where:
        not exists(FS', source)
        and dest in FS'.Directories
        and forall c in descendants(FS, source) :
            not exists(FS', c))
        and forall c in descendants(FS, source) where isDirectory(FS, c):
            isDirectory(FS', dest + childElements(source, c))
        and forall c in descendants(FS, source) where not isDirectory(FS, c):
            data(FS', dest + childElements(s, c)) == data(FS, c)
```



##### Renaming into a path where the parent path does not exist

```python
not exists(FS, parent(dest))
```

There is no consistent behavior here.

*HDFS*

The outcome is no change to FileSystem state, with a return value of false.

```python
FS' = FS; result = False
```


*Local Filesystem*

The outcome is as a normal rename, with the additional (implicit) feature
that the parent directories of the destination also exist.

```python
exists(FS', parent(dest))
```


*Other Filesystems *

Other filesystems strictly reject the operation, raising a `FileNotFoundException`

##### Concurrency requirements

* The core operation of `rename/2`, moving one entry in the filesystem to
another, MUST be atomic. Many applications rely on this as a way to commit operations.

* Some FileSystem implementations perform checks on the destination
FileSystem before and after the rename. One example of this is `ChecksumFileSystem`, which
provides checksummed access to local data. The entire sequence MAY NOT be atomic.

##### Implementation Notes

**Files open for reading, writing or appending**

The behavior of `rename/2` on an open file is unspecified: whether it is
allowed, what happens to later attempts to read from or write to the open stream

**Renaming a directory onto itself**

The return code of renaming a directory onto itself is unspecified.

**Destination exists and is a file**

Renaming a file atop an existing file is specified as failing, raising an exception.

* Local FileSystem : the rename succeeds; the destination file is replaced by the source file.

* HDFS : The rename fails, no exception is raised. Instead the method call simply returns false.

**Missing source file**

If the source file `source` does not exist,  `FileNotFoundException` should be raised.

HDFS fails without raising an exception; `rename()` merely returns false.

    FS' = FS
    result = false

The behavior of HDFS here must not be considered a feature to replicate.


## References

- [Posix `rename()`](https://pubs.opengroup.org/onlinepubs/9699919799/functions/rename.html)

- [rename(2) — Linux manual page](https://man7.org/linux/man-pages/man2/rename.2.html)

> If newpath already exists, it will be atomically replaced, so that
there is no point at which another process attempting to access
newpath will find it missing.  However, there will probably be a
window in which both oldpath and newpath refer to the file being
renamed.

Linux extends the Posix arity-2 rename with their own rename/3 API call, one which blocks
overwriting.
