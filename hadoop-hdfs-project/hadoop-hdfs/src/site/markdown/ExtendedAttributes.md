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

Extended Attributes in HDFS
===========================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

*Extended attributes* (abbreviated as *xattrs*) are a filesystem feature that allow user applications to associate additional metadata with a file or directory. Unlike system-level inode metadata such as file permissions or modification time, extended attributes are not interpreted by the system and are instead used by applications to store additional information about an inode. Extended attributes could be used, for instance, to specify the character encoding of a plain-text document.

### HDFS extended attributes

Extended attributes in HDFS are modeled after extended attributes in Linux (see the Linux manpage for [attr(5)](http://man7.org/linux/man-pages/man5/attr.5.html)). An extended attribute is a *name-value pair*, with a string name and binary value. Xattrs names must also be prefixed with a *namespace*. For example, an xattr named *myXattr* in the *user* namespace would be specified as **user.myXattr**. Multiple xattrs can be associated with a single inode.

### Namespaces and Permissions

In HDFS, there are five valid namespaces: `user`, `trusted`, `system`, `security`, and `raw`. Each of these namespaces have different access restrictions.

The `user` namespace is the namespace that will commonly be used by client applications. Access to extended attributes in the user namespace is controlled by the corresponding file permissions.

The `trusted` namespace is available only to HDFS superusers.

The `system` namespace is reserved for internal HDFS use. This namespace is not accessible through userspace methods, and is reserved for implementing internal HDFS features.

The `security` namespace is reserved for internal HDFS use. This namespace is generally not accessible through userspace methods. One particular use of `security` is the `security.hdfs.unreadable.by.superuser` extended attribute. This xattr can only be set on files, and it will prevent the superuser from reading the file's contents. The superuser can still read and modify file metadata, such as the owner, permissions, etc. This xattr can be set and accessed by any user, assuming normal filesystem permissions. This xattr is also write-once, and cannot be removed once set. This xattr does not allow a value to be set.

The `raw` namespace is reserved for internal system attributes that sometimes need to be exposed. Like `system` namespace attributes they are not visible to the user except when `getXAttr`/`getXAttrs` is called on a file or directory in the `/.reserved/raw` HDFS directory hierarchy. These attributes can only be accessed by the superuser. An example of where `raw` namespace extended attributes are used is the `distcp` utility. Encryption zone meta data is stored in `raw.*` extended attributes, so as long as the administrator uses `/.reserved/raw` pathnames in source and target, the encrypted files in the encryption zones are transparently copied.

Interacting with extended attributes
------------------------------------

The Hadoop shell has support for interacting with extended attributes via `hadoop fs -getfattr` and `hadoop fs -setfattr`. These commands are styled after the Linux [getfattr(1)](http://man7.org/linux/man-pages/man1/getfattr.1.html) and [setfattr(1)](http://man7.org/linux/man-pages/man1/setfattr.1.html) commands.

### getfattr

`hadoop fs -getfattr [-R] -n name | -d [-e en] <path`\>

Displays the extended attribute names and values (if any) for a file or directory.

| | |
|:---- |:---- |
| -R | Recursively list the attributes for all files and directories. |
| -n name | Dump the named extended attribute value. |
| -d | Dump all extended attribute values associated with pathname. |
| -e \<encoding\> | Encode values after retrieving them. Valid encodings are "text", "hex", and "base64". Values encoded as text strings are enclosed in double quotes ("), and values encoded as hexadecimal and base64 are prefixed with 0x and 0s, respectively. |
| \<path\> | The file or directory. |

### setfattr

`hadoop fs -setfattr -n name [-v value] | -x name <path`\>

Sets an extended attribute name and value for a file or directory.

| | |
|:---- |:---- |
| -n name | The extended attribute name. |
| -v value | The extended attribute value. There are three different encoding methods for the value. If the argument is enclosed in double quotes, then the value is the string inside the quotes. If the argument is prefixed with 0x or 0X, then it is taken as a hexadecimal number. If the argument begins with 0s or 0S, then it is taken as a base64 encoding. |
| -x name | Remove the extended attribute. |
| \<path\> | The file or directory. |

Configuration options
---------------------

HDFS supports extended attributes out of the box, without additional configuration. Administrators could potentially be interested in the options limiting the number of xattrs per inode and the size of xattrs, since xattrs increase the on-disk and in-memory space consumption of an inode.

*   `dfs.namenode.xattrs.enabled`

    Whether support for extended attributes is enabled on the NameNode. By default, extended attributes are enabled.

*   `dfs.namenode.fs-limits.max-xattrs-per-inode`

    The maximum number of extended attributes per inode. By default, this limit is 32.

*   `dfs.namenode.fs-limits.max-xattr-size`

    The maximum combined size of the name and value of an extended attribute in bytes. By default, this limit is 16384 bytes.


