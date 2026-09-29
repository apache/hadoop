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

## Configuration properties

### General configuration

* `fs.gs.project.id` (not set by default)

  Google Cloud Project ID with access to Google Cloud Storage buckets.
  Required only for list buckets and create bucket operations.

* `fs.gs.working.dir` (default: `/`)

  The directory relative `gs:` uris resolve in inside the default bucket.

* `fs.gs.rewrite.max.chunk.size` (default: `512m`)

  Maximum size of object chunk that will be rewritten in a single rewrite
  request when `fs.gs.copy.with.rewrite.enable` is set to `true`.

* `fs.gs.bucket.delete.enable` (default: `false`)

  If `true`, recursive delete on a path that refers to a Cloud Storage bucket
  itself or delete on that path when it is empty will result in deletion of
  the bucket itself. If `false`, any operation that normally would have
  deleted the bucket will be ignored. Setting to `false` preserves the typical
  behavior of `rm -rf /` which translates to deleting everything inside of
  root, but without clobbering the filesystem authority corresponding to that
  root path in the process.

* `fs.gs.block.size` (default: `64m`)

  The reported block size of the file system. This does not change any
  behavior of the connector or the underlying Google Cloud Storage objects.
  However, it will affect the number of splits Hadoop MapReduce uses for a
  given input.

* `fs.gs.create.items.conflict.check.enable` (default: `true`)

  Enables a check that ensures that conflicting directories do not exist when
  creating files and conflicting files do not exist when creating directories.

* `fs.gs.marker.file.pattern` (not set by default)

  If set, files that match specified pattern are copied last during folder
  rename operation.

### Authentication

* `fs.gs.auth.type` (default: `COMPUTE_ENGINE`)

  What type of authentication mechanism to use for Google Cloud Storage
  access.

  Valid values:

  * `APPLICATION_DEFAULT` - configures
    [Application Default Credentials](https://javadoc.io/doc/com.google.auth/google-auth-library-oauth2-http/latest/com/google/auth/oauth2/GoogleCredentials.html)
    authentication

  * `COMPUTE_ENGINE` - configures Google Compute Engine service account
    authentication

  * `SERVICE_ACCOUNT_JSON_KEYFILE` - configures JSON keyfile service account
    authentication

  * `UNAUTHENTICATED` - configures unauthenticated access

  * `USER_CREDENTIALS` - configure [user credentials](#user-credentials)

* `fs.gs.auth.service.account.json.keyfile` (not set by default)

  The path to the JSON keyfile for the service account when `fs.gs.auth.type`
  property is set to `SERVICE_ACCOUNT_JSON_KEYFILE`. The file must exist at
  the same path on all nodes

#### User credentials

User credentials allows you to access Google resources on behalf of a user, with
the according permissions associated to this user.

To achieve this the connector will use the
[refresh token grant flow](https://oauth.net/2/grant-types/refresh-token/) to
retrieve a new access tokens when necessary.

In order to use this authentication type, you will first need to retrieve a
refresh token using the
[authorization code grant flow](https://oauth.net/2/grant-types/authorization-code)
and pass it to the connector with OAuth client ID and secret:

* `fs.gs.auth.client.id` (not set by default)

  The OAuth2 client ID.

* `fs.gs.auth.client.secret` (not set by default)

  The OAuth2 client secret.

* `fs.gs.auth.refresh.token` (not set by default)

  The refresh token.

### IO configuration

* `fs.gs.inputstream.support.gzip.encoding.enable` (default: `false`)

  If set to `false` then reading files with GZIP content encoding (HTTP header
  `Content-Encoding: gzip`) will result in failure (`IOException` is thrown).

  This feature is disabled by default because processing of
  [GZIP encoded](https://cloud.google.com/storage/docs/transcoding#decompressive_transcoding)
  files is inefficient and error-prone in Hadoop and Spark.

* `fs.gs.outputstream.buffer.size` (default: `8m`)

  Write buffer size used by the file system API to send the data to be
  uploaded to Cloud Storage upload thread via pipes. The various pipe types
  are documented below.

* `fs.gs.outputstream.sync.min.interval` (default: `0`)

  Output stream configuration that controls the minimum interval between
  consecutive syncs. This allows to avoid getting rate-limited by Google Cloud
  Storage. Default is `0` - no wait between syncs. Note that `hflush()` will
  be no-op if called more frequently than minimum sync interval and `hsync()`
  will block until an end of a min sync interval.

### Fadvise feature configuration

* `fs.gs.inputstream.fadvise` (default: `AUTO`)

  Tunes reading objects behavior to optimize HTTP GET requests for various use
  cases.

  This property controls fadvise feature that allows to read objects in
  different modes:

  * `SEQUENTIAL` - in this mode connector sends a single streaming
    (unbounded) Cloud Storage request to read object from a specified
    position sequentially.

  * `RANDOM` - in this mode connector will send bounded Cloud Storage range
    requests (specified through HTTP Range header) which are more efficient
    in some cases (e.g. reading objects in row-columnar file formats like
    ORC, Parquet, etc).

    Range request size is limited by whatever is greater, `fs.gs.io.buffer`
    or read buffer size passed by a client.

    To avoid sending too small range requests (couple bytes) - could happen
    if `fs.gs.io.buffer` is 0 and client passes very small read buffer,
    minimum range request size is limited to 2 MB by default configurable
    through `fs.gs.inputstream.min.range.request.size` property

  * `AUTO` - in this mode (adaptive range reads) connector starts to send
    bounded range requests when reading non gzip-encoded objects instead of
    streaming requests as soon as first backward read or forward read for
    more than `fs.gs.inputstream.inplace.seek.limit` bytes was detected.

  * `AUTO_RANDOM` - It is complementing `AUTO` mode which uses sequential
    mode to start with and adapts to bounded range requests. `AUTO_RANDOM`
    mode uses bounded channel initially and adapts to sequential requests if
    consecutive requests are within `fs.gs.inputstream.min.range.request.size`.
    gzip-encode object will bypass this adoption, it will always be a
    streaming(unbounded) channel. This helps in cases where egress limits is
    getting breached for customer because `AUTO` mode will always lead to
    one unbounded channel for a file. `AUTO_RANDOM` will avoid such unwanted
    unbounded channels.

* `fs.gs.fadvise.request.track.count` (default: `3`)

  Self adaptive fadvise mode uses distance between the served requests to
  decide the access pattern. This property controls how many such requests
  need to be tracked. It is used when `AUTO_RANDOM` is selected.

* `fs.gs.inputstream.inplace.seek.limit` (default: `8m`)

  If forward seeks are within this many bytes of the current position, seeks
  are performed by reading and discarding bytes in-place rather than opening a
  new underlying stream.

* `fs.gs.inputstream.min.range.request.size` (default: `2m`)

  Minimum size in bytes of the read range for Cloud Storage request when
  opening a new stream to read an object.