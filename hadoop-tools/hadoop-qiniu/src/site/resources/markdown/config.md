Qiniu Hadoop Configuration Documentation

```yaml
# Qiniu authentication, must be given, otherwise an exception will be thrown
auth:
  accessKey: ''
  secretKey: ''

# Proxy settings
proxy:
  # Whether to enable proxy
  enable: false

  # Proxy service address and port
  hostname: '127.0.0.1'
  port: 8080

  # Proxy authentication, if necessary
  username: null
  password: null

  # Proxy type，HTTP or SOCKS
  type: 'HTTP'


# Download settings
download:
  # Download url scheme weather use https. Default is true, use https.
  useHttps: true

  # File download block size, default is 4MB. If disk cache is enabled, modifying this value may clear the disk cache.
  blockSize: 4194304

  # Download domain name, string type, can be bound to cdn domain name by yourself. If this configuration item is 
  # configured, the download will be prioritized using this domain name, otherwise the default source site domain 
  # name will be used.
  domain: null

  # Download sign 
  sign:
    # Whether to enable download sign, default is true.
    enable: true

    # Expiration time, in seconds, the default is 3 minutes, and a new signature will be generated for each download.
    expires: 180

  # Download cache settings
  cache:
    # Disk cache
    disk:
      # Whether to enable disk cache, default is false.
      enable: false

      # Disk cache expiration time, in seconds, the default is 1 day, that is 24*3600.
      expires: 86400

      # Disk lru cache block number, default is 120.
      blocks: 120

      # Disk cache directory, the default is null, which will use the folder ${hadoop.tmp.dir}/qiniu
      dir: null

    # Memory cache
    memory:
      # Whether to enable memory cache, default is true.
      enable: true

      # Memory lru cache block number, default is 25.
      blocks: 25

  # Random read policy optimization, when seek is called, switch to random mode
  random:
    # Whether to enable random read policy optimization, default is false.
    enable: false

    # Random read policy optimization block size, default is 65536 bytes, that is 64KB.
    blockSize: 65536

    # Random read policy optimization block number, default is 100.
    maxBlocks: 100

# For file upload, file management related API, whether to use https, the default is true
useHttps: true

# Upload settings
upload:
  # If upload failed and the host get from uc server, use default upload host. Default value is true
  useDefaultUpHostIfNone: true

  # Upload sign
  sign:
    # Upload sign expiration time, in seconds, the default is 7 days, that is 7*24*3600=604800.
    expires: 604800

  # Use accelerate upload host first, default is false
  accUpHostFirst: false

  # The max value of upload concurrency, default is 1 
  maxConcurrentTasks: 1

  v2:
    # Whether to enable v2 upload, default is true
    enable: true

    # The uploaded block size of v2 upload, default is 32MB
    blockSize: 33554432

  # Hadoop and qiniu-java-sdk pipe buffer size, default is 16MB
  bufferSize: 16777216

# Custom region configuration
customRegion:
  # You can name the region id yourself, for example: z0, watch will used as the namespace of the region host configuration. 
  id: z0

  # Private cloud host, the namespace is the user-defined region id
  custom:
    z0:
      # The center domain name server, if this field is configured, the service will be preferred for domain name query,
      # and the subsequent related domain name fields can be configured without.
      ucServer: 'https://uc.qiniuapi.com'

      rsHost: 'rs-z0.qiniuapi.com'
      rsfHost: 'rsf-z0.qiniuapi.com'
      apiHost: 'api.qiniuapi.com'
      iovipHost: 'iovip.qiniuio.com'
      accUpHosts: [ 'upload.qiniup.com' ]
      srcUpHosts: [ 'up.qiniup.com' ]
      ioSrcHost: 'kodo-cn-east-1.qiniucs.com'

# Client simulated file system related settings
client:
  cache:
    # Whether to enable file system cache, default is true.
    enable: true

    # The maximum number of cache item, default is 1000.
    maxCapacity: 100

  copy:
    listProducer:
      # Use list api version v2, default is false
      useListV2: false

      # List limit of single request, default is 1000
      singleRequestLimit: 1000

      # Producer queue size, default is 100
      bufferSize: 100

      # Producer queue offer timeout, default is 10 seconds
      offerTimeout: 10

    batchConsumer:
      # Consumer queue size, default is 1000
      bufferSize: 1000

      # Consumer count, default is 4
      count: 4

      # Consumer single batch request limit, default is 200
      singleBatchRequestLimit: 200

      # Consumer poll timeout, default is 10 seconds
      pollTimeout: 10

  delete:
    listProducer:
      useListV2: false
      singleRequestLimit: 1000
      bufferSize: 100
      offerTimeout: 10

    batchConsumer:
      bufferSize: 1000
      count: 4
      singleBatchRequestLimit: 200
      pollTimeout: 10

  # list file settings, the configuration is the same as copy/delete listProducer
  list:
    useListV2: false
    singleRequestLimit: 1000
    bufferSize: 100
    offerTimeout: 10

logger:
  # Logger level, default is INFO, if there is an error, you can adjust it to DEBUG to get more error information.
  level: "INFO"

test:
  # Use mock kodo service in test, default is false
  useMock: true
```