Qiniu Hadoop 配置文档

```yaml
# 七牛鉴权，必须给出否则将抛出异常
auth:
  accessKey: ''
  secretKey: ''

# 代理设置
proxy:
  # 是否启用代理
  enable: false

  # 代理服务地址与端口
  hostname: '127.0.0.1'
  port: 8080

  # 用户名与密码字符串
  username: null
  password: null

  # 代理类型，HTTP or SOCKS
  type: 'HTTP'


# 文件下载相关设置
download:
  # 默认使用https
  useHttps: true
  # 文件下载块大小，默认为4MB，若启用了磁盘缓存，则修改该值可能会清空磁盘缓存
  blockSize: 4194304
  # 下载域名，字符串类型，可自行绑定cdn域名
  # 若配置了该配置项，则下载优先使用该域名，否则将使用默认源站域名
  domain: null

  # 下载签名相关
  sign:
    enable: true  # 默认启用
    # 过期时间，单位为秒，默认设置为3分钟，每次下载都会重新生成签名
    expires: 180

  # 下载缓存设置
  cache:
    # 磁盘缓存
    disk:
      enable: false   # 默认关闭
      expires: 86400  # 磁盘缓存过期时间，单位是秒，默认1天，即24*3600
      blocks: 120     # 默认设置LRU缓存块数120
      dir: null       # 下载缓冲区路径，默认为null，将使用文件夹${hadoop.tmp.dir}/qiniu

    # 内存缓存
    memory:
      enable: true  # 默认开启
      blocks: 25    # 默认设置LRU缓存块数25

  # 随机读取策略优化, 当seek被调用时，切换至random模式
  random:
    enable: false     # 默认关闭
    blockSize: 65536  # 随机读取策略模式下的单次请求块大小
    maxBlocks: 100       # 随机读取策略模式下的内存缓存块数

# 对于文件上传，文件管理相关API, 是否使用https，默认为 true
useHttps: true

# 文件上传相关设置
upload:
  # 使用 AutoRegion 时，如果从区域信息得到上传 host 失败，使用默认的上传域名上传，
  # 默认 是 upload.qiniup.com, upload-z1.qiniup.com, upload-z2.qiniup.com,
  # upload-na0.qiniup.com, upload-as0.qiniup.com
  useDefaultUpHostIfNone: true
  sign:
    # 上传签名过期时间，单位为秒，默认设置为7天，即7*24*3600
    expires: 604800
  accUpHostFirst: false
  maxConcurrentTasks: 1
  v2:
    # 是否启用v2分片上传
    enable: true
    # v2分片上传块大小，默认32MB
    blockSize: 33554432

  # hadoop与java sdk之间的pipe缓冲区大小，默认16MB
  bufferSize: 16777216

# 区域与私有云相关设置
customRegion:
  id: z0

  # 私有云ID自定义配置，子命名空间均为用户自定义region id
  custom:
    z0:
      # 中心域名服务器，若配置了该字段，则优先该服务进行域名查询，可无需配置后续的相关域名字段
      ucServer: 'https://uc.qiniuapi.com' # 中心域名服务器

      rsHost: 'rs-z0.qiniuapi.com'      # 对象管理域名
      rsfHost: 'rsf-z0.qiniuapi.com'    # 对象列举域名
      apiHost: 'api.qiniuapi.com'    # 计量查询域名
      iovipHost: 'iovip.qiniuio.com' # 源站下载域名

      # 用于上传
      accUpHosts: [ 'upload.qiniup.com' ] # 加速上传
      srcUpHosts: [ 'up.qiniup.com' ] # 源站上传

      # 默认源站域名，在hadoop中用于对象的下载
      ioSrcHost: 'kodo-cn-east-1.qiniucs.com'

# 客户端模拟文件系统相关设置
client:
  # 文件系统缓存，目前主要针对部分场景下的stat性能做缓存优化
  cache:
    enable: true
    maxCapacity: 100

  # 文件复制
  copy:
    listProducer:
      useListV2: false  # 默认不使用listV2
      singleRequestLimit: 1000  # 单次列举请求最大拉取列表限制，默认1000条
      bufferSize: 100   # 生产者队列大小
      offerTimeout: 10  # 当生产者队列满后的自旋重试的等待时间

    batchConsumer:
      bufferSize: 1000  # 消费者队列大小
      count: 4          # 消费者数量
      singleBatchRequestLimit: 200  # 消费者单批请求处理数量
      pollTimeout: 10   # 消费者拉取消费队列的自旋重试的等待时间

  # 文件删除
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

  # 文件列举，配置同 copy/delete 的 listProducer
  list:
    useListV2: false  # 默认不使用listV2
    singleRequestLimit: 1000  # 单次列举请求最大拉取列表限制，默认1000条
    bufferSize: 100   # 生产者队列大小
    offerTimeout: 10  # 当生产者队列满后的自旋重试的等待时间

logger:
  level: "INFO" # 日志级别调整，默认为INFO，若有报错，可调整为DEBUG可获取到更多错误信息

test:
  # 在测试时使用 mock 服务，可实现离线集成测试，默认为 false
  useMock: true
```