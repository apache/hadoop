import matplotlib.pyplot as plt
import numpy as np
js = {"MkdirDeeplyConcurrentlyTest": {"data": {"deep": 10, "dirs": 10, "consumers": 8}, "kodoTime": 5353, "s3aTime": 105}, "RandomOpenBigFileConcurrentlyTest": {"data": {"randomReadCount": 100, "readers": 3, "blocks": 10, "blockSize": 4194304}, "kodoTime": 4863, "s3aTime": 3243}, "SequenceOpenBigFileSeriallyTest": {"data": {"readers": 1, "blocks": 10, "readerBufferSize": 4194304, "blockSize": 4194304}, "kodoTime": 4989, "s3aTime": 3953}, "SequenceOpenBigFileConcurrentlyTest": {"data": {"readers": 1, "blocks": 10, "readerBufferSize": 4194304, "blockSize": 4194304}, "kodoTime": 4636, "s3aTime": 4295}, "MkdirLargelyConcurrentlyTest": {"data": {"dirs": 100, "consumers": 8}, "kodoTime": 12691, "s3aTime": 416}, "CreateSmallFileConcurrentlyTest": {"data": {"files": 100, "consumers": 8}, "kodoTime": 9551, "s3aTime": 1674}, "ListBigDirectoryConcurrentlyTest": {"data": {"readers": 4, "count": 10},

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                "kodoTime": 3575, "s3aTime": 4024}, "ListBigDirectorySeriallyTest": {"data": {"readers": 1, "count": 10}, "kodoTime": 11496, "s3aTime": 13176}, "CreateSmallFileSeriallyTest": {"data": {"files": 100, "consumers": 1}, "kodoTime": 23229, "s3aTime": 11735}, "MkdirLargelySeriallyTest": {"data": {"dirs": 100, "consumers": 1}, "kodoTime": 12842, "s3aTime": 2752}, "CreateBigFileSeriallyTest": {"data": {"blocks": 2, "files": 5, "consumers": 1, "blockSize": 2}, "kodoTime": 26025, "s3aTime": 5791}, "CreateBigFileConcurrentlyTest": {"data": {"blocks": 2, "files": 5, "consumers": 8, "blockSize": 2}, "kodoTime": 12244, "s3aTime": 6309}, "RandomOpenBigFileSeriallyTest": {"data": {"randomReadCount": 100, "readers": 3, "blocks": 10, "blockSize": 4194304}, "kodoTime": 5829, "s3aTime": 15267}, "MkdirDeeplySeriallyTest": {"data": {"deep": 10, "dirs": 10, "consumers": 1}, "kodoTime": 5253, "s3aTime": 375}}
scenes = js.keys()

kodoTimes = [js[scene]['kodoTime'] for scene in scenes]
s3aTimes = [js[scene]['s3aTime'] for scene in scenes]

# 构建数据
plt.figure(figsize=(12, 8))
x_data = scenes
y_data = kodoTimes
y_data2 = s3aTimes
bar_width = 0.3

plt.barh(range(len(x_data)), y_data, height=bar_width)
plt.barh(np.arange(len(x_data))+bar_width, y_data2, height=bar_width)

# 在柱状图上显示具体数值, ha参数控制水平对齐方式, va控制垂直对齐方式
for x, y in enumerate(y_data):
    plt.text(y+0.02, x, '%s' % y, ha='right', va='top')
for x, y in enumerate(y_data2):
    plt.text(y, x+bar_width, '%s' % y, ha='right', va='bottom')

# 重新设置坐标轴刻度，不设置的话从1开始显示数字
plt.yticks(np.arange(len(x_data))+bar_width/2, x_data)

plt.legend(["kodo", "s3a"], loc=1)

plt.show()
