import matplotlib.pyplot as plt
import numpy as np
import json
from typing import Dict, Any

all_js = {}

with open('report.log', mode='r', encoding='utf-8') as f:
    # 批量读取每一非空行并json解析，增量地更新all_js字典
    jsons = map(lambda x: json.loads(x),
                filter(lambda x: len(x) > 0,
                       map(lambda x: x.strip(),
                           f.readlines())))
    for jsn in jsons:
        all_js.update(jsn)


scenes = all_js.keys()
scenes = sorted(scenes)

kodoTimes = []
s3aTimes = []
labels = []

for scene in scenes:
    result: Dict[str, Any] = all_js[scene]
    kodoTime = result.get('kodoTime', 0)
    kodoTimes.append(kodoTime)
    s3aTime = result.get('s3aTime', 0)
    s3aTimes.append(s3aTime)
    if kodoTime <= s3aTime:
        labels.append(f'{scene} [ GOOD ]')
    else:
        labels.append(f'{scene} [ BAD ]')

# 构建数据
plt.figure(figsize=(12, 8))
x_data = labels
y_data = kodoTimes
y_data2 = s3aTimes
bar_width = 0.3

plt.barh(y=range(len(x_data)),
         width=y_data,
         height=bar_width)

plt.barh(y=np.arange(len(x_data))+bar_width,
         width=y_data2,
         height=bar_width)

# 在柱状图上显示具体数值, ha参数控制水平对齐方式, va控制垂直对齐方式
for x, y in enumerate(y_data):
    plt.text(x=y+0.02,
             y=x, s='%s' % y,
             ha='right',
             va='top')
for x, y in enumerate(y_data2):
    plt.text(x=y,
             y=x+bar_width,
             s='%s' % y,
             ha='right',
             va='bottom')

# 重新设置坐标轴刻度，不设置的话从1开始显示数字
plt.yticks(
    ticks=np.arange(len(x_data))+bar_width/2,
    labels=x_data,
)

plt.legend(["kodo", "s3a"], loc=1)

plt.subplots_adjust(
    left=0.274,
    bottom=0.057,
    right=0.954,
    top=0.977,
    wspace=0.2,
    hspace=0.2,
)
plt.show()
