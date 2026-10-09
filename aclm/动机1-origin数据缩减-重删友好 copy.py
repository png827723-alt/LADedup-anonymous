import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

# 数据
origin = [15991, 14218]
compress = [8350, 7411]
dedup = [3013, 2936]
labels = ['Redis-40MB', 'KeyDB-40MB']

color_ori = 'gray'
color_com = 'green'
color_dedup = 'blue'

hatch_ori = '//////////'
hatch_com = '\\\\\\\\\\\\\\\\\\\\'
hatch_dedup = 'xxxxxxxx'

width = 0.2
gap = 0.02

x = np.arange(len(labels))*0.75  # [0, 1] 两组位置

fig, ax = plt.subplots(figsize=(4.5, 2.5))

# 绘制柱状图
ax.bar(x - width - gap, origin, width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
ax.bar(x, compress, width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
ax.bar(x + width + gap, dedup, width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)

# 坐标与样式设置
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.set_ylabel("Total Footprint of Dirty Pages (MB)")
ax.grid(axis='y', linestyle='--', linewidth=0.5)
ax.xaxis.grid(False)

# 图例
legend_patches = [
    Patch(facecolor='white', edgecolor=color_ori, hatch=hatch_ori, label='CloudHopper', linewidth=1),
    Patch(facecolor='white', edgecolor=color_com, hatch=hatch_com, label='MBDPC', linewidth=1),
    Patch(facecolor='white', edgecolor=color_dedup, hatch=hatch_dedup, label='Deduplication', linewidth=1)
]
fig.legend(handles=legend_patches, loc='upper center', ncol=3, frameon=False, fontsize=10)

plt.tight_layout(rect=[0, 0, 1, 0.93])
plt.savefig("mot-1-1.pdf", dpi=300)
plt.show()
