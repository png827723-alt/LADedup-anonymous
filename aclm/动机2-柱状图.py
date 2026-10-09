import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

# 数据
labels = ["400Mbps", "800Mbps", "1600Mbps", "2400Mbps"]
origin = [437.64, 191.98, 97.90, 28.42]
compress = [202.39, 152.58, 122.30, 99.45]
dedup = [118.08, 91.38, 73.32, 71.97]

# 样式参数
color_ori = 'gray'
color_com = 'green'
color_dedup = 'blue'

hatch_ori = '//////////'
hatch_com = '\\\\\\\\\\\\\\\\\\\\'
hatch_dedup = 'xxxxxxxx'

width = 0.4
gap = 0.5
x = 0

# 创建 2x2 子图
fig, axs = plt.subplots(1, 4, figsize=(6.4, 2.3), sharey=False)

for i in range(4):
    ax = axs.flat[i]  # 正确访问二维数组中的第 i 个子图
    val_ori = origin[i]
    val_com = compress[i]
    val_dedup = dedup[i]

    # 三根柱子位置
    ax.bar(x  - gap, val_ori, width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
    ax.bar(x, val_com, width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
    ax.bar(x + gap, val_dedup, width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)

    ax.set_xticks([x])
    ax.set_xticklabels([labels[i]])
    # if i == 0 or i == 2:
    if i == 0:
        # ax.set_ylabel("Total Migration Time (s)")
        ax.set_ylabel("Total Migration Time (s)")
    ax.grid(axis='y', linestyle='--', linewidth=0.5)
    ax.xaxis.grid(False)

# 图例
legend_patches = [
    Patch(facecolor='white', edgecolor=color_ori, hatch=hatch_ori, label='CloudHopper', linewidth=1),
    Patch(facecolor='white', edgecolor=color_com, hatch=hatch_com, label='MBDPC', linewidth=1),
    Patch(facecolor='white', edgecolor=color_dedup, hatch=hatch_dedup, label='Deduplication', linewidth=1)
]
# fig.text(0.04, 0.5, "Total Migration Time (s)", va='center', rotation='vertical', fontsize=10)

fig.legend(handles=legend_patches,
           loc='upper center', ncol=3, frameon=False, fontsize=10)

plt.tight_layout(rect=[0, 0, 1, 0.93])
plt.savefig("mot-2.pdf", dpi=300)
plt.show()
