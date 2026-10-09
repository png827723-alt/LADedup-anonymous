import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

# 数据
labels_grouped = [
    ["Redis\n40MB", "Redis\n100MB"],
    ["KeyDB\n40MB", "KeyDB\n100MB"],
    ["Dragonfly\n40MB", "Dragonfly\n100MB"],
    ["MySQL\n20Th", "MySQL\n50Th"],
    ["Postgre\n20Th", "Postgre\n50Th"],
    ["Machine\nlearning", "NPB\n-CG"]

]

#15991  29765  13255
origin_size = [5000, 6000, 5000, 6000, 1997, 7990, 1413, 1724, 722, 854,1078,1345]
compression_size = [8126, 5000, 6443, 5000, 312, 4741, 233, 424, 56, 99,712,1006]
deduplication_size = [2083, 5360, 2367, 5487, 1368, 2283, 1154, 1595, 497, 923,1362]
aware_size = [1856, 4268, 2135, 5022, 333, 2469, 230, 402, 50,100,562,1024]

# 样式
color_ori = 'gray'
color_com = 'green'
color_dedup = 'blue'
color_aware = 'red'

hatch_ori = '//////////'
hatch_com = '\\\\\\\\\\\\\\\\\\\\'
hatch_dedup = 'xxxxxxxx'
hatch_aware = '........'

width = 0.2
gap = 0.04

# 创建 1x4 子图
fig, axs = plt.subplots(1, 6, figsize=(10, 2.1), sharey=False)

# Redis
x0 = np.arange(2)*1.3
axs[0].bar(x0 - 1.5 * width-1.5*gap, origin_size[0:2], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[0].bar(x0 - 0.5 * width-0.5*gap, compression_size[0:2], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[0].bar(x0 + 0.5 * width+0.5*gap, deduplication_size[0:2], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[0].bar(x0 + 1.5 * width+1.5*gap, aware_size[0:2], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[0].set_xticks(x0)
axs[0].set_xticklabels(labels_grouped[0], ha='center', fontsize=8)
axs[0].set_ylabel("Data Transfer Size (MB)")
axs[0].grid(axis='y', linestyle='--', linewidth=0.5)
axs[0].xaxis.grid(False)
axs[0].bar(x0 - 1.5 * width-1.5*gap, origin_size[0:2], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[0].text(x0[0] - 1.5 * width-1.5*gap, origin_size[0] + 2, 15991, ha='center', va='bottom', fontsize=8)
axs[0].text(x0[1] - 0.5 * width-0.5*gap, compression_size[1] + 2, 13255, ha='center', va='bottom', fontsize=8)
axs[0].text(x0[1] - 1.5 * width-1.5*gap, origin_size[1] + 2, 29765, ha='center', va='bottom', fontsize=8)

# KeyDB
x1 = np.arange(2)*1.3
axs[1].bar(x1 - 1.5 * width -1.5*gap, origin_size[2:4], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[1].bar(x1 - 0.5 * width-0.5*gap, compression_size[2:4], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[1].bar(x1 + 0.5 * width+0.5*gap, deduplication_size[2:4], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[1].bar(x1 + 1.5 * width+1.5*gap, aware_size[2:4], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[1].set_xticks(x1)
axs[1].set_xticklabels(labels_grouped[1], ha='center', fontsize=8)
axs[1].grid(axis='y', linestyle='--', linewidth=0.5)
axs[1].xaxis.grid(False)
axs[1].text(x0[0] - 1.5 * width-1.5*gap, origin_size[2] + 2, 14218, ha='center', va='bottom', fontsize=8)
axs[1].text(x0[1] - 0.5 * width-0.5*gap, compression_size[3] + 2, 14916, ha='center', va='bottom', fontsize=8)
axs[1].text(x0[1] - 1.5 * width-1.5*gap, origin_size[3] + 2, 27121, ha='center', va='bottom', fontsize=8)
# Dragonfly
x2 = np.arange(2)*1.3
axs[2].bar(x2 - 1.5 * width  -1.5*gap, origin_size[4:6], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[2].bar(x2 - 0.5 * width-0.5*gap, compression_size[4:6], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[2].bar(x2 + 0.5 * width+0.5*gap, deduplication_size[4:6], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[2].bar(x2 + 1.5 * width +1.5*gap, aware_size[4:6], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[2].set_xticks(x2)
axs[2].set_xticklabels(labels_grouped[2], ha='center', fontsize=8)
axs[2].grid(axis='y', linestyle='--', linewidth=0.5)
axs[2].xaxis.grid(False)

# MySQL
x3 = np.arange(2)*1.3
axs[3].bar(x3 - 1.5 * width -1.5*gap, origin_size[6:8], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[3].bar(x3 - 0.5 * width-0.5*gap, compression_size[6:8], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[3].bar(x3 + 0.5 * width+0.5*gap, deduplication_size[6:8], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[3].bar(x3 + 1.5 * width +1.5*gap, aware_size[6:8], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[3].set_xticks(x3)
axs[3].set_xticklabels(labels_grouped[3],ha='center', fontsize=8)
axs[3].grid(axis='y', linestyle='--', linewidth=0.5)
axs[3].xaxis.grid(False)

# PostgreSQL
x4 = np.arange(2)*1.4
axs[4].bar(x4 - 1.5 * width -1.5*gap, origin_size[8:10], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[4].bar(x4 - 0.5 * width-0.5*gap, compression_size[8:10], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[4].bar(x4 + 0.5 * width+0.5*gap, deduplication_size[8:10], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[4].bar(x4 + 1.5 * width +1.5*gap, aware_size[8:10], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[4].set_xticks(x4)
axs[4].set_xticklabels(labels_grouped[4],ha='center', fontsize=8)
axs[4].grid(axis='y', linestyle='--', linewidth=0.5)
axs[4].xaxis.grid(False)

# ml和科学计算
x5 = np.arange(2)*1.4
axs[5].bar(x5 - 1.5 * width -1.5*gap, origin_size[10:12], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[5].bar(x5 - 0.5 * width-0.5*gap, compression_size[10:12], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[5].bar(x5 + 0.5 * width+0.5*gap, deduplication_size[10:12], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[5].bar(x5 + 1.5 * width +1.5*gap, aware_size[10:12], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[5].set_xticks(x5)
axs[5].set_xticklabels(labels_grouped[5],ha='center', fontsize=8)
axs[5].grid(axis='y', linestyle='--', linewidth=0.5)
axs[5].xaxis.grid(False)


# 图例
legend_patches = [
    Patch(facecolor='white', edgecolor=color_ori, hatch=hatch_ori, label='CloudHopper'),
    Patch(facecolor='white', edgecolor=color_com, hatch=hatch_com, label='MBDPC'),
    Patch(facecolor='white', edgecolor=color_dedup, hatch=hatch_dedup, label='Deduplication'),
    Patch(facecolor='white', edgecolor=color_aware, hatch=hatch_aware, label='AwareCLM')
]
fig.legend(handles=legend_patches, loc='upper center', ncol=4, frameon=False, fontsize=9)

plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig("eva-trans.pdf", dpi=300)
plt.show()
