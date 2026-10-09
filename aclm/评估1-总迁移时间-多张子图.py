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

origin_tmt = [191.98, 150, 188.95, 150, 34.1, 112.2, 26, 33.79,26.6,31.8,30,33.9]
compression_tmt = [152.58, 120, 151.44, 120, 13.89, 84.2, 14.45, 20.9,6,12.4,25.6,27.8]
deduplication_tmt = [82, 92.02, 74.96, 89.2, 33.91, 71.8, 28.47, 36.3,21.7,32.4,28.4,27.1]
aware_tmt = [63.58, 81.71, 60.65, 80.2, 13.45, 70.9, 14.52, 19.9,5.8,12.4,22.4,27.1]

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


fig, axs = plt.subplots(1, 6, figsize=(10, 2.1), sharey=False)

# Redis
x0 = np.arange(2)*1.3
axs[0].bar(x0 - 1.5 * width-1.5*gap, origin_tmt[0:2], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[0].bar(x0 - 0.5 * width-0.5*gap, compression_tmt[0:2], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[0].bar(x0 + 0.5 * width+0.5*gap, deduplication_tmt[0:2], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[0].bar(x0 + 1.5 * width+1.5*gap, aware_tmt[0:2], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[0].set_xticks(x0)
axs[0].set_xticklabels(labels_grouped[0], ha='center', fontsize=8)
axs[0].set_ylabel("Total Migration Time (s)")
axs[0].grid(axis='y', linestyle='--', linewidth=0.5)
axs[0].xaxis.grid(False)
axs[0].bar(x0 - 1.5 * width-1.5*gap, origin_tmt[0:2], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[0].text(x0[1] - 1.5 * width-1.5*gap, origin_tmt[1] + 2, 330, ha='center', va='bottom', fontsize=8)
axs[0].text(x0[1] - 0.5 * width-0.5*gap, compression_tmt[1] + 2, 244, ha='center', va='bottom', fontsize=8)

# KeyDB
x1 = np.arange(2)*1.3
axs[1].bar(x1 - 1.5 * width -1.5*gap, origin_tmt[2:4], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[1].bar(x1 - 0.5 * width-0.5*gap, compression_tmt[2:4], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[1].bar(x1 + 0.5 * width+0.5*gap, deduplication_tmt[2:4], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[1].bar(x1 + 1.5 * width+1.5*gap, aware_tmt[2:4], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[1].set_xticks(x1)
axs[1].set_xticklabels(labels_grouped[1], ha='center', fontsize=8)
axs[1].grid(axis='y', linestyle='--', linewidth=0.5)
axs[1].xaxis.grid(False)
axs[1].text(x1[1] - 1.5 * width-1.5*gap, origin_tmt[3] + 2, 318, ha='center', va='bottom', fontsize=8)
axs[1].text(x1[1] - 0.5 * width-0.5*gap, compression_tmt[3] + 2, 240, ha='center', va='bottom', fontsize=8)

# Dragonfly
x2 = np.arange(2)*1.3
axs[2].bar(x2 - 1.5 * width  -1.5*gap, origin_tmt[4:6], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[2].bar(x2 - 0.5 * width-0.5*gap, compression_tmt[4:6], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[2].bar(x2 + 0.5 * width+0.5*gap, deduplication_tmt[4:6], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[2].bar(x2 + 1.5 * width +1.5*gap, aware_tmt[4:6], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[2].set_xticks(x2)
axs[2].set_xticklabels(labels_grouped[2], ha='center', fontsize=8)
axs[2].grid(axis='y', linestyle='--', linewidth=0.5)
axs[2].xaxis.grid(False)

# MySQL
x3 = np.arange(2)*1.4
axs[3].bar(x3 - 1.5 * width -1.5*gap, origin_tmt[6:8], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[3].bar(x3 - 0.5 * width-0.5*gap, compression_tmt[6:8], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[3].bar(x3 + 0.5 * width+0.5*gap, deduplication_tmt[6:8], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[3].bar(x3 + 1.5 * width +1.5*gap, aware_tmt[6:8], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[3].set_xticks(x3)
axs[3].set_xticklabels(labels_grouped[3],ha='center', fontsize=8)
axs[3].grid(axis='y', linestyle='--', linewidth=0.5)
axs[3].xaxis.grid(False)

# PostgreSQL
x4 = np.arange(2)*1.4
axs[4].bar(x4 - 1.5 * width -1.5*gap, origin_tmt[8:10], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[4].bar(x4 - 0.5 * width-0.5*gap, compression_tmt[8:10], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[4].bar(x4 + 0.5 * width+0.5*gap, deduplication_tmt[8:10], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[4].bar(x4 + 1.5 * width +1.5*gap, aware_tmt[8:10], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
axs[4].set_xticks(x4)
axs[4].set_xticklabels(labels_grouped[4],ha='center', fontsize=8)
# axs[4].tick_params(axis='x', rotation=10)
axs[4].grid(axis='y', linestyle='--', linewidth=0.5)
axs[4].xaxis.grid(False)

# ml和科学计算
x5 = np.arange(2)*1.4
axs[5].bar(x5 - 1.5 * width -1.5*gap, origin_tmt[10:12], width, color='white', edgecolor=color_ori, hatch=hatch_ori, zorder=1)
axs[5].bar(x5 - 0.5 * width-0.5*gap, compression_tmt[10:12], width, color='white', edgecolor=color_com, hatch=hatch_com, zorder=1)
axs[5].bar(x5 + 0.5 * width+0.5*gap, deduplication_tmt[10:12], width, color='white', edgecolor=color_dedup, hatch=hatch_dedup, zorder=1)
axs[5].bar(x5 + 1.5 * width +1.5*gap, aware_tmt[10:12], width, color='white', edgecolor=color_aware, hatch=hatch_aware, zorder=1)
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
plt.savefig("eva-tmt.pdf", dpi=300)
plt.show()
