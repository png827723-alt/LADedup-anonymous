import matplotlib.pyplot as plt
import numpy as np
plt.rcParams.update({
    "font.size": 8,         # 小字体，适合论文
    "axes.titlesize": 8,
    "axes.labelsize": 8,
    "xtick.labelsize": 7,
    "ytick.labelsize": 7,
    "legend.fontsize": 7,
})

# 假设有 20 个任务
labels = ['Fast','Level1','Level2','Level3','Level4','Level5']
lz4 = [0.48,0.49,0.937,1.10,1.55,2.49]

x = np.arange(len(labels))

fig, ax = plt.subplots(figsize=(3.5, 2.5))

# 两条折线
ax.plot(x, lz4, marker='s', label='LZ4 Compression', color='black', linewidth=2)



# xtick_indices = sorted(set([0] + list(range(4, len(labels), 5))))
ax.set_xticks(x)
ax.set_xticklabels(labels)
# ax.set_xticklabels([labels[i] for i in xtick_indices], rotation=45)
ax.set_xlabel('Compression Level')

# 其他配置
ax.set_ylabel('Reduction Adaptation Index')
ax.yaxis.grid(True, linestyle='--', linewidth=0.5)
ax.xaxis.grid(False)  # 确保不显示竖线
plt.legend(
    loc='lower center',              # 图例“放在上方”，需配合 y < 0 调整
    bbox_to_anchor=(0.5, 1.02),      # 横向居中，稍微高出图
    ncol=2,                          # 两列排列（可按需要设为 1、2、4）
    frameon=True                    # 不显示边框（论文常见）
)
plt.tight_layout()
plt.savefig('eva-lz4.pdf', dpi=300)
plt.show()
