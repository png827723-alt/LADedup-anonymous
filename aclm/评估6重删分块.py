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
labels = ['4K','8K','16K','32K','64K']
cdc = [0.62,0.58,0.65,0.67,0.74]
fix =[1.36,1.38,1.52,1.33,1.27]

x = np.arange(len(labels))

fig, ax = plt.subplots(figsize=(3, 2))

# 两条折线
ax.plot(x, cdc, marker='s', label='FastCDC', color='black', linewidth=2)
ax.plot(x, fix, marker='^', label='FSC', color='grey', linewidth=2)



# xtick_indices = sorted(set([0] + list(range(4, len(labels), 5))))
ax.set_xticks(x)
ax.set_xticklabels(labels)
# ax.set_xticklabels([labels[i] for i in xtick_indices], rotation=45)
ax.set_xlabel('Chunk Size (B)')

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
plt.savefig('eva-cdc.pdf', dpi=300)
plt.show()
