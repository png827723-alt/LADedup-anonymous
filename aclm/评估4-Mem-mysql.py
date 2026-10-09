import matplotlib.pyplot as plt
import numpy as np

# 带宽横坐标（单位 Mbps）
x = [1, 2, 3, 4, 5, 6, 7, 8,9,10,11]

# 四组不同的实验数据（示例）
origin = [18.06, 22, 24, 26.04, 27.5, 28.46, 28.75, 29.31, 30.2, 30.48, 30.5]
lz4 =    [42.75, 60, 72, 81.2, 91, 102, 109.36, 105, 107.18, 104.45,109.93]
dedup =  [27.2, 52, 68., 70, 85, 90, 94, 99, 102,105, 100.2]
aware =  [38.2, 65, 73.12, 90, 99, 110, 108, 101, 98,103, 110]

# 创建图形
plt.figure(figsize=(3.5, 2.5))

# 绘制四条线
plt.plot(x, origin, label='CloudHopper', marker='o')
plt.plot(x, lz4, label='MBDPC', marker='s')
plt.plot(x, dedup, label='Deduplication', marker='^')
plt.plot(x, aware, label='AwareCLM', marker='x')

# 设置横坐标显示为你想要的刻度
plt.xticks([1, 5, 10, 11])

# 添加标签、图例、标题
plt.xlabel("Iteration Number")
plt.ylabel("Average Memory Usage (MB)")
# plt.title('Migration Time under Different Bandwidths')
plt.legend(
    loc='lower center',              # 图例“放在上方”，需配合 y < 0 调整
    bbox_to_anchor=(0.5, 1.02),      # 横向居中，稍微高出图
    ncol=2,                          # 两列排列（可按需要设为 1、2、4）
    frameon=True                    # 不显示边框（论文常见）
)
plt.grid(True)

plt.savefig("mem-mysql.pdf", bbox_inches='tight')

# 显示图
plt.show()

