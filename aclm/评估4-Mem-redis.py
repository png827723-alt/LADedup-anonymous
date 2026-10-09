import matplotlib.pyplot as plt
import numpy as np

# 带宽横坐标（单位 Mbps）
x = [1, 2, 3, 4, 5, 6, 7, 8,9,10,11,12,13,14,15,16,17]

# 四组不同的实验数据（示例）
origin = [   21.46, 25.95, 27.39, 28.8, 30.86, 30.34, 28.95, 28.43, 29.28, 29.28,29.38, 29.23, 30.34, 30.98, 31.65, 31.65, 31.65]
lz4 =    [42.75, 68, 75, 85.33, 95.59, 104.8, 109.36, 113.88, 117.18, 120.45,124.93, 130.47, 135.5, 137.75, 137.75, 137.75, 137.92]
dedup =  [23.38, 50.2, 65.3, 70, 75, 90, 101, 94, 120,125, 126, 132.25, 144.01, 139.82, 142.43, 138, 140.56]
aware =  [38.2, 66.18, 73.12, 90, 95, 110, 102, 105, 110,113, 120, 134, 125, 140, 136.17, 145, 145.91]

# 创建图形
plt.figure(figsize=(3.5, 2.5))

# 绘制四条线
plt.plot(x, origin, label='CloudHopper', marker='o')
plt.plot(x, lz4, label='MBDPC', marker='s')
plt.plot(x, dedup, label='Deduplication', marker='^')
plt.plot(x, aware, label='AwareCLM', marker='x')

# 设置横坐标显示为你想要的刻度
plt.xticks([1, 5, 10, 15, 17])

# 添加标签、图例、标题
plt.xlabel("Iteration Number")
plt.ylabel("Average Memory Usage (MB)")
# plt.title('Migration Time under Different Bandwidths')
plt.legend(
    loc='lower center',              # 图例“放在上方”，需配合 y < 0 调整
    bbox_to_anchor=(0.5, 1.02),      # 横向居中，稍微高出图
    ncol=2,                          # 两列排列（可按需要设为 1、2、4）
    frameon=True                    
)
plt.grid(True)

plt.savefig("mem-redis.pdf", bbox_inches='tight')

# 显示图
plt.show()

