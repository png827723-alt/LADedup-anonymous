import matplotlib.pyplot as plt
import numpy as np

# 带宽横坐标（单位 Mbps）
x = [1, 2, 3, 4, 5, 6, 7, 8,9,10,11]

# 四组不同的实验数据（示例）
origin = [0.5,2,2.03,3,4,6,8,10,12,14,16]
lz4 =    [7,15,21,29,38,48,45,47,42,43,48]
dedup =  [1.77,4.93,22.49,32,37,35,34,40,42,39,40]
aware =  [6.5,23,28,42,45,45,50,38,40,48,50]

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
plt.ylabel("Average CPU Usage (%)")
# plt.title('Migration Time under Different Bandwidths')
plt.legend(
    loc='lower center',              # 图例“放在上方”，需配合 y < 0 调整
    bbox_to_anchor=(0.5, 1.02),      # 横向居中，稍微高出图
    ncol=2,                          # 两列排列（可按需要设为 1、2、4）
    frameon=True                    # 不显示边框（论文常见）
)
plt.grid(True)

plt.savefig("cpu-mysql.pdf", bbox_inches='tight')

# 显示图
plt.show()

