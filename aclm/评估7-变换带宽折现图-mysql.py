import matplotlib.pyplot as plt
import numpy as np

# 带宽横坐标（单位 Mbps）
x = [400, 600, 800, 1000, 1200, 1400, 1600, 1800,2000,2200,2400,3200]

# 四组不同的实验数据（示例）
origin = [53, 40, 33, 30, 28.56, 25.4, 24.7,  23.8,  22.9, 22.3,21.7,20.6]
lz4 =    [24.5, 21.5, 20.6, 20.4, 19.7, 19.4, 19, 18.7, 18.3,17.9,17.6,17.2]
dedup =  [54, 42, 36,   34.5,  33.8,  31.5,  31.3,  30.8,  30.1, 29.2, 28,28.9]
aware =  [24.4,  21.5, 20.8,  20.4,  19.9,  19.6,  18.9,  18.4,  17.8,  17.5,17.3,17.1]

# 创建图形
plt.figure(figsize=(3.5, 2.5))

# 绘制四条线
plt.plot(x, origin, label='CloudHopper', marker='o')
plt.plot(x, lz4, label='MBDPC', marker='s')
plt.plot(x, dedup, label='Deduplication', marker='^')
plt.plot(x, aware, label='AwareCLM', marker='x')

# 设置横坐标显示为你想要的刻度
plt.xticks([400, 800, 1200, 1600, 2000,2400,3200])

# 添加标签、图例、标题
plt.xlabel('Network Bandwidth (Mbps)')
plt.ylabel('Total Migration Time (s)')
# plt.title('Migration Time under Different Bandwidths')
plt.legend()
plt.grid(True)

plt.savefig("varing-bandwidth-mysql.pdf", bbox_inches='tight')

# 显示图
plt.show()

