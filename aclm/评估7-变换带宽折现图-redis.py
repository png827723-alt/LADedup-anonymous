import matplotlib.pyplot as plt
import numpy as np

# 带宽横坐标（单位 Mbps）
x = [400, 600, 800, 1000, 1200, 1400, 1600, 1800,2000,2200,2400]

# 四组不同的实验数据（示例）
origin = [402, 295, 195, 162, 131, 111, 99,  73,  60,  44,32]
lz4 =    [203, 150, 142, 135, 130, 125, 122, 115, 111,108,106]
dedup =  [118, 108, 91,   85,  80,  75,  73,  72,  71, 70, 71]
aware =  [95,   89, 81,   75,  72,  72,  70,  69,  69,  50,36]

# 创建图形
plt.figure(figsize=(3.5, 2.5))

# 绘制四条线
plt.plot(x, origin, label='CloudHopper', marker='o')
plt.plot(x, lz4, label='MBDPC', marker='s')
plt.plot(x, dedup, label='Deduplication', marker='^')
plt.plot(x, aware, label='AwareCLM', marker='x')

# 设置横坐标显示为你想要的刻度
plt.xticks([400, 800, 1200, 1600, 2000,2400])

# 添加标签、图例、标题
plt.xlabel('Network Bandwidth (Mbps)')
plt.ylabel('Total Migration Time (s)')
# plt.title('Migration Time under Different Bandwidths')
plt.legend(
    # loc='lower center',              # 图例“放在上方”，需配合 y < 0 调整
    # bbox_to_anchor=(0.5, 1.02),      # 横向居中，稍微高出图
    # ncol=2,                          # 两列排列（可按需要设为 1、2、4）
    # frameon=True                    # 不显示边框（论文常见）
)
plt.grid(True)

plt.savefig("varing-bandwidth-redis.pdf", bbox_inches='tight')

# 显示图
plt.show()

