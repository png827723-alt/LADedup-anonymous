import matplotlib.pyplot as plt
import numpy as np

# 带宽横坐标（单位 Mbps）
x = [1, 2, 3, 4, 5, 6, 7, 8,9,10,11,12,13,14,15,16,17]

# 四组不同的实验数据（示例）
origin = [0.38,1.11,2.03,3.27,4.83,6.64,8.76,10.91,12.99,14.92,16.06,16.48,16.66,16.75,16.81,16.84,16.86]
lz4 =    [7.62,23.14,38,50,62,71,79,84,89,91,91,92,90,91,90,89,89.58]
dedup =  [1.77,4.93,22.49,32,48,50,64,65,70,80,78,79,80,75,79,80,81]
aware =  [6.5,23,39,48,58,60,73,75,81,90,88,87,89,90,91,91,88]

# 创建图形
plt.figure(figsize=(3.5, 2.5))

# methods = [
#     ("CloudHopper", 'x:'),
#     ("MBDPC", 's--'),        # 方块 + 虚线
#     ("Deduplication", 'o-'),       # 圆点 + 实线
#     ("AwareCLM", '^-.'), # x点 + 点划线
# ]

# # 自定义标题，标明线型与方法名对应关系
# title_lines = [
#     "Line Styles:",
#     f"{methods[0][1]}: {methods[0][0]}",
#     f"{methods[1][1]}: {methods[1][0]}",
#     f"{methods[2][1]}: {methods[2][0]}",
#     f"{methods[3][1]}: {methods[3][0]}"
# ]
# plt.title("\n".join(title_lines), fontsize=10)

# 绘制四条线
plt.plot(x, origin, label='CloudHopper', marker='o')
plt.plot(x, lz4, label='MBDPC', marker='s')
plt.plot(x, dedup, label='Deduplication', marker='^')
plt.plot(x, aware, label='AwareCLM', marker='x')

# 设置横坐标显示为你想要的刻度
plt.xticks([1, 5, 10, 15, 17])

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

plt.savefig("cpu-redis.pdf", bbox_inches='tight')

# 显示图
plt.show()

