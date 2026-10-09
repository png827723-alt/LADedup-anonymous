
def compute_pairwise_improvement(arr):
    """
    输入一个长度为偶数的数组，计算每对相邻值之间的提升率。
    提升率计算方式为：(后一项 - 前一项) / 前一项
    """
    if len(arr) % 2 != 0:
        raise ValueError("输入数组的长度必须是偶数。")

    improvement_rates = []
    for i in range(0, len(arr), 2):
        before = arr[i]
        after = arr[i + 1]
        rate = (after - before) / before
        improvement_rates.append(rate)
        print(f"Pair {i//2}: ({before} → {after}) 提升率 = {rate:.2%}")

    return improvement_rates


origin_tmt = [191.98, 330, 188.95, 318, 34.1, 112.2, 26, 33.79]
compression_tmt = [152.58, 244, 151.44, 240, 13.89, 84.2, 14.45, 20.9]
deduplication_tmt = [82, 92.02, 74.96, 89.2, 33.91, 71.8, 28.47, 36.3]
aware_tmt = [63.58, 81.71, 60.65, 80.2, 13.45, 70.9, 14.52, 19.9]

compute_pairwise_improvement(origin_tmt)
compute_pairwise_improvement(compression_tmt)
compute_pairwise_improvement(deduplication_tmt)
compute_pairwise_improvement(aware_tmt)