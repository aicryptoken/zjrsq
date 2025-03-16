import json
from collections import defaultdict

# 读取两个JSON文件
with open('static/space_results.json', 'r', encoding='utf-8') as f:
    space_data = json.load(f)
with open('static/catering_results.json', 'r', encoding='utf-8') as f:
    catering_data = json.load(f)

# 提取财务分析数据
space_bars = space_data["财务数据"]["财务分析_bar"]
catering_bars = catering_data["财务分析"]["财务分析_bar"]

# 使用字典按周存储销售收入数据
combined_data = defaultdict(dict)

# 处理 space 数据
for item in space_bars:
    week = item["周"]
    combined_data[week]["周"] = week
    combined_data[week]["场景收入"] = item["销售收入"]

# 处理 catering 数据并合并
for item in catering_bars:
    week = item["订单周"]
    combined_data[week]["周"] = week
    combined_data[week]["餐饮收入"] = item["销售收入"]

# 填充缺失数据为 0 并规范化输出结构
result = []
for week in combined_data:
    week_data = {
        "周": combined_data[week]["周"],
        "场景收入": combined_data[week].get("场景收入", 0.0),
        "餐饮收入": combined_data[week].get("餐饮收入", 0.0)
    }
    result.append(week_data)

# 按周排序
result.sort(key=lambda x: x["周"])

# 保存到新的JSON文件
with open('static/group_results.json', 'w', encoding='utf-8') as f:
    json.dump({"销售收入": {"销售收入_stacked": result}}, f, ensure_ascii=False, indent=2)

print("合并完成，已保存到 static/group_results.json")