import json
from collections import defaultdict
from datetime import datetime

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

# 转换为列表并按周排序
result = []
for week in combined_data:
    week_data = {
        "周": combined_data[week]["周"],
        "场景收入": combined_data[week].get("场景收入", 0.0),
        "餐饮收入": combined_data[week].get("餐饮收入", 0.0)
    }
    result.append(week_data)
result.sort(key=lambda x: x["周"])

# 定义截止日期
cutoff_date = datetime.strptime("2024-04-30", "%Y-%m-%d")
cutoff2_date = datetime.strptime("2023-11-06", "%Y-%m-%d")

# 计算wow和mom，创建单独的trailing_4_week数据结构
trailing_data = []
for i in range(len(result)):
    # 计算trailing 4 week收入
    space_trailing_4 = sum(d["场景收入"] for d in result[max(0, i-3):i+1])
    catering_trailing_4 = sum(d["餐饮收入"] for d in result[max(0, i-3):i+1])
    
    week_trailing = {
        "周": result[i]["周"],
        "场景_trailing_4_week收入": space_trailing_4,
        "餐饮_trailing_4_week收入": catering_trailing_4
    }
    
    # 将周转换为日期以进行比较
    week_date = datetime.strptime(result[i]["周"], "%Y-%m-%d")
    
    # 计算wow（与上周相比）
    if i > 0:
        prev_space = trailing_data[i-1]["场景_trailing_4_week收入"]
        prev_catering = trailing_data[i-1]["餐饮_trailing_4_week收入"]
        
        space_wow = (space_trailing_4 / prev_space - 1) * 100 if prev_space != 0 else 0
        week_trailing["场景_wow"] = 0 if week_date <= cutoff_date else space_wow
        catering_wow = (catering_trailing_4 / prev_catering - 1) * 100 if prev_catering != 0 else 0
        week_trailing["餐饮_wow"] = 0 if week_date <= cutoff2_date else catering_wow
    else:
        week_trailing["场景_wow"] = 0
        week_trailing["餐饮_wow"] = 0
    
    # 计算mom（与4周前相比）
    if i >= 4:
        prev_4_space = trailing_data[i-4]["场景_trailing_4_week收入"]
        prev_4_catering = trailing_data[i-4]["餐饮_trailing_4_week收入"]
        
        space_mom = (space_trailing_4 / prev_4_space - 1) * 100 if prev_4_space != 0 else 0
        week_trailing["场景_mom"] = 0 if week_date <= cutoff_date else space_mom
        catering_mom = (catering_trailing_4 / prev_4_catering - 1) * 100 if prev_4_catering != 0 else 0
        week_trailing["餐饮_mom"] = 0 if week_date <= cutoff2_date else catering_mom
    else:
        week_trailing["场景_mom"] = 0
        week_trailing["餐饮_mom"] = 0
    
    trailing_data.append(week_trailing)

# 创建wow_result和mom_result
wow_result = [{
    "周": week["周"],
    "场景周环比": week["场景_wow"],
    "餐饮周环比": week["餐饮_wow"]
} for week in trailing_data]

mom_result = [{
    "周": week["周"],
    "场景月环比": week["场景_mom"],
    "餐饮月环比": week["餐饮_mom"]
} for week in trailing_data]

# 构建输出结构
output_data = {
    "销售收入_stacked": result,
    "过去四周收入周环比(%)_line": wow_result,
    "过去四周收入月环比(%)_line": mom_result
}

# 保存到新的JSON文件
with open('static/group_results.json', 'w', encoding='utf-8') as f:
    json.dump({'集团财务':output_data}, f, ensure_ascii=False, indent=2)

# 打印结果供参考
print("合并和计算完成，已保存到 static/group_results.json")
print("\n周环比数据示例:", wow_result[-1] if wow_result else "无数据")
print("月环比数据示例:", mom_result[-1] if mom_result else "无数据")