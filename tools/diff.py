import pandas as pd
import sys
import os

def compare_csvs(old_file, new_file, type_name, filter_col=None, compare_col=None):
    if not os.path.exists(new_file):
        print(f"{new_file} 不存在，跳过 {type_name} 对比")
        return
    
    try:
        df_old = pd.read_csv(old_file, low_memory=False, parse_dates=[filter_col] if filter_col else None, 
                            date_format='%m/%d/%y')
        df_new = pd.read_csv(new_file, low_memory=False, parse_dates=[filter_col] if filter_col else None, 
                            date_format='%m/%d/%y')
    except Exception as e:
        print(f"读取 {type_name} 文件失败: {e}")
        return

    old_columns = set(df_old.columns)
    new_columns = set(df_new.columns)
    added_columns = new_columns - old_columns
    deleted_columns = old_columns - new_columns
    
    if added_columns:
        print(f"{type_name} 列新增: {', '.join(added_columns)}")
    if deleted_columns:
        print(f"{type_name} 列减少: {', '.join(deleted_columns)}")
    if not added_columns and not deleted_columns:
        print(f"{type_name} 列无变化")

    if filter_col:
        if filter_col not in df_old.columns or filter_col not in df_new.columns:
            print(f"错误: {type_name} 文件中缺少 '{filter_col}' 列")
            return
        
        old_values = set(df_old[filter_col].dropna().astype(str))
        new_values = set(df_new[filter_col].dropna().astype(str))
        common_values = old_values & new_values
        
        print(f"调试信息 - {type_name} {filter_col}:")
        print(f"旧文件唯一值数量: {len(old_values)}, 示例: {list(old_values)[:5]}")
        print(f"新文件唯一值数量: {len(new_values)}, 示例: {list(new_values)[:5]}")
        print(f"重合值数量: {len(common_values)}, 示例: {list(common_values)[:5]}")
        
        if not common_values:
            print(f"{type_name} 无重合的 {filter_col}")
            return
        
        df_old = df_old[df_old[filter_col].astype(str).isin(common_values)]
        df_new = df_new[df_new[filter_col].astype(str).isin(common_values)]

    if compare_col not in df_old.columns or compare_col not in df_new.columns:
        print(f"错误: {type_name} 文件中缺少 '{compare_col}' 列")
        return

    old_items = set(df_old[compare_col].dropna().astype(str))
    new_items = set(df_new[compare_col].dropna().astype(str))

    added_items = new_items - old_items
    deleted_items = old_items - new_items
    
    item_name = "会员" if compare_col == "会员号" else "订单"
    if added_items:
        print(f"{type_name} 新增 {len(added_items)} 个{item_name}")
        print(f"新增{item_name}示例: {list(added_items)[:5]}")  # 输出新增订单号
    else:
        print(f"{type_name} 无新增{item_name}")
    if deleted_items:
        print(f"{type_name} 减少 {len(deleted_items)} 个{item_name}")
        print(f"减少{item_name}示例: {list(deleted_items)[:5]}")  # 输出减少订单号
    else:
        print(f"{type_name} 无减少{item_name}")
    if not added_items and not deleted_items:
        print(f"{type_name} {compare_col} 完全匹配，无变化")

def main():
    db_path = "../db/"
    compare_csvs(db_path + "raw_membership.csv", db_path + "new_membership.csv", 
                "membership", compare_col="会员号")
    print("\n" + "-"*50 + "\n")
    compare_csvs(db_path + "raw_flipos.csv", db_path + "new_flipos.csv", 
                "flipos", filter_col="对账日期", compare_col="订单号")
    print("\n" + "-"*50 + "\n")
    compare_csvs(db_path + "raw_space.csv", db_path + "new_space.csv", 
                "space", filter_col="预定日期", compare_col="订单编号")

if __name__ == "__main__":
    main()