import pandas as pd
import sys

# python3 diff.py ../db/Raw_membership.csv ../db/new_membership.csv


def compare_csvs(old_file, new_file):
    # 读取两个 CSV 文件
    try:
        df_old = pd.read_csv(old_file)
        df_new = pd.read_csv(new_file)
    except Exception as e:
        print(f"读取文件失败: {e}")
        return

    # 1. 检查列的增减和变化
    old_columns = set(df_old.columns)
    new_columns = set(df_new.columns)

    # 列新增
    added_columns = new_columns - old_columns
    if added_columns:
        print(f"列新增: {', '.join(added_columns)}")

    # 列减少
    deleted_columns = old_columns - new_columns
    if deleted_columns:
        print(f"列减少: {', '.join(deleted_columns)}")

    # 检查列是否一致
    if old_columns == new_columns:
        print("列无变化")
    else:
        print("列发生变化，请检查上述增减情况")

    # 2. 只检查"会员号"列的差异
    if '会员号' not in df_old.columns or '会员号' not in df_new.columns:
        print("错误: 一个或两个文件中缺少 '会员号' 列")
        return

    # 提取会员号并转换为集合
    old_members = set(df_old['会员号'].dropna().astype(str))  # 转换为字符串，避免类型问题
    new_members = set(df_new['会员号'].dropna().astype(str))

    # 新增会员（在新文件中存在，但在旧文件中不存在）
    added_members = new_members - old_members
    if added_members:
        print(f"新增 {len(added_members)} 个会员")
    else:
        print("无新增会员")

    # 减少会员（在旧文件中存在，但在新文件中不存在）
    deleted_members = old_members - new_members
    if deleted_members:
        print(f"减少 {len(deleted_members)} 个会员")
    else:
        print("无减少会员")

    # 检查是否完全匹配（1对1匹配）
    if len(old_members) == len(new_members) and old_members == new_members:
        print("会员号完全匹配，无变化")

def main():
    # 检查命令行参数
    if len(sys.argv) != 3:
        print("用法: python diff.py old.csv new.csv")
        sys.exit(1)

    old_file = sys.argv[1]
    new_file = sys.argv[2]

    # 执行对比
    compare_csvs(old_file, new_file)

if __name__ == "__main__":
    main()