import sqlite3
import json
import ideapod_catering
import ideapod_space
import ideapod_group

def get_db_connection():
    conn = sqlite3.connect('db/ideapod.db')
    conn.row_factory = sqlite3.Row
    return conn

def save_analysis_results(choices=None):
    # 如果没有指定选择，默认执行所有分析
    if choices is None:
        choices = {1, 2, 3}

    with get_db_connection() as conn:
        try:
            if 3 in choices:
                group_result = ideapod_group.analyze(conn)
                if 'error' not in group_result:
                    with open('static/group_results.json', 'w', encoding='utf-8') as f:
                        json.dump(group_result, f, ensure_ascii=False, indent=4)
                    print("集团分析结果保存成功")
                else:
                    print(f"集团分析错误: {group_result['error']}")

            if 2 in choices:
                catering_result = ideapod_catering.analyze(conn)
                if 'error' not in catering_result:
                    with open('static/catering_results.json', 'w', encoding='utf-8') as f:
                        json.dump(catering_result, f, ensure_ascii=False, indent=4)
                    print("餐饮分析结果保存成功")
                else:
                    print(f"餐饮分析错误: {catering_result['error']}")
            
            if 1 in choices:
                space_result = ideapod_space.analyze(conn)
                if 'error' not in space_result:
                    with open('static/space_results.json', 'w', encoding='utf-8') as f:
                        json.dump(space_result, f, ensure_ascii=False, indent=4)
                    print("空间分析结果保存成功")
                else:
                    print(f"空间分析错误: {space_result['error']}")
        except Exception as e:
            print(f"保存分析结果时出错: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    print("请选择要需要的分析 (用逗号分隔):")
    print("1 - 空间分析")
    print("2 - 餐饮分析")
    print("3 - 集团分析")
    user_input = input("请输入选择 (例如: 1,2,3)，默认执行全部: ").strip()
    
    if user_input:
        try:
            choices = set(int(x.strip()) for x in user_input.split(',') if x.strip())
            # 验证输入是否有效
            if not choices.issubset({1, 2, 3}):
                print("错误：请输入有效的选项 (1,2,3)")
            elif not choices:
                print("错误：输入为空，请重新输入")
            else:
                save_analysis_results(choices)
        except ValueError:
            print("错误：请输入有效的数字，用逗号分隔 (例如: 1,2,3)")
    else:
        save_analysis_results()  # 留空时执行全部
        
    print("分析完成，结果已保存到对应的json文件中")