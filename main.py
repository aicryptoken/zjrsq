import sqlite3
import json
import ideapod_catering
import ideapod_space

def get_db_connection():
    conn = sqlite3.connect('db/ideapod.db')
    conn.row_factory = sqlite3.Row
    return conn

def save_analysis_results():
    with get_db_connection() as conn:
        try:
            catering_result = ideapod_catering.analyze(conn)
            if 'error' not in catering_result:
                with open('static/catering_results.json', 'w', encoding='utf-8') as f:
                    json.dump(catering_result, f, ensure_ascii=False, indent=4)
                print("餐饮分析结果保存成功")
            else:
                print(f"餐饮分析错误: {catering_result['error']}")
            
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
    save_analysis_results()
    print("分析结果已保存到 static/catering_results.json 和 static/space_results.json")