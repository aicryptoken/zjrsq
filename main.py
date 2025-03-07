import sqlite3
import json
import ideapod_catering
import ideapod_space

def get_db_connection():
    conn = sqlite3.connect('db/ideapod.db')
    conn.row_factory = sqlite3.Row
    return conn

def save_analysis_results():
    # 获取数据库连接
    with get_db_connection() as conn:
        # 计算餐饮分析结果
        catering_result = ideapod_catering.analyze(conn)
        if 'error' not in catering_result:
            with open('static/catering_results.json', 'w', encoding='utf-8') as f:
                json.dump(catering_result, f, ensure_ascii=False, indent=4)
        
        # 计算空间分析结果
        space_result = ideapod_space.analyze(conn)
        if 'error' not in space_result:
            with open('static/space_results.json', 'w', encoding='utf-8') as f:
                json.dump(space_result, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    save_analysis_results()
    print("分析结果已保存到 static/catering_results.json 和 static/space_results.json")