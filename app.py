import sqlite3
from flask import Flask, render_template
import json
import ideapod_catering  # 导入餐饮分析脚本
import ideapod_space     # 导入空间分析脚本
import ideapod_fetch
import meta              # 导入元数据分析脚本

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('db/ideapod.db')
    conn.row_factory = sqlite3.Row
    return conn

# 首页
@app.route('/')
def home():
    return render_template('index.html')

# 餐饮分析路由
@app.route('/catering')
def catering():
    with get_db_connection() as conn:
        result = ideapod_catering.analyze(conn)
    if 'error' in result:
        return render_template('error.html', error=result['error'])
    return render_template('catering.html', result=result)

# 空间分析路由
@app.route('/space')
def space():
    with get_db_connection() as conn:
        result = ideapod_space.analyze(conn)
    if 'error' in result:
        return render_template('error.html', error=result['error'])
    return render_template('space.html', result=result)

# 元数据分析路由
@app.route('/meta')
def meta():
    conn = get_db_connection()
    meta.analyze(conn)  # 假设它生成 metadata_report.json
    conn.close()
    with open('metadata_report.json', 'r') as f:
        result = json.load(f)
    return render_template('meta.html', result=result)

# 目前暂时不需要，先放在这里
# 更新数据路由
@app.route('/update')
def update():
    conn = get_db_connection()
    ideapod_fetch.clean_and_update(conn)  # 假设脚本更新数据库
    conn.close()
    return "数据已更新！<a href='/'>返回首页</a>"


if __name__ == '__main__':
    app.run(debug=True)