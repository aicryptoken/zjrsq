import sqlite3
from flask import Flask, render_template
import json
import meta  # 保留，因为 /meta 路由仍然需要

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('db/ideapod.db')
    conn.row_factory = sqlite3.Row
    return conn

# 首页
@app.route('/')
def home():
    return render_template('index.html')

# 餐饮分析路由 - 从 JSON 文件读取
@app.route('/catering')
def catering():
    try:
        with open('static/catering_results.json', 'r', encoding='utf-8') as f:
            results = json.load(f)
        if 'error' in results:
            return render_template('error.html', error=results['error'])
        return render_template('catering.html', results=results)
    except FileNotFoundError:
        return render_template('error.html', error="餐饮分析数据文件未找到，请先运行预计算脚本")

# 空间分析路由 - 从 JSON 文件读取
@app.route('/space')
def space():
    try:
        with open('static/space_results.json', 'r', encoding='utf-8') as f:
            results = json.load(f)
        if 'error' in results:
            return render_template('space.html', error=results['error'])
        return render_template('space.html', results=results)
    except FileNotFoundError:
        return render_template('error.html', error="空间分析数据文件未找到，请先运行预计算脚本")

# 元数据分析路由
@app.route('/meta')
def meta():
    conn = get_db_connection()
    meta.analyze(conn)
    conn.close()
    with open('metadata_report.json', 'r') as f:
        result = json.load(f)
    return render_template('meta.html', result=result)

if __name__ == '__main__':
    app.run(debug=True)