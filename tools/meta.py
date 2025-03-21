import sqlite3
import json

def is_numeric_type(data_type):
    """
    判断字段数据类型是否为数值型。
    SQLite 的类型系统比较宽松，常见数值型包括：INTEGER、REAL、NUMERIC、FLOAT、DOUBLE
    """
    data_type = data_type.upper()
    return any(t in data_type for t in ["INT", "REAL", "NUMERIC", "FLOAT", "DOUBLE"])

def get_table_metadata(cursor, table_name):
    """
    获取指定表的详细元数据信息，包括表结构、外键、索引、行数、数值型统计和样本记录
    """
    metadata = {}
    metadata["table_name"] = table_name

    # 1. 获取表结构信息
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns_info = cursor.fetchall()
    columns = []
    for col in columns_info:
        col_dict = {
            "cid": col[0],
            "name": col[1],
            "type": col[2],
            "notnull": bool(col[3]),
            "default_value": col[4],
            "primary_key": bool(col[5])
        }
        columns.append(col_dict)
    metadata["columns"] = columns

    # 2. 获取外键信息
    cursor.execute(f"PRAGMA foreign_key_list({table_name});")
    fk_info = cursor.fetchall()
    foreign_keys = []
    for fk in fk_info:
        # fk 中各项依次为：id, seq, table, from, to, on_update, on_delete, match
        fk_dict = {
            "id": fk[0],
            "seq": fk[1],
            "table": fk[2],
            "from": fk[3],
            "to": fk[4],
            "on_update": fk[5],
            "on_delete": fk[6],
            "match": fk[7]
        }
        foreign_keys.append(fk_dict)
    metadata["foreign_keys"] = foreign_keys

    # 3. 获取索引信息
    cursor.execute(f"PRAGMA index_list({table_name});")
    index_info = cursor.fetchall()
    indexes = []
    for idx in index_info:
        # idx 中各项依次为：seq, name, unique, origin, partial
        idx_dict = {
            "seq": idx[0],
            "name": idx[1],
            "unique": bool(idx[2]),
            "origin": idx[3],
            "partial": bool(idx[4]) if len(idx) > 4 else None
        }
        indexes.append(idx_dict)
    metadata["indexes"] = indexes

    # 4. 获取表记录数
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
    except Exception as e:
        row_count = None
    metadata["row_count"] = row_count

    # 5. 获取数值型列的基本统计信息（最小值、最大值、平均值）
    stats = {}
    for col in columns:
        col_name = col["name"]
        col_type = col["type"]
        if is_numeric_type(col_type):
            try:
                cursor.execute(f"SELECT MIN({col_name}), MAX({col_name}), AVG({col_name}) FROM {table_name};")
                min_val, max_val, avg_val = cursor.fetchone()
                stats[col_name] = {
                    "min": min_val,
                    "max": max_val,
                    "avg": avg_val
                }
            except Exception as e:
                stats[col_name] = {
                    "min": None,
                    "max": None,
                    "avg": None,
                    "error": str(e)
                }
    metadata["numeric_stats"] = stats

    # 6. 获取样本记录（取前 10 条，并以字典形式展示列名和值）
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
        sample_records = cursor.fetchall()
        sample_data = []
        column_names = [col["name"] for col in columns]
        for record in sample_records:
            record_dict = dict(zip(column_names, record))
            sample_data.append(record_dict)
        metadata["sample_records"] = sample_data
    except Exception as e:
        metadata["sample_records"] = []

    return metadata

def generate_metadata_report(db_file, output_json="../db/metadata_report.json"):
    """
    连接到数据库，遍历所有表，生成详细的元数据报告，并将结果保存为 JSON 文件
    """
    report = {}
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # 获取所有表名
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        report["tables"] = table_names

        # 对每个表生成详细元数据
        report["tables_metadata"] = []
        for table_name in table_names:
            table_meta = get_table_metadata(cursor, table_name)
            report["tables_metadata"].append(table_meta)

        conn.close()
    except sqlite3.Error as e:
        report["error"] = str(e)

    # 将元数据报告写入 JSON 文件（便于后续 AI 读取和处理）
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=4)

    print(f"元数据报告已生成并保存至 {output_json}")

if __name__ == "__main__":
    db_file = "../db/ideapod.db"
    generate_metadata_report(db_file)
