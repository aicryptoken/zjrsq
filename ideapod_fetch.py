import pandas as pd
import sqlite3

def format_dates_in_space_file(space_df):
    """
    Format the date columns in the space DataFrame.
    Handles both MM/DD/YY and YYYY-MM-DD formats.
    """
    def convert_date(date_series):
        try:
            return pd.to_datetime(date_series, format="%m/%d/%y %H:%M").dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            return pd.to_datetime(date_series).dt.strftime("%Y-%m-%d %H:%M")

    date_columns = ["预定开始时间", "预定结束时间", "实际结束时间", "创建时间"]
    for col in date_columns:
        if col in space_df.columns:
            space_df[col] = convert_date(space_df[col])

    return space_df

def load_and_prepare_data(catering_file, space_file, member_file, product_file, database_path):
    """
    Load CSV files, clean and prepare data, then save to SQLite database
    """
    # 读取 CSV 文件
    catering_df = pd.read_csv(catering_file)
    space_df = pd.read_csv(space_file)
    member_df = pd.read_csv(member_file)
    product_df = pd.read_csv(product_file)

    # 删除指定列
    catering_df.drop(columns=[
        "号牌", "FLIPOS版本", "状态", "代金券", "下单门店", "门店编号", "门店区域", 
        "入账门店", "ERP流水号", "第三方外卖平台单号", "配送平台", 
        "配送平台订单编号", "包装费", "配送费", "积分", "收银备注"
    ], inplace=True)
    
    space_df.drop(columns=["用户昵称"], inplace=True)
    
    member_df.drop(columns=[
        "UnionID", "OpenID", "昵称", "标签", 
        "首次消费门店", "最后消费门店"
    ], inplace=True)

    product_df.columns = ['商品名','数量统计','基础产品', '营销系列', '口味', '套餐']
    product_df['商品名'] = product_df['商品名'].str.strip()
    product_df['基础产品'] = product_df['基础产品'].str.strip()
    product_df['数量统计'] = pd.to_numeric(product_df['数量统计'], errors='coerce')
    product_df[['营销系列', '口味', '套餐']] = product_df[['营销系列', '口味', '套餐']].fillna('')

    # 格式化日期列
    space_df = format_dates_in_space_file(space_df)
    space_df["订单备注"] = space_df["订单备注"].str.replace("\n", ",", regex=False)

    # 类型转换
    catering_df["会员号"] = catering_df["会员号"].astype(str)
    space_df["会员号"] = space_df["会员号"].astype(str)
    member_df["会员号"] = member_df["会员号"].astype(str)

    if "手机号" in member_df.columns:
        member_df["手机号"] = member_df["手机号"].astype(str)

    if "手机号" in space_df.columns:
        space_df["手机号"] = space_df["手机号"].astype(str)

    if "实际结束时间" in space_df.columns:
        space_df['实际结束时间'] = space_df['实际结束时间'].fillna('NA')
        
    # 设置主键
    catering_df.set_index("会员号", inplace=True)
    space_df.set_index("会员号", inplace=True)
    member_df.set_index("会员号", inplace=True)
    product_df.set_index("商品名", inplace=True) 


    # 保存到 SQLite 数据库
    conn = sqlite3.connect(database_path)
    try:
        catering_df.to_sql("Catering", conn, if_exists="replace", index=True)
        space_df.to_sql("Space", conn, if_exists="replace", index=True)
        member_df.to_sql("Member", conn, if_exists="replace", index=True)
        product_df.to_sql("Product", conn, if_exists="replace", index=True)
        print("数据已成功导入到 SQLite 数据库！")
    finally:
        conn.close()

def clean_database_tables(db_path):
    """
    Clean and standardize database tables
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        # 修改 Catering 表的字段名
        cursor.execute("ALTER TABLE Catering RENAME COLUMN '入账时间（原下单时间）' TO 下单时间")
        conn.commit()
    finally:
        conn.close()

def main():
    # 文件路径
    catering_file = "Raw_flipos_orders.csv"
    space_file = "Raw_space_orders.csv"
    member_file = "Raw_membership.csv"
    product_file = "db/ideapod_product.csv"
    database_path = "ideapod.db"

    # 加载和准备数据
    load_and_prepare_data(catering_file, space_file, member_file, product_file, database_path)

    # 清理数据库表
    clean_database_tables(database_path)

if __name__ == "__main__":
    main()
