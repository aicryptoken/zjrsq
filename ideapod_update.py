import pandas as pd
import sqlite3
import os
from datetime import datetime

def preprocess_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Unified datetime preprocessing for all tables"""
    datetime_columns = [
        '创建时间', '预定开始时间', '预定结束时间', '实际结束时间',  # Space
        '下单时间',  # Catering
        '加入时间', '会员注册完成时间', '首次消费时间', '最后消费时间'  # Member
    ]
    for col in datetime_columns:
        if col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def clean_catering_data(catering_df):
    """Clean and preprocess catering data"""
    catering_df.drop(columns=[
        "号牌", "FLIPOS版本", "状态", "代金券", "下单门店", "门店编号", "门店区域", 
        "入账门店", "ERP流水号", "第三方外卖平台单号", "配送平台", 
        "配送平台订单编号", "包装费", "配送费", "积分", "收银备注"
    ], inplace=True, errors='ignore')
    catering_df["会员号"] = catering_df["会员号"].astype(str)
    catering_df = preprocess_datetime(catering_df)
    catering_df.set_index("会员号", inplace=True)
    return catering_df

def clean_space_data(space_df):
    """Clean and preprocess space data"""
    space_df.drop(columns=["用户昵称"], inplace=True, errors='ignore')
    space_df["订单备注"] = space_df["订单备注"].str.replace("\n", ",", regex=False)
    space_df["会员号"] = space_df["会员号"].astype(str)
    if "手机号" in space_df.columns:
        space_df["手机号"] = space_df["手机号"].astype(str)
    if "实际结束时间" in space_df.columns:
        space_df['实际结束时间'] = space_df['实际结束时间'].fillna('NA')
    space_df = preprocess_datetime(space_df)
    space_df.set_index("会员号", inplace=True)
    return space_df

def clean_member_data(member_df):
    """Clean and preprocess member data"""
    member_df.drop(columns=[
        "UnionID", "OpenID", "昵称", "标签", 
        "首次消费门店", "最后消费门店"
    ], inplace=True, errors='ignore')
    member_df["会员号"] = member_df["会员号"].astype(str)
    if "手机号" in member_df.columns:
        member_df["手机号"] = member_df["手机号"].astype(str)
    member_df = preprocess_datetime(member_df)
    member_df.set_index("会员号", inplace=True)
    return member_df

def update_catering_table(conn, new_file):
    """Update catering table with new data"""
    new_df = pd.read_csv(new_file)
    new_df = clean_catering_data(new_df)
    
    min_time = new_df['下单时间'].min()
    
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM Catering 
        WHERE 下单时间 >= ?
    """, (min_time,))
    
    new_df.to_sql("Catering", conn, if_exists="append", index=True)
    print(f"Catering table updated with data from {new_file}")

def update_space_table(conn, new_file):
    """Update space table with new data"""
    new_df = pd.read_csv(new_file)
    new_df = clean_space_data(new_df)
    
    min_time = new_df['创建时间'].min()
    
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM Space 
        WHERE 创建时间 >= ?
    """, (min_time,))
    
    new_df.to_sql("Space", conn, if_exists="append", index=True)
    print(f"Space table updated with data from {new_file}")

def preprocess_existing_tables(database_path):
    """Preprocess datetime columns in existing database tables"""
    conn = sqlite3.connect(database_path)
    try:
        # 处理 Catering 表
        catering_df = pd.read_sql("SELECT * FROM Catering", conn, index_col="会员号")
        catering_df = preprocess_datetime(catering_df)
        catering_df.to_sql("Catering", conn, if_exists="replace", index=True)
        
        # 处理 Space 表
        space_df = pd.read_sql("SELECT * FROM Space", conn, index_col="会员号")
        space_df = preprocess_datetime(space_df)
        space_df.to_sql("Space", conn, if_exists="replace", index=True)
        
        # 处理 Member 表
        member_df = pd.read_sql("SELECT * FROM Member", conn, index_col="会员号")
        member_df = preprocess_datetime(member_df)
        member_df.to_sql("Member", conn, if_exists="replace", index=True)
        
        conn.commit()
        print("Existing tables preprocessed successfully")
    except sqlite3.OperationalError as e:
        print(f"Skipping preprocessing of existing tables: {e}")
    finally:
        conn.close()

def load_static_tables(database_path, member_file, product_file):
    """Load membership and product tables (static data)"""
    conn = sqlite3.connect(database_path)
    try:
        member_df = pd.read_csv(member_file)
        member_df = clean_member_data(member_df)
        
        product_df = pd.read_csv(product_file)
        product_df.columns = ['商品名','数量统计','基础产品', '营销系列', '口味', '套餐']
        product_df['商品名'] = product_df['商品名'].str.strip()
        product_df['基础产品'] = product_df['基础产品'].str.strip()
        product_df['数量统计'] = pd.to_numeric(product_df['数量统计'], errors='coerce')
        product_df[['营销系列', '口味', '套餐']] = product_df[['营销系列', '口味', '套餐']].fillna('')
        product_df.set_index("商品名", inplace=True)
        
        member_df.to_sql("Member", conn, if_exists="replace", index=True)
        product_df.to_sql("Product", conn, if_exists="replace", index=True)
        print("Static tables (Member and Product) loaded successfully")
    finally:
        conn.close()

def update_database():
    """Main function to update database"""
    database_path = "db/ideapod.db"
    member_file = "db/raw_membership.csv"
    product_file = "db/ideapod_product.csv"
    new_catering_file = "db/new_flipos.csv"
    new_space_file = "db/new_space.csv"
    
    # 加载静态表并处理历史数据
    load_static_tables(database_path, member_file, product_file)
    preprocess_existing_tables(database_path)
    
    # 更新动态表
    conn = sqlite3.connect(database_path)
    try:
        if os.path.exists(new_catering_file):
            update_catering_table(conn, new_file=new_catering_file)
        else:
            print("No new_flipos.csv found, skipping catering update")
            
        if os.path.exists(new_space_file):
            update_space_table(conn, new_file=new_space_file)
        else:
            print("No new_space.csv found, skipping space update")
            
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    update_database()