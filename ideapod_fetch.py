import pandas as pd
import sqlite3

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
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def load_and_prepare_data(catering_file, space_file, member_file, product_file, database_path):
    """
    Load CSV files, clean and prepare data, then save to SQLite database
    """
    # 读取 CSV 文件
    catering_df = pd.read_csv(catering_file, dtype={"会员号": str,"订单号": str,"原订单号": str})
    space_df = pd.read_csv(space_file, dtype={"手机号": str})
    member_df = pd.read_csv(member_file, dtype={"会员号": str, "手机号": str})
    product_df = pd.read_csv(product_file)

    # 删除指定列
    catering_df.rename(columns={'入账时间（原下单时间）': '下单时间'}, inplace=True)
    catering_df.drop(columns=[
        "号牌", "FLIPOS版本", "状态", "代金券", "下单门店", "门店编号", "门店区域", 
        "入账门店", "ERP流水号", "第三方外卖平台单号", "配送平台", 
        "配送平台订单编号", "包装费", "配送费", "积分", "收银备注"
    ], inplace=True)
    space_df.rename(columns={'支付金额1': '吧台场景收入','支付金额2':'大众点评场景收入'}, inplace=True)
    space_df.drop(columns=["用户昵称"], inplace=True)
    member_df.drop(columns=["UnionID", "OpenID", "昵称", "标签", "首次消费门店", "最后消费门店"], inplace=True)

    
    product_df['商品名'] = product_df['商品名'].str.strip()
    product_df['产品类型'] = product_df['产品类型'].str.strip()
    product_df[['场景','食品','饮品','甜品','卡券','营销系列', '口味', '价格','备注']] = product_df[['场景','食品','饮品','甜品','卡券','营销系列', '口味', '价格','备注']].fillna('')
    
    
    # 格式化日期列
    catering_df = preprocess_datetime(catering_df)
    space_df = preprocess_datetime(space_df)
    member_df = preprocess_datetime(member_df)
    # 取消备注中的换行符
    space_df["订单备注"] = space_df["订单备注"].str.replace("\n", ",", regex=False)
    space_df["预定备注"] = space_df["预定备注"].str.replace("\n", ",", regex=False)
    catering_df["备注"] = catering_df["备注"].str.replace("\n", ",", regex=False)
    # 类型转换
    catering_df["会员号"] = catering_df["会员号"].astype(str)
    space_df["手机号"] = space_df["手机号"].astype(str)
    member_df["会员号"] = member_df["会员号"].astype(str)
    if "手机号" in member_df.columns:
        member_df["手机号"] = member_df["手机号"].astype(str)

    if "实际结束时间" in space_df.columns:
        space_df['实际结束时间'] = space_df['实际结束时间'].fillna('NA')

    # 内容处理
    space_df['订单商品名'] = space_df['订单商品名'].fillna('').str.strip().str.replace('上海洛克外滩店-', '').str.replace('ideaPod 二楼专注-', '').str.replace(' the Box', '')
        # 名字修改对应关系需要确认
    space_df['订单商品名'] = space_df['订单商品名'].replace({'丛林心流舱·日':'心流舱·巴赫','丛林心流舱·月':'心流舱·荣格','丛林心流舱·星':'心流舱·雨果','丛林心流舱·辰':'心流舱·牛顿','一层半帘区1':'蘑菇半帘区'})
    space_df['订单商品名'] = space_df['订单商品名'].apply(
        lambda x: '图书馆专注区' if x == '图书馆专注' else x
    )
        
    # 设置主键
    catering_df.set_index("会员号", inplace=True)
    space_df.set_index("手机号", inplace=True)
    member_df.set_index("会员号", inplace=True)
    product_df.set_index("商品名", inplace=True)

    # 保存到 SQLite 数据库并清理表
    conn = sqlite3.connect(database_path)
    try:
        catering_df.to_sql("Catering", conn, if_exists="replace", index=True)
        space_df.to_sql("Space", conn, if_exists="replace", index=True)
        member_df.to_sql("Member", conn, if_exists="replace", index=True)
        product_df.to_sql("Product", conn, if_exists="replace", index=True)

        print("数据已成功导入到 SQLite 数据库并完成清理！")
    finally:
        conn.close()

def main():
    # 文件路径
    catering_file = "db/raw_flipos.csv"
    space_file = "db/raw_space.csv"
    member_file = "db/raw_membership.csv"
    product_file = "db/ideapod_product.csv"
    database_path = "db/ideapod.db"

    # 加载和准备数据
    load_and_prepare_data(catering_file, space_file, member_file, product_file, database_path)

if __name__ == "__main__":
    main()