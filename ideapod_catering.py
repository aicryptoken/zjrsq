import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, Any

def connect_to_db(db_path: str) -> sqlite3.Connection:
    """Efficiently connect to SQLite database"""
    return sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)

def preprocess_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Central datetime preprocessing to reduce redundant operations"""
    datetime_columns = ['创建时间', '预定日期', '下单时间']
    for col in datetime_columns:
        if col in df.columns:
            print(f"检查列: {col}, 数据类型为: {df[col].dtype}")  # 调试用，生产环境可移除
            if not pd.api.types.is_datetime64_any_dtype(df[col]):  # 检查是否已经是日期时间类型
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except Exception as e:
                    print(f"转换 {col} 列时出错：{e}")
    return df

def financial_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    财务分析：包括月度实收金额和时间段销售分析。
    - 月度实收金额：按月汇总实收金额、订单数量和平均订单价格。
    - 时间段销售分析：按小时分析订单数量、实收金额和平均订单价格。
    """
    catering_df = preprocess_datetime(catering_df)
    catering_df['订单月份'] = catering_df['下单时间'].dt.to_period('M')
    catering_df['订单时刻'] = catering_df['下单时间'].dt.hour

    # 月度实收金额分析
    monthly_revenue_df = catering_df.groupby('订单月份').agg(
        消费总金额=('实收', 'sum'),
        订单数量=('订单号', 'count'),
        订单单价=('实收', 'mean')
    ).reset_index()
    monthly_revenue_df['订单月份'] = monthly_revenue_df['订单月份'].astype(str)

    # 时间段销售分析
    time_period_analysis = catering_df.groupby(['订单月份', '订单时刻']).agg(
        订单数量=('订单号', 'count'),
        消费总金额=('实收', 'sum'),
        订单单价=('实收', 'mean')
    ).reset_index()
    time_period_analysis['订单月份'] = time_period_analysis['订单月份'].astype(str)

    return {
        '1 - 月度实收金额': monthly_revenue_df,
        '1 - 时间段销售分析': time_period_analysis
    }

def order_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    订单分析：包括订单来源分析、支付方式分析、退单分析、退单类型分析和服务方式分析。
    - 订单来源分析：按订单来源分析订单数量、实收金额和平均订单价格。
    - 支付方式分析：按支付方式分析订单数量、实收金额、平均订单价格、折扣订单比例和平均折扣。
    - 退单分析：按月分析退单数量和退单金额。
    - 退单类型分析：按月分析退单类型的分布。
    - 服务方式分析：按月分析服务方式的分布。
    """
    catering_df = preprocess_datetime(catering_df)
    catering_df['订单月份'] = catering_df['下单时间'].dt.to_period('M')
    catering_df['退款类型'] = catering_df['退单类型'].fillna('未退单')

    # 订单来源分析
    order_source_analysis = catering_df.groupby(['订单月份', '订单来源']).agg(
        订单数量=('订单号', 'count'),
        消费总金额=('实收', 'sum'),
        订单单价=('实收', 'mean')
    ).reset_index()
    order_source_analysis['订单月份'] = order_source_analysis['订单月份'].astype(str)

    # 支付方式分析
    payment_method_analysis = catering_df.groupby(['订单月份', '支付方式']).agg(
        订单数量=('订单号', 'count'),
        消费总金额=('实收', 'sum'),
        订单单价=('实收', 'mean'),
        折扣订单数占比=('打折', lambda x: (x > 0).mean() * 100),
        平均折扣=('打折', 'mean')
    ).reset_index()
    payment_method_analysis['订单月份'] = payment_method_analysis['订单月份'].astype(str)

    # 退单分析
    refund_analysis = catering_df.groupby(['订单月份', '退单类型']).agg(
        退单数量=('订单号', 'count'),
        退单金额=('实收', 'sum')
    ).reset_index()
    total_orders = catering_df.groupby('订单月份').size().reset_index(name='total_orders')
    refund_analysis = pd.merge(refund_analysis, total_orders, on='订单月份')
    refund_analysis['退款单占比'] = refund_analysis['退款单数量'] / refund_analysis['total_orders'] * 100
    refund_analysis['订单月份'] = refund_analysis['订单月份'].astype(str)

    # 退单类型分析
    refund_type_pivot = catering_df.groupby(['订单月份', '退款类型']).size().unstack(fill_value=0)
    refund_type_pivot = refund_type_pivot.apply(lambda x: x / x.sum() * 100, axis=1).reset_index()
    refund_type_pivot['订单月份'] = refund_type_pivot['订单月份'].astype(str)

    # 服务方式分析
    service_type_pivot = catering_df.groupby(['订单月份', '服务方式']).size().unstack(fill_value=0)
    service_type_pivot = service_type_pivot.apply(lambda x: x / x.sum() * 100, axis=1).reset_index()
    service_type_pivot['订单月份'] = service_type_pivot['订单月份'].astype(str)

    return {
        '2- 订单来源分析': order_source_analysis,
        '2- 支付方式分析': payment_method_analysis,
        '2- 退单分析': refund_analysis,
        '2- 退单类型分析': refund_type_pivot,
        '2- 服务方式分析': service_type_pivot
    }

def product_analysis(catering_df: pd.DataFrame, conn) -> dict:
    """
    商品分析：按月统计每个基础产品的销售数量。从数据库读取商品信息。
    """
    # 读取产品的基本信息
    product_df = pd.read_sql_query("SELECT * FROM Product", conn)

    def parse_products(product_str, order_date):
        """解析商品字符串并返回 DataFrame"""
        try:
            products = product_str.split(',')
            parsed_products = []
            for p in products:
                parts = p.split('x')
                if len(parts) == 2:
                    product_name = parts[0].strip()
                    quantity = float(parts[1].strip())
                    parsed_products.append({'product': product_name, 'quantity': quantity, '订单日期': order_date})
            return pd.DataFrame(parsed_products)
        except Exception as e:
            print(f"解析商品字符串时出错: {e}")
            return pd.DataFrame()

    # 解析每个订单的商品
    product_analysis = catering_df[['商品', '下单时间']].apply(lambda x: parse_products(x['商品'], x['下单时间']), axis=1)
    product_sales = pd.concat(product_analysis.tolist(), ignore_index=True)

    # 计算每个订单的月份
    product_sales['订单月份'] = product_sales['订单日期'].dt.to_period('M').astype(str)

    # 合并商品基本信息，包括基础产品、营销系列等
    product_sales = pd.merge(product_sales, product_df[['商品名', '基础产品', '营销系列', '口味', '套餐']], 
                             left_on='product', right_on='商品名', how='left')

    # 按月和基础产品统计销售数量
    product_monthly_sales = product_sales.groupby(['订单月份', '基础产品']).agg(
        总消费数量=('quantity', 'sum')
    ).reset_index()

    return {
        '3- 每月基础产品销售分析': product_monthly_sales.to_dict(orient='records')
    }


def marketing_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    营销分析：分析促销优惠的使用情况，包括订单数量、实收金额、平均折扣和总折扣金额。
    """
    catering_df = preprocess_datetime(catering_df)
    catering_df['订单月份'] = catering_df['下单时间'].dt.to_period('M')
    catering_df['促销类型'] = catering_df['使用优惠'].fillna('无优惠')

    promotion_analysis = catering_df.groupby(['订单月份', '促销类型']).agg(
        订单总数=('订单号', 'count'),
        消费总金额=('实收', 'sum'),
        平均折扣=('打折', 'mean'),
        总折扣金额=('打折', 'sum')
    ).reset_index()
    promotion_analysis['订单月份'] = promotion_analysis['订单月份'].astype(str)

    return {
        '4- 促销优惠分析': promotion_analysis
    }

def user_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    用户分析：按月统计不同会员活动类型的订单行为，包括订单数量、实收金额、平均订单数量、平均收入和平均订单价格。
    """
    catering_df = preprocess_datetime(catering_df)
    catering_df['订单月份'] = catering_df['下单时间'].dt.to_period('M')
    
    # 给会员号赋予唯一标识
    catering_df['member_id'] = np.where(
        catering_df['会员号'].isna(),
        catering_df.index.map(lambda x: f'8888000{x:04d}'),
        catering_df['会员号']
    )

    def classify_member_activity(total_orders):
        if total_orders == 1:
            return '1单用户'
        elif 2 <= total_orders <= 5:
            return '2-5单用户'
        elif 6 <= total_orders <= 10:
            return '5-10单用户'
        return '>10单用户'

    # 按会员号和月份统计订单行为
    member_monthly_analysis = catering_df.groupby(['member_id', '订单月份']).agg(
        总订单数=('订单号', 'count'),           # 修改为中文：总订单数
        总收入=('实收', 'sum'),               # 修改为中文：总收入
        平均订单数量=('商品', lambda x: x.str.count(',').mean() + 1),  # 修改为中文：平均订单数量
        平均收入=('实收', 'mean'),            # 修改为中文：平均收入
        平均订单价格=('实收', 'mean')         # 修改为中文：平均订单价格
    ).reset_index()

    # 给不同活动类型赋值
    member_monthly_analysis['用户消费频率'] = member_monthly_analysis['总订单数'].apply(classify_member_activity)

    # 按照每个月和不同会员活动类型统计
    member_activity_monthly_analysis = member_monthly_analysis.groupby(['订单月份', '用户消费频率']).agg(
        总订单数=('总订单数', 'sum'),          # 修改为中文：总订单数
        总收入=('总收入', 'sum'),             # 修改为中文：总收入
        平均订单数量=('平均订单数量', 'mean'), # 修改为中文：平均订单数量
        平均收入=('平均收入', 'mean'),        # 修改为中文：平均收入
        平均订单价格=('平均订单价格', 'mean') # 修改为中文：平均订单价格
    ).reset_index()

    member_activity_monthly_analysis['订单月份'] = member_activity_monthly_analysis['订单月份'].astype(str)

    return {
        '5- 会员活动按月分析': member_activity_monthly_analysis
    }


def analyze(conn):
    """
    分析 Catering 表数据，返回结果字典。
    参数 conn: Flask 提供的数据库连接对象。
    """
    try:
        # Read catering data
        catering_df = pd.read_sql_query("SELECT * FROM Catering", conn)
        
        
        # Perform analysis
        financial_results = financial_analysis(catering_df)
        order_results = order_analysis(catering_df)           
        product_results = product_analysis(catering_df, conn)      
        marketing_results = marketing_analysis(catering_df)            
        user_results = user_analysis(catering_df) 
        
        # Combine all results
        all_results = {
            '财务分析': financial_results,
            '订单分析': order_results,
            '商品分析': product_results,
            '市场分析': marketing_results,
            '用户分析': user_results,
        }

        # 将 DataFrame 转换为可序列化的格式，以便模板渲染
        def convert_df_to_dict(data):
            if isinstance(data, pd.DataFrame):
                return data.to_dict(orient='records')
            return data

        processed_results = {}
        for category, data in all_results.items():
            processed_results[category] = {
                key: convert_df_to_dict(value) for key, value in data.items()
            }

        return processed_results

    except sqlite3.Error as e:
        return {'error': f"Database error: {e}"}
    except Exception as e:
        return {'error': f"An error occurred: {e}"}