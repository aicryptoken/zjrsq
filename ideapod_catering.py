import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, Any
from datetime import timedelta

def connect_to_db(db_path: str) -> sqlite3.Connection:
    """Efficiently connect to SQLite database"""
    return sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)

def preprocess_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Central datetime preprocessing to reduce redundant operations"""
    datetime_columns = ['创建时间', '预定日期', '下单时间']
    for col in datetime_columns:
        if col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except Exception as e:
                    print(f"转换 {col} 列时出错：{e}")
    return df

def financial_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    财务分析：包括周度实收金额、周内销售金额/订单、时间段销售金额/订单
    """
    catering_df = preprocess_datetime(catering_df)
    
    # 添加时间维度
    catering_df['订单周'] = catering_df['下单时间'].dt.to_period('W-MON').apply(lambda x: x.start_time.date())
    catering_df['订单月份'] = catering_df['下单时间'].dt.to_period('M')
    catering_df['星期'] = catering_df['下单时间'].dt.day_name()  # 英文星期
    catering_df['订单时刻'] = catering_df['下单时间'].dt.hour

    # 周度收入分析
    weekly_revenue_df = catering_df.groupby('订单周').agg(
        销售收入=('实收', 'sum'),
        订单量=('订单号', 'count'),
        订单单价=('实收', 'mean')
    ).reset_index()
    weekly_revenue_df['订单周'] = weekly_revenue_df['订单周'].astype(str)

    # 定义星期顺序（英文转中文）
    weekday_order_en = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekday_order_cn = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']

    # 周内收入分布
    weekday_sales = catering_df.groupby(['订单月份', '星期', catering_df['下单时间'].dt.date]).agg(
        日销售金额=('实收', 'sum')  # 先按天汇总
    ).reset_index().groupby(['订单月份', '星期']).agg(
        日均销售金额=('日销售金额', 'mean')  # 再取日均值
    ).unstack(fill_value=0)
    weekday_sales.columns = [weekday_order_cn[weekday_order_en.index(col[1])] for col in weekday_sales.columns]  # 转换为中文
    weekday_sales = weekday_sales.reindex(columns=weekday_order_cn, fill_value=0).reset_index()
    weekday_sales['订单月份'] = weekday_sales['订单月份'].astype(str)

    # 周内单量分布
    weekday_orders = catering_df.groupby(['订单月份', '星期', catering_df['下单时间'].dt.date]).agg(
        日订单数量=('订单号', 'count')  # 先按天计数
    ).reset_index().groupby(['订单月份', '星期']).agg(
        日均订单数量=('日订单数量', 'mean')  # 再取日均值
    ).unstack(fill_value=0)
    weekday_orders.columns = [weekday_order_cn[weekday_order_en.index(col[1])] for col in weekday_orders.columns]  # 转换为中文
    weekday_orders = weekday_orders.reindex(columns=weekday_order_cn, fill_value=0).reset_index()
    weekday_orders['订单月份'] = weekday_orders['订单月份'].astype(str)

    # 日内收入分布
    hourly_sales = catering_df.groupby(['订单月份', '订单时刻', catering_df['下单时间'].dt.date]).agg(
        日销售金额=('实收', 'sum')  # 先按天汇总
    ).reset_index().groupby(['订单月份', '订单时刻']).agg(
        日均销售金额=('日销售金额', 'mean')  # 再取日均值
    ).unstack(fill_value=0)
    hourly_sales.columns = [f'{int(col[1])}时' for col in hourly_sales.columns]
    hourly_sales = hourly_sales.reset_index()
    hourly_sales['订单月份'] = hourly_sales['订单月份'].astype(str)

    # 日内单量分布
    hourly_orders = catering_df.groupby(['订单月份', '订单时刻', catering_df['下单时间'].dt.date]).agg(
        日订单数量=('订单号', 'count')  # 先按天计数
    ).reset_index().groupby(['订单月份', '订单时刻']).agg(
        日均订单数量=('日订单数量', 'mean')  # 再取日均值
    ).unstack(fill_value=0)
    hourly_orders.columns = [f'{int(col[1])}时' for col in hourly_orders.columns]
    hourly_orders = hourly_orders.reset_index()
    hourly_orders['订单月份'] = hourly_orders['订单月份'].astype(str)

    return {
        '销售收入_bar': weekly_revenue_df,
        '周内收入分布_stacked': weekday_sales,
        '周内单量分布_stacked': weekday_orders,
        '日内收入分布_stacked': hourly_sales,
        '日内单量分布_stacked': hourly_orders
    }

def order_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    订单分析：仅保留订单来源分析，按周拆分为销售金额、订单数量和订单单价
    """
    catering_df = preprocess_datetime(catering_df)
    catering_df['订单周'] = catering_df['下单时间'].dt.to_period('W-MON').apply(lambda x: x.start_time.date())

    # 销售收入分析
    source_sales = catering_df.groupby(['订单周', '订单来源']).agg(
        销售收入=('实收', 'sum')
    ).unstack(fill_value=0)
    source_sales.columns = [col[1] for col in source_sales.columns]
    source_sales = source_sales.reset_index()
    source_sales['订单周'] = source_sales['订单周'].astype(str)

    # 订单数量分析
    source_orders = catering_df.groupby(['订单周', '订单来源']).agg(
        订单数量=('订单号', 'count')
    ).unstack(fill_value=0)
    source_orders.columns = [col[1] for col in source_orders.columns]
    source_orders = source_orders.reset_index()
    source_orders['订单周'] = source_orders['订单周'].astype(str)

    # 订单单价分析（销售收入/订单数量）
    source_price = source_sales.copy()
    order_cols = [col for col in source_orders.columns if col != '订单周']
    for col in order_cols:
        source_price[col] = source_sales[col] / source_orders[col].replace(0, np.nan)  # 避免除以0
    source_price = source_price.fillna(0)  # 将NaN值填充为0

    return {
        '销售收入_来源_stacked': source_sales,
        '订单量_来源_stacked': source_orders,
        '订单单价_来源_stacked': source_price
    }

def product_analysis(catering_df: pd.DataFrame, conn) -> dict:
    """
    商品分析：3 - 产品周度销售分析，筛选前20个基础产品，按周输出销售数量
    """
    product_df = pd.read_sql_query("SELECT * FROM Product", conn)

    def parse_products(product_str, order_date):
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

    # 解析商品并合并基础产品信息
    catering_df = preprocess_datetime(catering_df)
    product_analysis = catering_df[['商品', '下单时间']].apply(lambda x: parse_products(x['商品'], x['下单时间']), axis=1)
    product_sales = pd.concat(product_analysis.tolist(), ignore_index=True)
    product_sales = pd.merge(product_sales, product_df[['商品名', '基础产品']], 
                            left_on='product', right_on='商品名', how='left')
    
    # 计算总销售量并筛选前20个基础产品
    top_products = product_sales.groupby('基础产品')['quantity'].sum().nlargest(20).index
    product_sales = product_sales[product_sales['基础产品'].isin(top_products)]
    
    # 添加周标识
    product_sales['订单周'] = product_sales['订单日期'].dt.to_period('W-MON').apply(lambda x: x.start_time.date())
    
    # 按周和基础产品统计销售数量
    weekly_product_sales = product_sales.groupby(['订单周', '基础产品']).agg(
        周销售数量=('quantity', 'sum')
    ).unstack(fill_value=0)
    weekly_product_sales.columns = [col[1] for col in weekly_product_sales.columns]
    weekly_product_sales = weekly_product_sales.reset_index()
    weekly_product_sales['订单周'] = weekly_product_sales['订单周'].astype(str)

    return {
        '产品销售量_bar': weekly_product_sales
    }

def marketing_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """营销分析保持不变"""
    catering_df = preprocess_datetime(catering_df)
    catering_df['订单月份'] = catering_df['下单时间'].dt.to_period('M')
    catering_df['促销类型'] = catering_df['使用优惠'].fillna('无优惠')

    promotion_analysis = catering_df.groupby(['订单月份', '促销类型']).agg(
        订单总数=('订单号', 'count'),
        销售收入=('实收', 'sum'),
        平均折扣=('打折', 'mean'),
        总折扣金额=('打折', 'sum')
    ).reset_index()
    promotion_analysis['订单月份'] = promotion_analysis['订单月份'].astype(str)

    return {
        '促销优惠分析（数据有误）_bar': promotion_analysis
    }

def user_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    用户分析：原来是以周度，将用户按订单次数分类，计算订单数量、总收入、平均订单价格
    现在修改为基于catering_df计算RFM用户价值
    """
    catering_df = preprocess_datetime(catering_df)
    # catering_df['订单周'] = catering_df['下单时间'].dt.to_period('W-MON').apply(lambda x: x.start_time.date())
    catering_df['member_id'] = np.where(
        catering_df['会员号'].isna(),
        catering_df.index.map(lambda x: f'8888000{x:04d}'),
        catering_df['会员号']
    )
    # 设置时间范围（过去180天）
    today = catering_df['下单时间'].max()
    begin_day = today - timedelta(days=180)  # 统计过去6个月的数据
    df_filtered = catering_df[catering_df['下单时间'] >= begin_day].copy()
    
    # 按会员号聚合计算所需指标
    rfm_analysis = df_filtered.groupby('member_id').agg({
        '下单时间': 'max',  # 最近一次下单时间
        '实收': 'sum',      # 总消费金额
        '订单号': 'count'   # 消费次数
    }).reset_index()
    
    # 重命名列
    rfm_analysis.columns = ['会员号', '最近下单时间', '总消费金额', '消费次数']
    rfm_analysis['Recency'] = (today - rfm_analysis['最近下单时间']).dt.days
    
    # 计算最近购买时间指数: e^(-λ * Recency), λ=0.015
    lambda_param = 0.015
    rfm_analysis['最近购买时间指数'] = np.exp(-lambda_param * rfm_analysis['Recency'])
    
    # 计算原始的消费力和消费频率指数
    rfm_analysis['monetary_raw'] = np.log(rfm_analysis['总消费金额'] + 1)
    rfm_analysis['frequency_raw'] = np.log(1 + rfm_analysis['消费次数'])
    
    # 进行min-max标准化
    monetary_min = rfm_analysis['monetary_raw'].min()
    monetary_max = rfm_analysis['monetary_raw'].max()
    frequency_min = rfm_analysis['frequency_raw'].min()
    frequency_max = rfm_analysis['frequency_raw'].max()
    
    rfm_analysis['消费力指数'] = (
        (rfm_analysis['monetary_raw'] - monetary_min) / (monetary_max - monetary_min)
    ) if monetary_max != monetary_min else 0
    
    rfm_analysis['消费频率指数'] = (
        (rfm_analysis['frequency_raw'] - frequency_min) / (frequency_max - frequency_min)
    ) if frequency_max != frequency_min else 0
    
    # 计算user_value
    weight_monetary = 0.6  # w = 0.6
    weight_frequency = 1 - weight_monetary  # 1 - w
    
    rfm_analysis['用户价值'] = (
        rfm_analysis['最近购买时间指数'] * 
        (rfm_analysis['消费力指数'] * weight_monetary + 
         rfm_analysis['消费频率指数'] * weight_frequency)
    )
    
    # 选择输出的列
    result = rfm_analysis[['会员号', '用户价值' ,'最近购买时间指数', '消费力指数', '消费频率指数']]
    
    return {
        'RFM分析结果': result
    }

def analyze(conn):
    """主分析函数"""
    try:
        catering_df = pd.read_sql_query("SELECT * FROM Catering", conn)
        
        financial_results = financial_analysis(catering_df)
        order_results = order_analysis(catering_df)
        product_results = product_analysis(catering_df, conn)
        marketing_results = marketing_analysis(catering_df)
        user_results = user_analysis(catering_df)

        all_results = {
            '财务数据': financial_results,
            '订单来源': order_results,
            '商品销量': product_results,
            '促销分析': marketing_results,
            '用户分析': user_results,
        }

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