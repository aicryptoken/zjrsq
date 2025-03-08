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
    
    catering_df['订单周'] = catering_df['下单时间'].dt.to_period('W-MON')
    catering_df['订单月份'] = catering_df['下单时间'].dt.to_period('M')
    catering_df['星期'] = catering_df['下单时间'].dt.day_name()
    catering_df['订单时刻'] = catering_df['下单时间'].dt.hour

    weekly_revenue_df = catering_df.groupby('订单周').agg(
        消费总金额=('实收', 'sum'),
        订单数量=('订单号', 'count'),
        订单单价=('实收', 'mean')
    ).reset_index()
    weekly_revenue_df['订单周'] = weekly_revenue_df['订单周'].astype(str)

    weekday_sales = catering_df.groupby(['订单月份', '星期']).agg(
        日均销售金额=('实收', 'mean')
    ).unstack(fill_value=0)
    weekday_sales.columns = [col[1] for col in weekday_sales.columns]
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekday_sales = weekday_sales.reindex(columns=weekday_order).reset_index()
    weekday_sales['订单月份'] = weekday_sales['订单月份'].astype(str)

    weekday_orders = catering_df.groupby(['订单月份', '星期']).agg(
        日均订单数量=('订单号', 'count')
    ).unstack(fill_value=0)
    weekday_orders.columns = [col[1] for col in weekday_orders.columns]
    weekday_orders = weekday_orders.reindex(columns=weekday_order).reset_index()
    weekday_orders['订单月份'] = weekday_orders['订单月份'].astype(str)

    hourly_sales = catering_df.groupby(['订单月份', '订单时刻']).agg(
        日均销售金额=('实收', 'mean')
    ).unstack(fill_value=0)
    hourly_sales.columns = [f'{int(col[1])}时' for col in hourly_sales.columns]
    hourly_sales = hourly_sales.reset_index()
    hourly_sales['订单月份'] = hourly_sales['订单月份'].astype(str)

    hourly_orders = catering_df.groupby(['订单月份', '订单时刻']).agg(
        日均订单数量=('订单号', 'count')
    ).unstack(fill_value=0)
    hourly_orders.columns = [f'{int(col[1])}时' for col in hourly_orders.columns]
    hourly_orders = hourly_orders.reset_index()
    hourly_orders['订单月份'] = hourly_orders['订单月份'].astype(str)

    return {
        '1 - 周度实收金额': weekly_revenue_df,
        '1 - 周内销售金额': weekday_sales,
        '1 - 周内销售订单': weekday_orders,
        '1 - 时间段销售金额': hourly_sales,
        '1 - 时间段订单数量': hourly_orders
    }

def order_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    订单分析：仅保留订单来源分析，按周拆分为销售金额和订单数量
    """
    catering_df = preprocess_datetime(catering_df)
    catering_df['订单周'] = catering_df['下单时间'].dt.to_period('W-MON')

    source_sales = catering_df.groupby(['订单周', '订单来源']).agg(
        消费总金额=('实收', 'sum')
    ).unstack(fill_value=0)
    source_sales.columns = [col[1] for col in source_sales.columns]
    source_sales = source_sales.reset_index()
    source_sales['订单周'] = source_sales['订单周'].astype(str)

    source_orders = catering_df.groupby(['订单周', '订单来源']).agg(
        订单数量=('订单号', 'count')
    ).unstack(fill_value=0)
    source_orders.columns = [col[1] for col in source_orders.columns]
    source_orders = source_orders.reset_index()
    source_orders['订单周'] = source_orders['订单周'].astype(str)

    return {
        '2 - 按订单来源分销售金额': source_sales,
        '2 - 按订单来源分订单数量': source_orders
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
    product_sales['订单周'] = product_sales['订单日期'].dt.to_period('W-MON')
    
    # 按周和基础产品统计销售数量
    weekly_product_sales = product_sales.groupby(['订单周', '基础产品']).agg(
        周销售数量=('quantity', 'sum')
    ).unstack(fill_value=0)
    weekly_product_sales.columns = [col[1] for col in weekly_product_sales.columns]
    weekly_product_sales = weekly_product_sales.reset_index()
    weekly_product_sales['订单周'] = weekly_product_sales['订单周'].astype(str)

    return {
        '3 - 产品周度销售分析': weekly_product_sales
    }

def marketing_analysis(catering_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """营销分析保持不变"""
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
    用户分析：改为周度数据，拆分为总订单数量、总收入、平均订单价格三个表
    """
    catering_df = preprocess_datetime(catering_df)
    catering_df['订单周'] = catering_df['下单时间'].dt.to_period('W-MON')
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

    # 按会员号和周统计订单行为
    member_weekly_analysis = catering_df.groupby(['member_id', '订单周']).agg(
        总订单数=('订单号', 'count'),
        总收入=('实收', 'sum'),
        平均订单价格=('实收', 'mean')
    ).reset_index()
    member_weekly_analysis['用户消费频率'] = member_weekly_analysis['总订单数'].apply(classify_member_activity)

    # 按周和用户消费频率统计，拆分为三个表
    # 总订单数量
    weekly_orders = member_weekly_analysis.groupby(['订单周', '用户消费频率']).agg(
        总订单数=('总订单数', 'sum')
    ).unstack(fill_value=0)
    weekly_orders.columns = [col[1] for col in weekly_orders.columns]
    weekly_orders = weekly_orders.reset_index()
    weekly_orders['订单周'] = weekly_orders['订单周'].astype(str)

    # 总收入
    weekly_revenue = member_weekly_analysis.groupby(['订单周', '用户消费频率']).agg(
        总收入=('总收入', 'sum')
    ).unstack(fill_value=0)
    weekly_revenue.columns = [col[1] for col in weekly_revenue.columns]
    weekly_revenue = weekly_revenue.reset_index()
    weekly_revenue['订单周'] = weekly_revenue['订单周'].astype(str)

    # 平均订单价格
    weekly_avg_price = member_weekly_analysis.groupby(['订单周', '用户消费频率']).agg(
        平均订单价格=('平均订单价格', 'mean')
    ).unstack(fill_value=0)
    weekly_avg_price.columns = [col[1] for col in weekly_avg_price.columns]
    weekly_avg_price = weekly_avg_price.reset_index()
    weekly_avg_price['订单周'] = weekly_avg_price['订单周'].astype(str)

    return {
        '5 - 会员周度总订单数量': weekly_orders,
        '5 - 会员周度总收入': weekly_revenue,
        '5 - 会员周度平均订单价格': weekly_avg_price
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
            '财务分析': financial_results,
            '订单分析': order_results,
            '商品分析': product_results,
            '市场分析': marketing_results,
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