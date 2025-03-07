import sqlite3
import pandas as pd
from typing import Dict, Tuple
import numpy as np
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
                df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def calculate_user_intervals(orders_df: pd.DataFrame) -> pd.DataFrame:
    """计算用户订单间隔"""
    orders_df['prev_order_date'] = orders_df.groupby('会员号')['创建时间'].shift(1)
    orders_df['order_interval'] = (orders_df['创建时间'] - orders_df['prev_order_date']).dt.days
    return orders_df

def calculate_user_metrics(orders_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """计算活跃用户、流失用户、重新激活用户和新增用户"""
    # 获取所有唯一的booking_month并转换为datetime
    unique_months = sorted(orders_df['booking_month'].unique())
    
    # 计算每个用户的最后订单日期和首次订单日期
    last_order_dates = orders_df.groupby('会员号')['创建时间'].max().reset_index()
    first_order_dates = orders_df.groupby('会员号')['创建时间'].min().reset_index()
    
    # 初始化结果容器
    active_users = []
    churn_users = []
    reactivated_users = []
    new_users = []
    
    for period_month in unique_months:
        # 将Period转换为datetime
        month_start = period_month.to_timestamp()
        month_end = period_month.to_timestamp('M')
        
        # 活跃用户
        active_30d = len(orders_df[(orders_df['创建时间'] >= month_end - timedelta(days=30)) &
                                 (orders_df['创建时间'] <= month_end)].会员号.unique())
        active_60d = len(orders_df[(orders_df['创建时间'] >= month_end - timedelta(days=60)) &
                                 (orders_df['创建时间'] <= month_end)].会员号.unique())
        active_90d = len(orders_df[(orders_df['创建时间'] >= month_end - timedelta(days=90)) &
                                 (orders_df['创建时间'] <= month_end)].会员号.unique())
        active_users.append({'booking_month': str(period_month), 'active_30d': active_30d,
                             'active_60d': active_60d, 'active_90d': active_90d})
        
        
        # 流失用户
        churn_30_60d = len(last_order_dates[(last_order_dates['创建时间'] < month_end - timedelta(days=30)) &
                                          (last_order_dates['创建时间'] >= month_end - timedelta(days=60))].会员号.unique())
        churn_60_90d = len(last_order_dates[(last_order_dates['创建时间'] < month_end - timedelta(days=60)) &
                                          (last_order_dates['创建时间'] >= month_end - timedelta(days=90))].会员号.unique())
        churn_90_120d = len(last_order_dates[(last_order_dates['创建时间'] < month_end - timedelta(days=90)) &
                                           (last_order_dates['创建时间'] >= month_end - timedelta(days=120))].会员号.unique())
        dormant_120d_plus = len(last_order_dates[last_order_dates['创建时间'] < month_end - timedelta(days=120)].会员号.unique())
        churn_users.append({'booking_month': str(period_month), 'churn_warning_30_60d': churn_30_60d,
                            'churn_warning_60_90d': churn_60_90d, 'churned_90_120d': churn_90_120d,
                            'dormant_120d_plus': dormant_120d_plus})
        
        
        # 重新激活用户
        users_with_previous_order = orders_df[(orders_df['创建时间'] < month_start - timedelta(days=90)) &
                                           (orders_df['创建时间'] >= month_start - timedelta(days=180))].会员号.unique()
        reactivated_in_month = len(orders_df[(orders_df['会员号'].isin(users_with_previous_order)) &
                                           (orders_df['创建时间'] >= month_start) &
                                           (orders_df['创建时间'] <= month_end)].会员号.unique())
        reactivated_users.append({'booking_month': str(period_month), 'reactivated_users': reactivated_in_month})
        
        
        # 新增用户
        new_in_month = len(first_order_dates[(first_order_dates['创建时间'] >= month_start) &
                                           (first_order_dates['创建时间'] <= month_end)].会员号.unique())
        new_users.append({'booking_month': str(period_month), 'new_user_count': new_in_month})
    
    return (
        pd.DataFrame(active_users),
        pd.DataFrame(churn_users),
        pd.DataFrame(reactivated_users),
        pd.DataFrame(new_users)
    )

def analyze_space_utilization(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """空间利用率优化分析"""
    # 预定vs实际使用时长分析
    space_df['使用率'] = (space_df['实际时长'] / space_df['预定时长']) * 100
    space_df['是否高效使用'] = (space_df['使用率'] >= 90).astype(int)

    # 高峰vs低谷时段分析
    peak_analysis = space_df.groupby('start_hour').agg({
        '订单编号': 'count',
        '实付金额': 'sum'
    }).reset_index()
    peak_analysis['收入占比'] = peak_analysis['实付金额'] / peak_analysis['实付金额'].sum() * 100
    peak_analysis['订单占比'] = peak_analysis['订单编号'] / peak_analysis['订单编号'].sum() * 100

    # 单次使用vs复购对比
    space_df['是否首单'] = (space_df.groupby('会员号')['创建时间'].transform('rank', method='first') == 1).astype(int)
    purchase_comparison = space_df.groupby('是否首单').agg({
        '实付金额': ['mean', 'sum'],
        '实际时长': 'mean',
        '订单编号': 'count'
    }).reset_index()

    return {
        '1-高峰时段分析': peak_analysis,
        '1-首单复购对比': purchase_comparison
    }

def analyze_order_optimization(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """订单收入优化分析"""
    # 加钟行为分析
    space_df['是否加钟'] = (space_df['加钟数'] > 0).astype(int)
    overtime_analysis = space_df.groupby(['订单商品名', 'start_hour']).agg({
        '是否加钟': 'sum',
        '订单编号': 'count', 
        '实付金额': 'mean'
    }).reset_index()
    overtime_analysis['加钟率'] = overtime_analysis['是否加钟'] / overtime_analysis['订单编号'] * 100

    # 未支付订单分析
    payment_status = space_df.groupby(['订单状态', '订单商品名']).agg({
        '订单编号': 'count',  
        '实付金额': 'sum'
    }).reset_index()
    
    total_orders = payment_status.groupby('订单商品名')['订单编号'].transform('sum')
    payment_status['订单占比'] = payment_status['订单编号'] / total_orders * 100

    return {
        '2-加钟分析': overtime_analysis,
        '2-支付状态分析': payment_status
    }

def analyze_member_value(space_df: pd.DataFrame, db_path: str) -> Dict[str, pd.DataFrame]:
    """会员消费复购分析及用户留存与流失分析"""
    # 1. 计算用户订单间隔
    space_df = calculate_user_intervals(space_df)
    
    # 2. 计算活跃用户、流失用户、重新激活用户和新增用户
    active_users_df, churn_users_df, reactivated_users_df, new_users_df = calculate_user_metrics(space_df)
    
    # 合并所有数据
    result_df = pd.merge(active_users_df, churn_users_df, on='booking_month', how='left')
    result_df = pd.merge(result_df, reactivated_users_df, on='booking_month', how='left')
    result_df = pd.merge(result_df, new_users_df, on='booking_month', how='left')
    
    member_level = space_df.groupby('等级').agg({
        '实付金额': ['mean', 'sum'],
        '订单编号': 'count',
        '会员号': 'nunique'
    }).reset_index()
    member_level.columns = ['等级', '平均实付金额', '总实付金额', '订单数', '独立会员数']

    lifecycle = space_df.groupby('会员号').agg({
        '创建时间': ['min', 'max'],
        '实付金额': 'sum',
        '订单编号': 'count'
    }).reset_index()
    lifecycle.columns = ['会员号', '首次创建时间', '最后创建时间', '总实付金额', '订单数']

    payment = space_df.groupby('支付方式').agg({
        '实付金额': ['mean', 'sum'],
        '订单编号': 'count'
    }).reset_index()
    payment.columns = ['支付方式', '平均实付金额', '总实付金额', '订单数']

    return {
        '3-会员等级消费': member_level,
        '3-用户生命周期': lifecycle,
        '3-支付方式影响': payment,
        '3-用户留存与流失': result_df
    }

def analyze_users(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """细化的用户分析"""
    def categorize_customers(total_orders):
        if total_orders == 1:
            return '单次消费'
        elif total_orders == 2:
            return '两次消费'
        elif 3 <= total_orders <= 5:
            return '3-5次消费'
        elif 6 <= total_orders <= 10:
            return '6-10次消费'
        return '10次以上'
    
    customer_analysis = space_df.groupby('会员号').agg({
        '订单编号': 'count', 
        '创建时间': ['min', 'max'],
        '实付金额': 'sum'
    })
    customer_analysis.columns = ['订单数', '首次消费时间', '最后消费时间', '总消费金额']
    customer_analysis = customer_analysis.reset_index()
    
    customer_analysis['customer_tier'] = customer_analysis['订单数'].apply(categorize_customers)
    customer_analysis['消费间隔'] = (customer_analysis['最后消费时间'] - 
                                customer_analysis['首次消费时间']).dt.days

    # 修改为基于 customer_tier 的分组分析
    customer_tier_analysis = customer_analysis.groupby('customer_tier').agg({
        '订单数': 'sum',
        '总消费金额': 'sum',
        '消费间隔': 'mean'
    }).reset_index()

    return {'4-客户分层分析': customer_tier_analysis}

def analyze_upgrades(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """合并的升舱分析"""
    # Convert 升舱 to 1/0 for aggregation
    space_df['升舱标记'] = (space_df['升舱'] == '是').astype(int)
    
    # Calculate basic metrics for all orders
    base_analysis = space_df.groupby('booking_month').agg({
        '升舱标记': 'sum',
        '订单编号': 'count' 
    }).reset_index()
    
    # Calculate revenue for upgraded orders
    upgrade_revenue = space_df[space_df['升舱标记'] == 1].groupby('booking_month').agg({
        '实付金额': 'sum'
    }).reset_index()
    upgrade_revenue.columns = ['booking_month', '升舱金额']
    
    # Calculate revenue for non-upgraded orders
    non_upgrade_revenue = space_df[space_df['升舱标记'] == 0].groupby('booking_month').agg({
        '实付金额': 'sum'
    }).reset_index()
    non_upgrade_revenue.columns = ['booking_month', '未升舱金额']
    
    # Combine all metrics
    upgrade_analysis = base_analysis.merge(
        upgrade_revenue, 
        on='booking_month', 
        how='left'
    ).merge(
        non_upgrade_revenue,
        on='booking_month',
        how='left'
    )
    
    # Rename and reorganize columns
    upgrade_analysis = upgrade_analysis.rename(columns={
        'booking_month': '月份',
        '升舱标记': '升舱订单数'
    })
    upgrade_analysis['未升舱订单数'] = upgrade_analysis['订单编号'] - upgrade_analysis['升舱订单数']
    
    # Fill any NaN values with 0
    upgrade_analysis = upgrade_analysis.fillna(0)
    
    # Select and order final columns
    upgrade_analysis = upgrade_analysis[[
        '月份', '未升舱订单数', '升舱订单数', '未升舱金额', '升舱金额'
    ]]
    
    # Convert month to string
    upgrade_analysis['月份'] = upgrade_analysis['月份'].astype(str)
    
    return {'5-升舱分析': upgrade_analysis}

def analyze_monthly_trends(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """合并的月度分析"""
    monthly_analysis = space_df.groupby('booking_month').agg({
        '订单编号': 'count',  
        '实付金额': ['sum', 'mean'],
        '会员号': 'nunique',
        '实际时长': ['sum', 'mean']
    })
    
    # Flatten column names
    monthly_analysis.columns = ['订单数', '总收入', '平均订单金额',
                              '活跃会员数', '总使用时长', '平均使用时长']
    monthly_analysis = monthly_analysis.reset_index()
    monthly_analysis['月份'] = monthly_analysis['booking_month'].astype(str)
    
    return {'6-月度趋势分析': monthly_analysis}

def analyze_space_types(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """合并的空间类型分析"""
    def calculate_space_utilization(df):
        # 获取空间类型
        space_type = df['订单商品名'].iloc[0]  # 直接从 DataFrame 中获取空间类型
        daily_hours = 17 if isinstance(space_type, str) and '心流舱' in space_type else 11
        total_hours = df['实际时长'].sum()
        days = df['创建时间'].dt.days_in_month.iloc[0]  # 从创建时间中获取月份的天数
        return (total_hours / (daily_hours * days)) * 100 if days > 0 else 0

    # 数据预处理：确保 '创建时间' 是 datetime 类型
    space_df['创建时间'] = pd.to_datetime(space_df['创建时间'])

    # 分组并计算利用率
    utilization_rates = (
        space_df.groupby(['booking_month', '订单商品名'])
        .apply(lambda x: calculate_space_utilization(x.reset_index(drop=True)))
        .reset_index(name='利用率')
    )

    # 分组并聚合基本指标
    space_type_analysis = space_df.groupby(['booking_month', '订单商品名']).agg({
        '订单编号': 'count', 
        '实付金额': ['sum', 'mean'],
        '实际时长': ['sum', 'mean']
    }).reset_index()

    # 展平多级列名
    space_type_analysis.columns = ['月份', '空间类型', '订单数', '总收入', '平均订单金额', '总使用时长', '平均使用时长']

    # 合并利用率数据
    space_type_analysis = space_type_analysis.merge(
        utilization_rates,
        left_on=['月份', '空间类型'],
        right_on=['booking_month', '订单商品名'],
        how='left'
    ).drop(columns=['booking_month', '订单商品名'])

    # 确保月份是字符串类型
    space_type_analysis['月份'] = space_type_analysis['月份'].astype(str)

    # 新增：时段使用率分析
    def calculate_hourly_utilization(df):
        space_type = df['订单商品名'].iloc[0]
        daily_hours = 17 if isinstance(space_type, str) and '心流舱' in space_type else 11
        total_hours = df['实际时长'].sum()
        return (total_hours / (daily_hours * len(df))) * 100

    hourly_utilization = (
        space_df.groupby(['start_hour', '订单商品名'])
        .apply(lambda x: calculate_hourly_utilization(x.reset_index(drop=True)))
        .reset_index(name='有效使用率')
    )

    hourly_utilization = hourly_utilization.groupby('start_hour').agg({
        '有效使用率': 'mean'
    }).reset_index()

    hourly_utilization.columns = ['时段', '有效使用率']

    return {
        '7-空间类型分析': space_type_analysis,
        '7-时段使用率分析': hourly_utilization  # 新增的时段使用率分析
    }

def convert_keys_to_str(data):
    """递归地将字典中的元组键转换为字符串"""
    if isinstance(data, dict):
        return {str(k) if isinstance(k, tuple) else k: convert_keys_to_str(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_keys_to_str(item) for item in data]
    return data
    
def analyze(conn):
    """
    分析 Space 表数据，返回结果字典。
    参数 conn: Flask 提供的数据库连接对象。
    """
    try:
        # 从数据库读取数据
        space_df = pd.read_sql_query("SELECT * FROM Space", conn)
        member_df = pd.read_sql_query("SELECT 会员号, 等级 FROM Member", conn)

        # 数据预处理
        space_df = space_df.merge(member_df, on='会员号', how='left')
        space_df['等级'] = space_df['等级'].fillna(-1)

        # 1) 删掉等级为0的数据
        space_df = space_df[space_df['等级'] != 0]

        # 2) 订单商品名 这一列，删除“上海洛克外滩店-”和“the Box”字符
        space_df['订单商品名'] = space_df['订单商品名'].fillna('').str.replace('上海洛克外滩店-', '').str.replace('the Box', '')

        # 处理时间字段
        space_df = preprocess_datetime(space_df)
        space_df['booking_month'] = space_df['创建时间'].dt.to_period('M')
        space_df['start_hour'] = pd.to_datetime(space_df['预定开始时间'], errors='coerce').dt.hour

        # 执行所有分析
        utilization_results = analyze_space_utilization(space_df)
        order_results = analyze_order_optimization(space_df)
        member_results = analyze_member_value(space_df, conn)  # 传递 conn 如果需要
        user_results = analyze_users(space_df)
        upgrade_results = analyze_upgrades(space_df)
        monthly_results = analyze_monthly_trends(space_df)
        space_type_results = analyze_space_types(space_df)

        # 合并所有结果为字典
        all_results = {
            'utilization': utilization_results,
            'order': order_results,
            'member': member_results,
            'user': user_results,
            'upgrade': upgrade_results,
            'monthly': monthly_results,
            'space_type': space_type_results
        }

        # 转换为可序列化格式
        def convert_df_to_dict(data):
            if isinstance(data, pd.DataFrame):
                return data.to_dict(orient='records')
            return data

        processed_results = {}
        for category, data in all_results.items():
            processed_results[category] = {key: convert_df_to_dict(value) for key, value in data.items()}
        
        processed_results = convert_keys_to_str(processed_results)

        return processed_results

    except sqlite3.Error as e:
        return {'error': f"Database error: {e}"}
    except Exception as e:
        return {'error': f"An error occurred: {e}"}
