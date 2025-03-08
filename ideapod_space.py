import sqlite3
import pandas as pd
from pandas import Timestamp, Period
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
    orders_df['上次下单日期'] = orders_df.groupby('会员号')['创建时间'].shift(1)
    orders_df['order_interval'] = (orders_df['创建时间'] - orders_df['上次下单日期']).dt.days
    return orders_df

def calculate_user_metrics(orders_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """计算活跃用户、流失用户、重新激活用户和新增用户"""
    unique_months = sorted(orders_df['booking_month'].unique())
    
    last_order_dates = orders_df.groupby('会员号')['创建时间'].max().reset_index()
    first_order_dates = orders_df.groupby('会员号')['创建时间'].min().reset_index()
    
    active_users, churn_users, reactivated_users, new_users = [], [], [], []
    
    for period_month in unique_months:
        month_start = period_month.to_timestamp()
        month_end = period_month.to_timestamp('M')
        
        active_30d = len(orders_df[(orders_df['创建时间'] >= month_end - timedelta(days=30)) &
                                 (orders_df['创建时间'] <= month_end)].会员号.unique())
        active_60d = len(orders_df[(orders_df['创建时间'] >= month_end - timedelta(days=60)) &
                                 (orders_df['创建时间'] <= month_end)].会员号.unique())
        active_90d = len(orders_df[(orders_df['创建时间'] >= month_end - timedelta(days=90)) &
                                 (orders_df['创建时间'] <= month_end)].会员号.unique())
        active_users.append({'booking_month': str(period_month), '活跃用户_30天': active_30d,
                             '活跃用户_60天': active_60d, '活跃用户_90天': active_90d})
        
        churn_30_60d = len(last_order_dates[(last_order_dates['创建时间'] < month_end - timedelta(days=30)) &
                                          (last_order_dates['创建时间'] >= month_end - timedelta(days=60))].会员号.unique())
        churn_60_90d = len(last_order_dates[(last_order_dates['创建时间'] < month_end - timedelta(days=60)) &
                                          (last_order_dates['创建时间'] >= month_end - timedelta(days=90))].会员号.unique())
        churn_90_120d = len(last_order_dates[(last_order_dates['创建时间'] < month_end - timedelta(days=90)) &
                                           (last_order_dates['创建时间'] >= month_end - timedelta(days=120))].会员号.unique())
        dormant_120d_plus = len(last_order_dates[last_order_dates['创建时间'] < month_end - timedelta(days=120)].会员号.unique())
        churn_users.append({'booking_month': str(period_month), '流失预警_30_60天': churn_30_60d,
                            '流失预警_60_90天': churn_60_90d, '已流失_90_120天': churn_90_120d,
                            '长期未活跃_120天以上': dormant_120d_plus})
        
        users_with_previous_order = orders_df[(orders_df['创建时间'] < month_start - timedelta(days=90)) &
                                           (orders_df['创建时间'] >= month_start - timedelta(days=180))].会员号.unique()
        reactivated_in_month = len(orders_df[(orders_df['会员号'].isin(users_with_previous_order)) &
                                           (orders_df['创建时间'] >= month_start) &
                                           (orders_df['创建时间'] <= month_end)].会员号.unique())
        reactivated_users.append({'booking_month': str(period_month), '重新激活用户': reactivated_in_month})
        
        new_in_month = len(first_order_dates[(first_order_dates['创建时间'] >= month_start) &
                                           (first_order_dates['创建时间'] <= month_end)].会员号.unique())
        new_users.append({'booking_month': str(period_month), '新增用户': new_in_month})
    
    return (
        pd.DataFrame(active_users),
        pd.DataFrame(churn_users),
        pd.DataFrame(reactivated_users),
        pd.DataFrame(new_users)
    )

def analyze_space_utilization(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """空间利用率优化分析"""
    space_df['使用率'] = (space_df['实际时长'] / space_df['预定时长']) * 100
    space_df['是否高效使用'] = (space_df['使用率'] >= 90).astype(int)

    peak_analysis = space_df.groupby('开始使用时刻').agg({
        '订单编号': 'count',
        '实付金额': 'sum'
    }).reset_index()
    peak_analysis['收入占比'] = peak_analysis['实付金额'] / peak_analysis['实付金额'].sum() * 100
    peak_analysis['订单占比'] = peak_analysis['订单编号'] / peak_analysis['订单编号'].sum() * 100

    space_df['是否首单'] = (space_df.groupby('会员号')['创建时间'].transform('rank', method='first') == 1).astype(int)
    purchase_comparison = space_df.groupby('是否首单').agg({
        '实付金额': ['mean', 'sum'],
        '实际时长': 'mean',
        '订单编号': 'count'
    }).reset_index()

    return {
        '6-高峰时段分析': peak_analysis,
        '6-首单复购对比': purchase_comparison
    }

def analyze_order_optimization(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """订单收入优化分析"""
    space_df['是否加钟'] = (space_df['加钟数'] > 0).astype(int)
    overtime_analysis = space_df.groupby(['订单商品名', '开始使用时刻']).agg({
        '是否加钟': 'sum',
        '订单编号': 'count', 
        '实付金额': 'mean'
    }).reset_index()
    overtime_analysis['加钟率'] = overtime_analysis['是否加钟'] / overtime_analysis['订单编号'] * 100

    return {
        '2-加钟分析': overtime_analysis
    }

def analyze_member_value(space_df: pd.DataFrame, db_path: str) -> Dict[str, pd.DataFrame]:
    space_df = calculate_user_intervals(space_df)
    active_users_df, churn_users_df, reactivated_users_df, new_users_df = calculate_user_metrics(space_df)
    result_df = pd.merge(active_users_df, churn_users_df, on='booking_month', how='left')
    result_df = pd.merge(result_df, reactivated_users_df, on='booking_month', how='left')
    result_df = pd.merge(result_df, new_users_df, on='booking_month', how='left')

    member_level = space_df.groupby('等级').agg({
        '实付金额': ['mean', 'sum'],
        '订单编号': 'count',
        '会员号': 'nunique'
    }).reset_index()
    member_level.columns = ['等级', '平均实付金额', '总实付金额', '订单数', '独立会员数']

    payment = space_df.groupby('支付方式').agg({
        '实付金额': ['mean', 'sum'],
        '订单编号': 'count'
    }).reset_index()
    payment.columns = ['支付方式', '平均实付金额', '总实付金额', '订单数']

    return {
        '3-会员等级消费': member_level,
        '3-支付方式影响': payment,
        '3-用户留存与流失': result_df
    }

def analyze_users(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
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
    customer_analysis['消费间隔'] = (customer_analysis['最后消费时间'] - customer_analysis['首次消费时间']).dt.days
    customer_analysis['首次消费时间'] = customer_analysis['首次消费时间'].fillna('未知').astype(str)
    customer_analysis['最后消费时间'] = customer_analysis['最后消费时间'].fillna('未知').astype(str)
    customer_analysis['用户消费次数'] = customer_analysis['订单数'].apply(categorize_customers)

    customer_tier_analysis = customer_analysis.groupby('用户消费次数').agg({
        '会员号': 'nunique',
        '订单数': 'sum',
        '总消费金额': 'sum',
        '消费间隔': 'mean'
    }).reset_index()
    customer_tier_analysis.columns = ['用户消费次数', '独立用户数', '订单数', '总消费金额', '平均消费间隔']

    lifecycle = space_df.groupby('会员号').agg({
        '创建时间': ['min', 'max'],
        '实付金额': 'sum',
        '订单编号': 'count'
    }).reset_index()
    lifecycle.columns = ['会员号', '首次创建时间', '最后创建时间', '总实付金额', '订单数']
    lifecycle['时间跨度'] = (lifecycle['最后创建时间'] - lifecycle['首次创建时间']).dt.days
    lifecycle['首次创建时间'] = lifecycle['首次创建时间'].fillna('未知').astype(str)
    lifecycle['最后创建时间'] = lifecycle['最后创建时间'].fillna('未知').astype(str)

    lifecycle_10_plus = lifecycle[lifecycle['订单数'] > 10]
    lifecycle_6_10 = lifecycle[lifecycle['订单数'].between(6, 10, inclusive='both')]

    return {
        '4-客户分层分析': customer_tier_analysis,
        '4-10次以上用户生命周期': lifecycle_10_plus,
        '4-6-10次用户生命周期': lifecycle_6_10
    }

def analyze_upgrades(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """合并的升舱分析"""
    space_df['升舱标记'] = (space_df['升舱'] == '是').astype(int)
    
    base_analysis = space_df.groupby('booking_month').agg({
        '升舱标记': 'sum',
        '订单编号': 'count' 
    }).reset_index()
    
    upgrade_revenue = space_df[space_df['升舱标记'] == 1].groupby('booking_month').agg({
        '实付金额': 'sum'
    }).reset_index()
    upgrade_revenue.columns = ['booking_month', '升舱金额']
    
    non_upgrade_revenue = space_df[space_df['升舱标记'] == 0].groupby('booking_month').agg({
        '实付金额': 'sum'
    }).reset_index()
    non_upgrade_revenue.columns = ['booking_month', '未升舱金额']
    
    upgrade_analysis = base_analysis.merge(
        upgrade_revenue, 
        on='booking_month', 
        how='left'
    ).merge(
        non_upgrade_revenue,
        on='booking_month',
        how='left'
    )
    
    upgrade_analysis = upgrade_analysis.rename(columns={
        'booking_month': '月份',
        '升舱标记': '升舱订单数'
    })
    upgrade_analysis['未升舱订单数'] = upgrade_analysis['订单编号'] - upgrade_analysis['升舱订单数']
    
    upgrade_analysis = upgrade_analysis.fillna(0)
    upgrade_analysis = upgrade_analysis[['月份', '未升舱订单数', '升舱订单数', '未升舱金额', '升舱金额']]
    upgrade_analysis['月份'] = upgrade_analysis['月份'].astype(str)
    
    return {'5-升舱分析': upgrade_analysis}

def analyze_weekly_finance(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """周度财务分析"""
    space_df['booking_week'] = space_df['创建时间'].dt.to_period('W-MON')
    weekly_analysis = space_df.groupby('booking_week').agg({
        '订单编号': 'count',  
        '实付金额': ['sum', 'mean'],
        '会员号': 'nunique',
        '实际时长': ['sum', 'mean']
    })
    
    weekly_analysis.columns = ['订单数', '总收入', '平均订单金额', '活跃会员数', '总使用时长', '平均使用时长']
    weekly_analysis = weekly_analysis.reset_index()
    weekly_analysis['周'] = weekly_analysis['booking_week'].astype(str)
    weekly_analysis = weekly_analysis.drop(columns=['booking_week'])
    weekly_analysis = weekly_analysis[['周', '订单数', '总收入', '平均订单金额', '活跃会员数', '总使用时长', '平均使用时长']]
    return {'1-周度财务分析': weekly_analysis}

def analyze_space_types(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """周度空间类型分析"""
    space_df['booking_week'] = space_df['创建时间'].dt.to_period('W-MON')
    
    def calculate_space_utilization(df):
        space_type = df['订单商品名'].iloc[0]
        daily_hours = 17 if isinstance(space_type, str) and '心流舱' in space_type else 11
        total_hours = df['实际时长'].sum()
        return (total_hours / (daily_hours * 7)) * 100  # 按周计算，7天

    results = {}
    for metric in ['订单数', '总收入', '总使用时长', '平均使用时长', '利用率']:
        if metric == '订单数':
            df = space_df.groupby(['booking_week', '订单商品名'])['订单编号'].count().reset_index()
            df.columns = ['周', '空间类型', '订单数']
        elif metric == '总收入':
            df = space_df.groupby(['booking_week', '订单商品名'])['实付金额'].sum().reset_index()
            df.columns = ['周', '空间类型', '总收入']
        elif metric == '总使用时长':
            df = space_df.groupby(['booking_week', '订单商品名'])['实际时长'].sum().reset_index()
            df.columns = ['周', '空间类型', '总使用时长']
        elif metric == '平均使用时长':
            df = space_df.groupby(['booking_week', '订单商品名'])['实际时长'].mean().reset_index()
            df.columns = ['周', '空间类型', '平均使用时长']
        elif metric == '利用率':
            df = space_df.groupby(['booking_week', '订单商品名']).apply(calculate_space_utilization).reset_index(name='利用率')
            df.columns = ['周', '空间类型', '利用率']
        
        df['周'] = df['周'].astype(str)
        results[f'7-空间类型分析-{metric}'] = df.pivot(index='周', columns='空间类型', values=metric).reset_index().rename_axis(None, axis=1)

    hourly_utilization = space_df.groupby(['booking_week', '开始使用时刻']).apply(
        lambda x: (x['实际时长'].sum() / (17 if '心流舱' in x['订单商品名'].iloc[0] else 11) / 7) * 100
    ).reset_index(name='有效使用率')
    hourly_utilization['booking_week'] = hourly_utilization['booking_week'].astype(str)
    hourly_utilization_pivot = hourly_utilization.pivot(index='booking_week', columns='开始使用时刻', values='有效使用率').reset_index().rename_axis(None, axis=1)
    hourly_utilization_pivot = hourly_utilization_pivot.rename(columns={'booking_week': '周'})
    results['7-周度时段使用率分析'] = hourly_utilization_pivot

    space_df['weekday'] = space_df['创建时间'].dt.day_name(locale='zh_CN')
    weekday_order = ['星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期日']
    space_df['booking_month'] = space_df['创建时间'].dt.to_period('M')
    weekly_day_utilization = space_df.groupby(['booking_month', 'weekday']).apply(
        lambda x: (x['实际时长'].sum() / (17 if '心流舱' in x['订单商品名'].iloc[0] else 11) / len(x['创建时间'].dt.date.unique())) * 100
    ).reset_index(name='有效使用率')
    weekly_day_utilization['booking_month'] = weekly_day_utilization['booking_month'].astype(str)
    weekly_day_pivot = weekly_day_utilization.pivot(index='booking_month', columns='weekday', values='有效使用率').reset_index().rename_axis(None, axis=1)
    weekly_day_pivot = weekly_day_pivot.reindex(columns=['booking_month'] + weekday_order)
    weekly_day_pivot = weekly_day_pivot.rename(columns={'booking_month': '月份'})
    results['7-周内使用率分析'] = weekly_day_pivot

    return results

def convert_keys_to_str(data):
    if isinstance(data, dict):
        return {str(k) if isinstance(k, tuple) else k: convert_keys_to_str(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_keys_to_str(item) for item in data]
    return data
    
def convert_df_to_dict(data):
    if isinstance(data, pd.DataFrame):
        df = data.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_period_dtype(df[col]):
                df[col] = df[col].fillna('未知').astype(str)
        return df.to_dict(orient='records')
    elif isinstance(data, dict):
        return {k: convert_df_to_dict(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_df_to_dict(item) for item in data]
    elif isinstance(data, (pd.Timestamp, pd.Period)) or pd.isna(data):
        return str(data) if not pd.isna(data) else '未知'
    return data

def analyze(conn):
    try:
        space_df = pd.read_sql_query("SELECT * FROM Space", conn)
        member_df = pd.read_sql_query("SELECT 会员号, 等级 FROM Member", conn)

        space_df = space_df.merge(member_df, on='会员号', how='left')
        space_df['等级'] = space_df['等级'].fillna(-1)
        space_df = space_df[space_df['等级'] != 0]
        space_df['订单商品名'] = space_df['订单商品名'].fillna('').str.replace('上海洛克外滩店-', '').str.replace('the Box', '')

        space_df = preprocess_datetime(space_df)
        space_df['booking_month'] = space_df['创建时间'].dt.to_period('M')
        space_df['开始使用时刻'] = pd.to_datetime(space_df['预定开始时间'], errors='coerce').dt.hour

        utilization_results = analyze_space_utilization(space_df)
        order_results = analyze_order_optimization(space_df)
        member_results = analyze_member_value(space_df, conn)
        user_results = analyze_users(space_df)
        upgrade_results = analyze_upgrades(space_df)
        weekly_results = analyze_weekly_finance(space_df)
        space_type_results = analyze_space_types(space_df)

        all_results = {
            '财务数据': weekly_results,
            '订单分析': order_results,
            '会员分析': member_results,
            '用户分析': user_results,
            '升舱分析': upgrade_results,
            '空间利用率分析': utilization_results,
            '空间类型周度分析': space_type_results
        }

        processed_results = {}
        for category, data in all_results.items():
            processed_results[category] = {key: convert_df_to_dict(value) for key, value in data.items()}
        
        processed_results = convert_keys_to_str(processed_results)
        return processed_results

    except sqlite3.Error as e:
        return {'error': f"Database error: {e}"}
    except Exception as e:
        return {'error': f"An error occurred: {e}"}