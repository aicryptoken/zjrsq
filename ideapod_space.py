import sqlite3
import pandas as pd
from pandas import Timestamp, Period
from typing import Dict, Tuple
import numpy as np
from datetime import timedelta
import json

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
    # 使用 np.where 避免除以零，NaN 保留为 None
    space_df['使用率'] = np.where(
        space_df['预定时长'] == 0, 
        None,  # 除以零时返回 None，而不是 0
        (space_df['实际时长'] / space_df['预定时长']) * 100
    )
    space_df['是否高效使用'] = space_df['使用率'].apply(lambda x: 1 if pd.notna(x) and x >= 90 else 0)

    peak_analysis = space_df.groupby('开始使用时刻').agg({
        '订单编号': 'count',
        '实付金额': 'sum'
    }).reset_index()
    total_revenue = peak_analysis['实付金额'].sum()
    peak_analysis['收入占比'] = np.where(
        total_revenue == 0, 
        None, 
        peak_analysis['实付金额'] / total_revenue * 100
    )
    total_orders = peak_analysis['订单编号'].sum()
    peak_analysis['订单占比'] = np.where(
        total_orders == 0, 
        None, 
        peak_analysis['订单编号'] / total_orders * 100
    )

    space_df['是否首单'] = (space_df.groupby('会员号')['创建时间'].transform('rank', method='first') == 1).astype(int)
    purchase_comparison = space_df.groupby('是否首单').agg({
        '实付金额': ['mean', 'sum'],
        '实际时长': 'mean',
        '订单编号': 'count'
    }).reset_index()

    return {
        '高峰时段分析_bar': peak_analysis,
        '首单复购对比_bar': purchase_comparison
    }

def analyze_order_optimization(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """订单收入优化分析"""
    # 确保时间列是datetime格式并提取月份
    space_df['月份'] = space_df['创建时间'].dt.strftime('%Y-%m')
    
    # 1. 加钟次数表
    overtime_count = pd.pivot_table(
        space_df[space_df['加钟数'] > 0],
        values='订单编号',
        index='月份',
        columns='订单商品名',
        aggfunc='count',
        fill_value=0
    )
    
    overtime_duration = pd.pivot_table(
        space_df[space_df['加钟数'] > 0],
        values='加钟数',  # 假设数据框中有这个列
        index='月份',
        columns='订单商品名',
        aggfunc='sum',
        fill_value=0
    )
    
    # 3. 平均每次加钟时长表
    avg_overtime_duration = pd.pivot_table(
        space_df[space_df['加钟数'] > 0],
        values='加钟数',
        index='月份',
        columns='订单商品名',
        aggfunc='mean',
        fill_value=0
    )

    # 4. 加钟收入总计表（假设加钟收入包含在实付金额中且与加钟相关）
    overtime_revenue = pd.pivot_table(
        space_df[space_df['加钟数'] > 0],
        values='实付金额',
        index='月份',
        columns='订单商品名',
        aggfunc='sum',
        fill_value=0
    )
    
    return {
        '加钟次数表_bar': overtime_count,
        '加钟时长总计表_bar': overtime_duration,
        '平均加钟时长表_bar': avg_overtime_duration,
        '加钟收入总计表_bar': overtime_revenue
    }


def analyze_member_value(space_df: pd.DataFrame, db_path: str) -> Dict[str, pd.DataFrame]:
    space_df = calculate_user_intervals(space_df)
    active_users_df, churn_users_df, reactivated_users_df, new_users_df = calculate_user_metrics(space_df)
    result_df = pd.merge(active_users_df, churn_users_df, on='booking_month', how='left')
    result_df = pd.merge(result_df, reactivated_users_df, on='booking_month', how='left')
    result_df = pd.merge(result_df, new_users_df, on='booking_month', how='left')

    # 按月和会员等级分组统计
    monthly_level_stats = space_df.groupby(['booking_month', '等级']).agg({
        '实付金额': ['mean', 'sum'],
        '订单编号': 'count',
        '会员号': 'nunique'
    }).reset_index()
    monthly_level_stats.columns = ['月份', '等级', '平均收入', '总收入', '订单量', '独立会员数量']

    # 平均收入表
    avg_revenue_table = pd.pivot_table(
        monthly_level_stats,
        values='平均收入',
        index='月份',  # 行是月份
        columns='等级',  # 列是等级
        fill_value=0
    )

    # 总收入表
    total_revenue_table = pd.pivot_table(
        monthly_level_stats,
        values='总收入',
        index='月份',  # 行是月份
        columns='等级',  # 列是等级
        fill_value=0
    )

    # 订单量表
    order_volume_table = pd.pivot_table(
        monthly_level_stats,
        values='订单量',
        index='月份',  # 行是月份
        columns='等级',  # 列是等级
        fill_value=0
    )

    # 独立会员数量表
    unique_members_table = pd.pivot_table(
        monthly_level_stats,
        values='独立会员数量',
        index='月份',  # 行是月份
        columns='等级',  # 列是等级
        fill_value=0
    )
    # 计算用户留存与流失率
    retention_churn_df = result_df.copy()
    
    # 计算留存率
    retention_churn_df['30天留存率'] = retention_churn_df['新增用户'] / retention_churn_df['活跃用户_30天']
    retention_churn_df['60天留存率'] = retention_churn_df['新增用户'] / retention_churn_df['活跃用户_60天']
    retention_churn_df['90天留存率'] = retention_churn_df['新增用户'] / retention_churn_df['活跃用户_90天']

    # 计算流失率
    retention_churn_df['30天流失率'] = retention_churn_df['流失预警_30_60天'] / retention_churn_df['活跃用户_30天'].shift(1)
    retention_churn_df['60天流失率'] = retention_churn_df['流失预警_60_90天'] / retention_churn_df['流失预警_30_60天'].shift(1)
    retention_churn_df['90天流失率'] = retention_churn_df['已流失_90_120天'] / retention_churn_df['流失预警_60_90天'].shift(1)
    
    # 计算90天用户激活率
    retention_churn_df['90天用户激活率'] = retention_churn_df['重新激活用户'] / (
        retention_churn_df['流失预警_60_90天'] + retention_churn_df['已流失_90_120天']
    )
    # 替换NaN值为0
    retention_churn_df = retention_churn_df.fillna(0)

    return {
        '每月平均收入_bar': avg_revenue_table,
        '每月总收入_bar': total_revenue_table,
        '每月订单量_bar': order_volume_table,
        '每月独立会员数量_bar': unique_members_table,
        '用户留存与流失率_bar': retention_churn_df
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
    customer_analysis['首次消费时间'] = customer_analysis['首次消费时间'].astype(str)
    customer_analysis['最后消费时间'] = customer_analysis['最后消费时间'].astype(str)
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
    lifecycle['首次创建时间'] = lifecycle['首次创建时间'].astype(str)
    lifecycle['最后创建时间'] = lifecycle['最后创建时间'].astype(str)

    lifecycle_10_plus = lifecycle[lifecycle['订单数'] > 10]
    lifecycle_6_10 = lifecycle[lifecycle['订单数'].between(6, 10, inclusive='both')]

    return {
        '客户分层分析_bar': customer_tier_analysis,
        '10次以上用户生命周期_bar': lifecycle_10_plus,
        '6-10次用户生命周期_bar': lifecycle_6_10
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
    
    # 不填充为 0，保留 NaN 转为 None
    upgrade_analysis = upgrade_analysis[['月份', '未升舱订单数', '升舱订单数', '未升舱金额', '升舱金额']]
    upgrade_analysis['月份'] = upgrade_analysis['月份'].astype(str)
    
    return {'升舱分析_bar': upgrade_analysis}

def analyze_weekly_finance(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """周度财务分析"""
    space_df['booking_week'] = space_df['创建时间'].dt.to_period('W-MON').apply(lambda x: x.start_time.date())
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
    return {'财务分析_bar': weekly_analysis}

def analyze_space_types(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """周度空间类型分析"""
    space_df['booking_week'] = space_df['创建时间'].dt.to_period('W-MON').apply(lambda x: x.start_time.date())
    
    def calculate_space_utilization(df):
        space_type = df['订单商品名'].iloc[0]
        daily_hours = 17 if isinstance(space_type, str) and '心流舱' in space_type else 11
        total_hours = df['实际时长'].sum()
        return 0 if daily_hours == 0 else (total_hours / (daily_hours * 7)) * 100

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
        results[f'每个空间{metric}分析_bar'] = df.pivot(index='周', columns='空间类型', values=metric).reset_index().rename_axis(None, axis=1)

    # 周度日内使用率
    hourly_utilization = space_df.groupby(['booking_week', '开始使用时刻']).apply(
        lambda x: 0 if (17 if '心流舱' in x['订单商品名'].iloc[0] else 11) == 0 else (x['实际时长'].sum() / (17 if '心流舱' in x['订单商品名'].iloc[0] else 11) / 7) * 100
    ).reset_index(name='有效使用率')
    hourly_utilization['booking_week'] = hourly_utilization['booking_week'].astype(str)
    hourly_utilization_pivot = hourly_utilization.pivot(index='booking_week', columns='开始使用时刻', values='有效使用率').reset_index().rename_axis(None, axis=1)
    hourly_utilization_pivot = hourly_utilization_pivot.rename(columns={'booking_week': '周'})
    new_columns = ['周'] + [f"{int(col)}点" for col in hourly_utilization_pivot.columns[1:]]
    hourly_utilization_pivot.columns = new_columns
    results['周度日内使用率_stacked'] = hourly_utilization_pivot

    # 周内使用率
    # 修正星期名称为简洁的“周一”格式
    space_df['weekday'] = space_df['创建时间'].dt.day_name()  # 先用英文生成
    # 映射到中文
    weekday_map = {
        'Monday': '周一', 'Tuesday': '周二', 'Wednesday': '周三', 
        'Thursday': '周四', 'Friday': '周五', 'Saturday': '周六', 'Sunday': '周日'
    }
    space_df['weekday'] = space_df['weekday'].map(weekday_map)
    weekday_order = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
    space_df['booking_month'] = space_df['创建时间'].dt.to_period('M')
    
    weekly_day_utilization = space_df.groupby(['booking_month', 'weekday']).apply(
        lambda x: 0 if len(x['创建时间'].dt.date.unique()) == 0 else 
        (x['实际时长'].sum() / (17 if '心流舱' in x['订单商品名'].iloc[0] else 11) / len(x['创建时间'].dt.date.unique())) * 100
    ).reset_index(name='有效使用率')
    weekly_day_utilization['booking_month'] = weekly_day_utilization['booking_month'].astype(str)
    weekly_day_pivot = weekly_day_utilization.pivot(index='booking_month', columns='weekday', values='有效使用率').reset_index().rename_axis(None, axis=1)
    weekly_day_pivot = weekly_day_pivot.reindex(columns=['booking_month'] + weekday_order, fill_value=0)  # 填充缺失值为0
    weekly_day_pivot = weekly_day_pivot.rename(columns={'booking_month': '月份'})
    results['周内使用率_stacked'] = weekly_day_pivot

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
        # 处理时间类型列
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_period_dtype(df[col]):
                df[col] = df[col].astype(str)
        # 处理时间类型的索引
        if pd.api.types.is_datetime64_any_dtype(df.index) or pd.api.types.is_period_dtype(df.index):
            df.index = df.index.astype(str)
        # 将 NaN 替换为 None，确保 JSON 中为 null
        return df.replace({np.nan: None}).to_dict(orient='records')
    elif isinstance(data, dict):
        return {k: convert_df_to_dict(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_df_to_dict(item) for item in data]
    elif isinstance(data, (pd.Timestamp, pd.Period)) or pd.isna(data):
        return str(data) if not pd.isna(data) else None
    return data

def analyze(conn):
    try:
        space_df = pd.read_sql_query("SELECT * FROM Space", conn)
        member_df = pd.read_sql_query("SELECT 会员号, 等级 FROM Member", conn)

        space_df = space_df.merge(member_df, on='会员号', how='left')
        space_df['等级'] = space_df['等级'].astype(str)
        space_df['等级'] = space_df['等级'].fillna('未注册用户')
        space_df['等级'] = space_df['等级'].replace({'2.0': '关联户', '1.0': '微信注册用户', 'nan': '未注册用户'})
        space_df = space_df[space_df['等级'] != '0']

        space_df['订单商品名'] = space_df['订单商品名'].fillna('').str.replace('上海洛克外滩店-', '').str.replace('the Box', '')
        space_df['订单商品名'] = space_df['订单商品名'].apply(
            lambda x: '图书馆专注区' if x == '图书馆专注' else x
        )
        # 预处理数据，不填充数值列的 NaN，保留为 None
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
            '空间利用率': utilization_results,
            '细化空间分析': space_type_results
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
