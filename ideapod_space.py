import sqlite3
import pandas as pd
from pandas import Timestamp, Period
from typing import Dict, Tuple
import numpy as np
from datetime import timedelta, time

def connect_to_db(db_path: str) -> sqlite3.Connection:
    """Efficiently connect to SQLite database"""
    return sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)

def preprocess_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Central datetime preprocessing to reduce redundant operations"""
    datetime_columns = ['创建时间', '预定开始时间', '预定结束时间']
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

def analyze_order(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """合并的订单优化和升舱分析"""
    # 确保时间列是datetime格式并提取月份
    space_df['月份'] = space_df['创建时间'].dt.strftime('%Y-%m')
    space_df['升舱标记'] = (space_df['升舱'] == '是').astype(int)
    
    # 1. 升舱分析 - 按月和订单商品名统计
    # 升舱订单数表
    upgrade_count = pd.pivot_table(
        space_df,
        values='升舱标记',
        index='月份',
        columns='订单商品名',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    # 升舱金额表
    upgrade_amount = pd.pivot_table(
        space_df[space_df['升舱标记'] == 1],
        values='实付金额',
        index='月份',
        columns='订单商品名',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    # 升舱订单数占比表
    upgrade_temp = pd.pivot_table(
        space_df,
        values=['升舱标记', '订单编号'],
        index=['月份', '订单商品名'],
        aggfunc={
            '升舱标记': 'sum',
            '订单编号': 'count'
        },
        fill_value=0
    ).reset_index()
    
    upgrade_ratio = pd.pivot_table(
        upgrade_temp,
        values='升舱标记',
        index='月份',
        columns='订单商品名',
        aggfunc=lambda x: (x / upgrade_temp.loc[x.index, '订单编号'] * 100).round(1),
        fill_value=0
    ).reset_index()
    
    # 2. 加钟分析 - 时长总计表
    overtime_duration = pd.pivot_table(
        space_df[space_df['加钟数'] > 0],
        values='加钟数',
        index='月份',
        columns='订单商品名',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    # 加钟收入总计表
    overtime_revenue = pd.pivot_table(
        space_df[space_df['加钟数'] > 0],
        values='实付金额',
        index='月份',
        columns='订单商品名',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    # 3. 预约分析
    space_df['预约标记'] = (space_df['临时/预约'] == '预约').astype(int)
    booking_temp = pd.pivot_table(
        space_df,
        values=['预约标记', '订单编号'],
        index=['月份', '订单商品名'],
        aggfunc={
            '预约标记': 'sum',
            '订单编号': 'count'
        },
        fill_value=0
    ).reset_index()
    
    booking_analysis = pd.pivot_table(
        booking_temp,
        values='预约标记',
        index='月份',
        columns='订单商品名',
        aggfunc=lambda x: (x / booking_temp.loc[x.index, '订单编号'] * 100).round(1),
        fill_value=0
    ).reset_index()
    
    return {
        '升舱订单数_bar': upgrade_count,
        '升舱金额_bar': upgrade_amount,
        '升舱订单占比_bar': upgrade_ratio,
        '加钟收入分析_bar': overtime_revenue,
        '加钟时长分析_bar': overtime_duration,
        '预约分析_bar': booking_analysis
    }


def analyze_member(member_df: pd.DataFrame, db_path: str) -> Dict[str, pd.DataFrame]:
    """合并后的会员分析函数"""
    member_df = calculate_user_intervals(member_df)
    unique_months = sorted(member_df['booking_month'].unique())
    last_order_dates = member_df.groupby('会员号')['创建时间'].max().reset_index()
    first_order_dates = member_df.groupby('会员号')['创建时间'].min().reset_index()
    
    # 初始化结果列表
    monthly_metrics = []
    
    for period_month in unique_months:
        month_start = period_month.to_timestamp()
        month_end = period_month.to_timestamp('M')
        
        # 当前月数据
        current_month_orders = member_df[(member_df['创建时间'] >= month_start) & 
                                       (member_df['创建时间'] <= month_end)]
        active_users = current_month_orders['会员号'].nunique()
        
        # 新增用户
        new_users = len(first_order_dates[(first_order_dates['创建时间'] >= month_start) &
                                        (first_order_dates['创建时间'] <= month_end)]['会员号'].unique())
        
        # 流失相关
        churn_30_60 = len(last_order_dates[(last_order_dates['创建时间'] < month_end - timedelta(days=30)) &
                                         (last_order_dates['创建时间'] >= month_end - timedelta(days=60))]['会员号'].unique())
        churn_60_90 = len(last_order_dates[(last_order_dates['创建时间'] < month_end - timedelta(days=60)) &
                                         (last_order_dates['创建时间'] >= month_end - timedelta(days=90))]['会员号'].unique())
        churn_long = len(last_order_dates[last_order_dates['创建时间'] < month_end - timedelta(days=120)]['会员号'].unique())
        
        # 重新激活用户
        prev_users = member_df[(member_df['创建时间'] < month_start - timedelta(days=90)) &
                             (member_df['创建时间'] >= month_start - timedelta(days=180))]['会员号'].unique()
        reactivated = len(current_month_orders[current_month_orders['会员号'].isin(prev_users)]['会员号'].unique())
        
        # 计算回购用户
        past_users = member_df[member_df['创建时间'] < month_start]['会员号'].unique()
        current_orders = current_month_orders[current_month_orders['会员号'].isin(past_users)].sort_values('创建时间')
        repurchase_users = len(current_orders.groupby('会员号').filter(
            lambda x: (x['创建时间'].max() - x['创建时间'].min()).days >= 1
        )['会员号'].unique())
        
        monthly_metrics.append({
            'booking_month': str(period_month),
            '活跃用户': active_users,
            '新增用户': new_users,
            '流失预警_30_60天': churn_30_60,
            '流失预警_60_90天': churn_60_90,
            '长期未活跃_120天以上': churn_long,
            '重新激活用户': reactivated,
            '回购用户': repurchase_users
        })
    
    counts_df = pd.DataFrame(monthly_metrics)
    
    # 计算按月和会员等级的统计
    monthly_level_stats = member_df.groupby(['booking_month', '等级']).agg({
        '实付金额': ['mean', 'sum'],
        '订单编号': 'count',
        '会员号': 'nunique'
    }).reset_index()
    monthly_level_stats.columns = ['月份', '等级', '平均收入', '总收入', '订单量', '独立会员数量']
    
    # 生成统计表
    avg_revenue_table = pd.pivot_table(monthly_level_stats, values='平均收入', index='月份', columns='等级', fill_value=0).reset_index()
    total_revenue_table = pd.pivot_table(monthly_level_stats, values='总收入', index='月份', columns='等级', fill_value=0).reset_index()
    order_volume_table = pd.pivot_table(monthly_level_stats, values='订单量', index='月份', columns='等级', fill_value=0).reset_index()
    unique_members_table = pd.pivot_table(monthly_level_stats, values='独立会员数量', index='月份', columns='等级', fill_value=0).reset_index()
    
    # 计算一个月留存率、回购率及流失率
    rates_df = pd.DataFrame({'booking_month': counts_df['booking_month']})
    rates_df['一个月留存率'] = 0.0
    rates_df['回购率'] = counts_df['回购用户'] / counts_df['活跃用户'] * 100
    rates_df['30天流失率'] = counts_df['流失预警_30_60天'] / counts_df['活跃用户'].shift(1) * 100
    rates_df['60天流失率'] = counts_df['流失预警_60_90天'] / counts_df['流失预警_30_60天'].shift(1) * 100
    
    for i, month in enumerate(unique_months[1:], 1):
        prev_month = unique_months[i-1]
        prev_new_users = member_df[(member_df['booking_month'] == prev_month) & 
                                 (member_df['会员号'].isin(first_order_dates[
                                     (first_order_dates['创建时间'] >= prev_month.to_timestamp()) &
                                     (first_order_dates['创建时间'] <= prev_month.to_timestamp('M'))
                                 ]['会员号']))]['会员号'].unique()
        current_active = member_df[(member_df['booking_month'] == month) & 
                                 (member_df['会员号'].isin(prev_new_users))]['会员号'].nunique()
        rates_df.loc[i, '一个月留存率'] = current_active / len(prev_new_users) * 100 if len(prev_new_users) > 0 else 0
    
    # 处理NaN值
    counts_df = counts_df.fillna(0)
    rates_df = rates_df.fillna(0)
    
    # 返回结果字典
    return {
        '每月平均收入_bar': avg_revenue_table,
        '每月总收入_bar': total_revenue_table,
        '每月订单量_bar': order_volume_table,
        '每月独立会员数量_bar': unique_members_table,
        '用户留存与流失_bar': counts_df,
        '用户留存与流失率_bar': rates_df
    }


def analyze_users(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    # 设置时间范围
    today = space_df['创建时间'].max()
    begin_day = today - timedelta(days=180)  # 统计关注6个月的用户
    # 过滤过去6个月的数据
    df_filtered = space_df[space_df['创建时间'] >= begin_day].copy()
    
    # 按会员号聚合计算所需指标
    rfm_analysis = df_filtered.groupby('会员号').agg({
        '创建时间': 'max',  # 最近一次消费时间
        '实付金额': 'sum',  # 总消费金额
        '订单编号': 'count'  # 消费次数
    }).reset_index()
    
    # 重命名列
    rfm_analysis.columns = ['会员号', '最近消费时间', '总消费金额', '消费次数']
    rfm_analysis['Recency'] = (today - rfm_analysis['最近消费时间']).dt.days
    # 计算指数: e^(-λ * Recency), λ=0.015
    lambda_param = 0.015
    rfm_analysis['最近购买时间指数'] = np.exp(-lambda_param * rfm_analysis['Recency'])
    
    # 2. 计算Monetary指数
    rfm_analysis['消费力指数'] = np.log(rfm_analysis['总消费金额'] + 1)
    
    # 3. 计算Frequency指数
    rfm_analysis['消费频率指数'] = np.log(1 + rfm_analysis['消费次数'])

    result = rfm_analysis[['会员号', '最近购买时间指数', '消费力指数', '消费频率指数']]
    
    return {
        'RFM分析_bar': result
    }

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

def analyze_space(space_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """空间分析"""

    space_df = space_df.copy()
    # 过滤时间和商品名
    space_df = space_df[space_df['创建时间'] >= '2023-09-19']
    space_df = space_df[~space_df['订单商品名'].isin(['丛林小剧院', '丛林心流舱'])]

    # 确保日期时间字段格式正确
    date_columns = ['创建时间', '预定开始时间', '预定结束时间']
    for col in date_columns:
        if col in space_df.columns and not pd.api.types.is_datetime64_any_dtype(space_df[col]):
            space_df[col] = pd.to_datetime(space_df[col], errors='coerce')
    
    # 定义有效时间段和每日可用时长
    def get_valid_time_range(product_name):
        if isinstance(product_name, str) and '心流舱' in product_name:
            return (time(7, 0), time(23, 59, 59))  # 7:00 - 24:00 (17小时)
        return (time(9, 0), time(20, 0))  # 9:00 - 20:00 (11小时)
    
    def get_daily_hours(product_name):
        if isinstance(product_name, str) and '心流舱' in product_name:
            return 17  # 7:00 - 24:00 (17小时)
        return 11  # 9:00 - 20:00 (11小时)

    # 高峰时段分析
    peak_analysis = space_df.groupby('开始使用时刻').agg({
        '订单编号': 'count',
        '实付金额': 'sum'
    }).reset_index()
    total_revenue = peak_analysis['实付金额'].sum()
    total_orders = peak_analysis['订单编号'].sum()
    peak_analysis['收入占比'] = np.where(total_revenue == 0, 0, peak_analysis['实付金额'] / total_revenue * 100)
    peak_analysis['订单占比'] = np.where(total_orders == 0, 0, peak_analysis['订单编号'] / total_orders * 100)
    peak_analysis.columns = ['开始使用时刻', '订单数', '收入', '收入占比', '订单占比']

    # 周度分析
    space_df['booking_week'] = space_df['创建时间'].dt.to_period('W-MON').apply(lambda x: x.start_time.date())
    
    results = {'高峰时段分析_bar': peak_analysis}

    # 基本指标分析 (订单数, 总收入, 总使用时长, 平均使用时长)
    for metric in ['订单数', '总收入', '总使用时长', '平均使用时长', '利用率']:
        try:
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
                # 利用率计算 - 每周每个空间类型的利用率
                weekly_products = []
                
                for (week, product_name), group in space_df.groupby(['booking_week', '订单商品名']):
                    daily_hours = get_daily_hours(product_name)
                    max_weekly_hours = daily_hours * 7
                    actual_hours = group['实际时长'].sum()
                    
                    # 计算利用率(%)
                    utilization = min(100, (actual_hours / max_weekly_hours) * 100) if max_weekly_hours > 0 else 0

                    weekly_products.append({
                        '周': week,
                        '空间类型': product_name,
                        '利用率': utilization
                    })
                
                df = pd.DataFrame(weekly_products)
            
            # 转换周为字符串格式
            df['周'] = df['周'].astype(str)
            
            # 透视表转换结果
            pivot_df = df.pivot(index='周', columns='空间类型', values=df.columns[-1])
            results[f'空间{metric}分析_bar'] = pivot_df.reset_index().rename_axis(None, axis=1)
        
        except Exception as e:
            print(f"Error in '{metric}' calculation: {str(e)}")
            # 如果出错，创建一个空的DataFrame作为结果
            results[f'空间{metric}分析_bar'] = pd.DataFrame(columns=['周'])

    # 周度日内使用率_bar (按分钟计算使用率)
    try:
        # 按周整理数据并计算
        hourly_usage_data = []
        
        for week, week_df in space_df.groupby('booking_week'):
            # 获取该周的所有日期
            week_start = pd.Timestamp(week)
            week_dates = [week_start + timedelta(days=i) for i in range(7)]
            # 收集有订单的商品类型
            products = week_df['订单商品名'].unique()
            
            # 遍历每个小时 (7-23点)
            for hour in range(7, 24):
                product_usage = {}
                valid_products = 0
                
                # 对每种商品分别计算
                for product in products:
                    # 检查该小时是否在商品的有效时间范围内
                    valid_start, valid_end = get_valid_time_range(product)
                    if hour < valid_start.hour or hour > valid_end.hour:
                        continue
                    
                    # 找出该商品在该周的所有订单
                    product_orders = week_df[week_df['订单商品名'] == product]
                    
                    # 计算该商品在这个小时的总使用分钟
                    total_minutes_used = 0
                    
                    # 遍历该周的每一天
                    for date in week_dates:
                        # 当天该小时的开始和结束时间
                        hour_start = pd.Timestamp(date.year, date.month, date.day, hour, 0, 0)
                        hour_end = pd.Timestamp(date.year, date.month, date.day, hour, 59, 59)
                        
                        # 遍历订单，检查是否有订单在这个时间段内
                        for _, order in product_orders.iterrows():
                            start_time = order['预定开始时间']
                            end_time = order['预定结束时间']
                            
                            # 检查订单是否与当前小时有重叠
                            if start_time <= hour_end and end_time >= hour_start:
                                # 计算重叠的分钟数
                                overlap_start = max(start_time, hour_start)
                                overlap_end = min(end_time, hour_end)
                                overlap_minutes = (overlap_end - overlap_start).total_seconds() / 60
                                total_minutes_used += overlap_minutes
                    
                    # 该商品这个小时在整周的总可用分钟 = 60分钟 * 7天
                    total_available_minutes = 60 * 7
                    
                    # 计算使用率
                    usage_rate = min(100, (total_minutes_used / total_available_minutes) * 100)
                    product_usage[product] = usage_rate
                    valid_products += 1
                
                # 所有有效商品的平均使用率
                avg_usage = sum(product_usage.values()) / valid_products if valid_products > 0 else 0
                
                hourly_usage_data.append({
                    '周': str(week),
                    '小时': f"{hour}点",
                    '使用率': avg_usage
                })
        
        # 创建DataFrame并透视
        hourly_df = pd.DataFrame(hourly_usage_data)
        if not hourly_df.empty:
            hourly_pivot = hourly_df.pivot(index='周', columns='小时', values='使用率').fillna(0)
            results['周度日内使用率_bar'] = hourly_pivot.reset_index()
        else:
            # 空结果
            results['周度日内使用率_bar'] = pd.DataFrame(columns=['周'])
    
    except Exception as e:
        print(f"Error in hourly utilization calculation: {str(e)}")
        results['周度日内使用率_bar'] = pd.DataFrame(columns=['周'])

    # 周内使用率_bar (按星期几分析)
    try:
        # 添加星期和月份字段
        space_df['weekday'] = space_df['创建时间'].dt.day_name()
        weekday_map = {
            'Monday': '周一', 'Tuesday': '周二', 'Wednesday': '周三', 
            'Thursday': '周四', 'Friday': '周五', 'Saturday': '周六', 'Sunday': '周日'
        }
        space_df['weekday'] = space_df['weekday'].map(weekday_map)
        weekday_order = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
        
        space_df['booking_month'] = space_df['创建时间'].dt.to_period('M')
        
        # 周内使用率数据
        weekday_usage_data = []
        
        for month, month_df in space_df.groupby('booking_month'):
            month_str = str(month)
            month_data = {'月份': month_str}
            
            # 获取该月每个星期的天数
            month_start = pd.Period(month).start_time
            month_end = pd.Period(month).end_time
            days_in_month = pd.date_range(start=month_start, end=month_end)
            weekday_counts = {day: 0 for day in weekday_order}
            
            for day in days_in_month:
                day_name = weekday_map.get(day.day_name())
                if day_name:
                    weekday_counts[day_name] += 1
            
            # 计算每个星期的使用率
            for weekday in weekday_order:
                # 筛选该星期的数据
                weekday_df = month_df[month_df['weekday'] == weekday]
                
                if weekday_df.empty or weekday_counts[weekday] == 0:
                    month_data[weekday] = 0
                    continue
                
                # 收集该星期不同商品的使用率
                product_usages = []
                products = weekday_df['订单商品名'].unique()
                
                for product in products:
                    # 筛选该商品的订单
                    product_df = weekday_df[weekday_df['订单商品名'] == product]
                    
                    # 计算该商品在该星期的使用率
                    daily_hours = get_daily_hours(product)
                    total_available_hours = daily_hours * weekday_counts[weekday]
                    total_used_hours = product_df['实际时长'].sum()
                    
                    # 计算使用率
                    usage_rate = min(100, (total_used_hours / total_available_hours) * 100) if total_available_hours > 0 else 0
                    product_usages.append(usage_rate)
                
                # 所有商品的平均使用率
                avg_usage = sum(product_usages) / len(product_usages) if product_usages else 0
                month_data[weekday] = avg_usage
            
            weekday_usage_data.append(month_data)
        
        # 创建DataFrame
        weekday_df = pd.DataFrame(weekday_usage_data)
        if not weekday_df.empty:
            # 确保列顺序正确 (月份, 周一, 周二, ...)
            column_order = ['月份'] + weekday_order 
            weekday_df = weekday_df.reindex(columns=column_order, fill_value=0)
            results['周内使用率_bar'] = weekday_df
        else:
            # 空结果
            empty_df = pd.DataFrame(columns=['月份'] + weekday_order)
            results['周内使用率_bar'] = empty_df
    
    except Exception as e:
        print(f"Error in weekday utilization calculation: {str(e)}")
        results['周内使用率_bar'] = pd.DataFrame(columns=['月份'] + ['周一', '周二', '周三', '周四', '周五', '周六', '周日'])

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
        space_df = space_df[space_df['等级'] != '0.0']

        space_df['订单商品名'] = space_df['订单商品名'].fillna('').str.replace('上海洛克外滩店-', '').str.replace('the Box', '')
        space_df['订单商品名'] = space_df['订单商品名'].apply(
            lambda x: '图书馆专注区' if x == '图书馆专注' else x
        )
        # 预处理数据，不填充数值列的 NaN，保留为 None
        space_df = preprocess_datetime(space_df)
        space_df['booking_month'] = space_df['创建时间'].dt.to_period('M')
        space_df['开始使用时刻'] = pd.to_datetime(space_df['预定开始时间'], errors='coerce').dt.hour

        order_results = analyze_order(space_df)
        member_results = analyze_member(space_df, conn)
        user_results = analyze_users(space_df)
        financial_results = analyze_weekly_finance(space_df)
        space_results = analyze_space(space_df)

        all_results = {
            '财务数据': financial_results,
            '订单分析': order_results,
            '会员分析': member_results,
            '用户分析': user_results,
            '空间具体分析': space_results
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
