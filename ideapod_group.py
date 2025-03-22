import json
import sqlite3
from collections import defaultdict
import datetime
import pandas as pd
import logging
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db/ideapod_group.log')
    ]
)

def preprocess_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Central datetime preprocessing to reduce redundant operations"""
    datetime_columns = [
        '创建时间', '预定开始时间', '预定结束时间',  # Space
        '下单时间',  # Catering
    ]
    for col in datetime_columns:
        if col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except Exception as e:
                    logging.error(f"转换 {col} 列时出错：{e}")
    return df

def convert_df_to_dict(data):
    """Convert pandas DataFrame objects to dictionaries suitable for JSON serialization."""
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
    elif isinstance(data, (pd.Timestamp, pd.Period, datetime.date)) or pd.isna(data):
        return str(data) if not pd.isna(data) else None
    return data

def analyze_finance(space_df: pd.DataFrame, catering_df: pd.DataFrame) -> dict:
    """周度和日度财务分析"""
    
    # Helper function to categorize incomes based on remarks
    def categorize_incomes(row):
        remark = str(row.get('订单备注', '')).lower()
        amount = row['吧台场景收入']
        monthly = amount if '月结' in remark else 0
        welfare = amount if '最福利' in remark else 0
        shooting = amount if '拍摄' in remark else 0
        return monthly, welfare, shooting

    # Daily analysis
    daily_catering = catering_df.groupby('订单日').agg(
        餐饮实收=('实收', 'sum')
    ).reset_index()
    
    daily_space = space_df.copy()
    # Apply categorization for all three conditions
    daily_space[['月结金额', '最福利金额', '拍摄金额']] = daily_space.apply(
        lambda row: pd.Series(categorize_incomes(row)), axis=1
    )
    
    # Aggregate daily data
    daily_categorized = daily_space.groupby('订单日').agg({
        '实付金额': 'sum',
        '吧台场景收入': 'sum',
        '大众点评场景收入': 'sum',
        '月结金额': 'sum',
        '最福利金额': 'sum',
        '拍摄金额': 'sum'
    }).reset_index()
    
    daily_categorized.columns = ['订单日', '场景毛收入', '吧台场景收入', '大众点评场景收入', 
                                '月结收入', '最福利场景收入', '拍摄收入']
    
    daily_data = daily_catering.merge(daily_categorized, on='订单日', how='outer').fillna(0)
    
    # 读取书玉的数据并合并到 daily_data
    try:
        external_data = pd.read_csv('db/shuyu_data.csv')
        # 确保日期格式一致
        external_data['日期'] = pd.to_datetime(external_data['日期'], format='%m/%d/%y', errors='coerce')
        daily_data['订单日'] = pd.to_datetime(daily_data['订单日'], errors='coerce')
      
        # 合并数据
        daily_data = daily_data.merge(
            external_data[['日期', '智能货柜收入', '最福利餐饮收入']],
            left_on='订单日',
            right_on='日期',
            how='outer'
        ).drop('日期', axis=1).fillna(0)
        
    except FileNotFoundError:
        logging.warning("shuyu_data.csv not found. Proceeding without external data.")
        daily_data['智能货柜收入'] = 0
        daily_data['最福利餐饮收入'] = 0
    except KeyError as e:
        logging.error(f"Error: {e}. Check if '智能货柜收入' and '最福利餐饮收入' exist in shuyu_data.csv.")
        daily_data['智能货柜收入'] = 0
        daily_data['最福利餐饮收入'] = 0

    # Calculate adjusted incomes
    daily_data['吧台调整场景收入'] = (daily_data['吧台场景收入'] - 
                                     daily_data['月结收入'] - 
                                     daily_data['最福利场景收入'])
    daily_data['吧台纯餐饮收入'] = (daily_data['餐饮实收'] - 
                                   daily_data['吧台调整场景收入'] - 
                                   daily_data['拍摄收入'])
    daily_data['场景收入'] = daily_data['场景毛收入'] - daily_data['月结收入']
    daily_data['餐饮收入'] = daily_data['吧台纯餐饮收入'] + daily_data['智能货柜收入'] + daily_data['最福利餐饮收入']
    
    daily_result = daily_data[['订单日', '餐饮收入', '场景收入', '餐饮实收','吧台纯餐饮收入', '吧台场景收入', 
                            '吧台调整场景收入','大众点评场景收入', '月结收入','智能货柜收入','最福利餐饮收入', '最福利场景收入', '拍摄收入'
                            ]].to_dict(orient='records')

    # Weekly analysis
    # 从 daily_data 中提取需要加总的列，并按 '订单周' 分组
    weekly_data = daily_data.copy()
    weekly_data['订单周'] = pd.to_datetime(weekly_data['订单日'], errors='coerce').dt.to_period('W-MON').apply(lambda x: x.start_time.date())
    
    weekly_data = weekly_data.groupby('订单周').agg({
        '餐饮收入': 'sum',
        '场景收入': 'sum',
        '餐饮实收': 'sum',
        '吧台场景收入': 'sum',
        '大众点评场景收入': 'sum',
        '月结收入': 'sum',
        '最福利场景收入': 'sum',
        '拍摄收入': 'sum',
        '吧台调整场景收入': 'sum',
        '吧台纯餐饮收入': 'sum',
        '智能货柜收入': 'sum',
        '最福利餐饮收入': 'sum'
    }).reset_index()
    
    weekly_result = weekly_data[['订单周', '餐饮收入', '场景收入', '餐饮实收','吧台纯餐饮收入', '吧台场景收入', 
                            '吧台调整场景收入','大众点评场景收入', '月结收入','智能货柜收入','最福利餐饮收入', '最福利场景收入', '拍摄收入'
                            ]].to_dict(orient='records')
    
    # Fixed: Using datetime.datetime instead of just datetime
    cutoff_date_space = datetime.datetime.strptime("2024-04-30", "%Y-%m-%d")
    cutoff_date_catering = datetime.datetime.strptime("2023-11-06", "%Y-%m-%d")    
    
    # Trailing 4 weeks analysis
    trailing_data = []
    for i in range(len(weekly_result)):
        space_trailing_4 = sum(d['场景收入'] for d in weekly_result[max(0, i-3):i+1])
        catering_trailing_4 = sum(d['餐饮收入'] for d in weekly_result[max(0, i-3):i+1])
        
        week_trailing = {
            "周": str(weekly_result[i]['订单周']),
            "场景_trailing_4_week收入": space_trailing_4,
            "餐饮_trailing_4_week收入": catering_trailing_4
        }

        # Fixed: Using datetime.datetime instead of just datetime
        week_date = datetime.datetime.strptime(week_trailing["周"], "%Y-%m-%d")
        
        # WoW calculation
        if i > 0:
            prev_space = trailing_data[i-1]["场景_trailing_4_week收入"]
            prev_catering = trailing_data[i-1]["餐饮_trailing_4_week收入"]
            
            # Avoid division by zero
            space_wow = (space_trailing_4 / prev_space - 1) * 100 if prev_space != 0 else 0
            week_trailing["场景_wow"] = 0 if week_date <= cutoff_date_space else space_wow
            
            catering_wow = (catering_trailing_4 / prev_catering - 1) * 100 if prev_catering != 0 else 0
            week_trailing["餐饮_wow"] = 0 if week_date <= cutoff_date_catering else catering_wow
        else:
            week_trailing["场景_wow"] = 0
            week_trailing["餐饮_wow"] = 0
        
        # MoM calculation
        if i >= 4:
            prev_4_space = trailing_data[i-4]["场景_trailing_4_week收入"]
            prev_4_catering = trailing_data[i-4]["餐饮_trailing_4_week收入"]
            
            # Avoid division by zero
            space_mom = (space_trailing_4 / prev_4_space - 1) * 100 if prev_4_space != 0 else 0
            week_trailing["场景_mom"] = 0 if week_date <= cutoff_date_space else space_mom
            
            catering_mom = (catering_trailing_4 / prev_4_catering - 1) * 100 if prev_4_catering != 0 else 0
            week_trailing["餐饮_mom"] = 0 if week_date <= cutoff_date_catering else catering_mom
        else:
            week_trailing["场景_mom"] = 0
            week_trailing["餐饮_mom"] = 0
        
        trailing_data.append(week_trailing)
    
    wow_result = [{"周": w["周"], "场景周环比": w["场景_wow"], "餐饮周环比": w["餐饮_wow"]} for w in trailing_data]
    mom_result = [{"周": w["周"], "场景月环比": w["场景_mom"], "餐饮月环比": w["餐饮_mom"]} for w in trailing_data]

    # Output structure
    output_data = {
        "周度销售收入_stacked": weekly_result,
        "过去四周收入周环比(%)_line": wow_result,
        "过去四周收入月环比(%)_line": mom_result,
        "日度销售收入_table": daily_result
    }

    return {'集团财务': output_data}

def analyze(conn):
    """主分析函数"""
    try:
        catering_df = pd.read_sql_query("SELECT * FROM Catering", conn)
        space_df = pd.read_sql_query("SELECT * FROM Space", conn)
       
        # Filter data
        catering_df = catering_df[catering_df['服务方式'] != '报损']
        
        # 内部用户付费的也算外部消费
        # space_df = space_df[space_df['等级'] != 'ideapod']

        catering_df = catering_df[catering_df['赠送'] == 0]
        
        # Preprocess datetime
        catering_df = preprocess_datetime(catering_df)
        space_df = preprocess_datetime(space_df)
        
        # Add date columns
        catering_df['订单月'] = catering_df['下单时间'].dt.to_period('M')
        catering_df['订单周'] = catering_df['下单时间'].dt.to_period('W-MON').apply(lambda x: x.start_time.date())
        catering_df['订单日'] = catering_df['下单时间'].dt.date
        space_df['订单月'] = space_df['预定开始时间'].dt.to_period('M')
        space_df['订单周'] = space_df['预定开始时间'].dt.to_period('W-MON').apply(lambda x: x.start_time.date())
        space_df['订单日'] = space_df['预定开始时间'].dt.date

        financial_results = analyze_finance(space_df, catering_df)

        # Process results for JSON serialization
        processed_results = {}
        for category, data in financial_results.items():
            processed_results[category] = {key: convert_df_to_dict(value) for key, value in data.items()}

        return processed_results

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        return {'error': f"Database error: {e}"}
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return {'error': f"An error occurred: {e}"}