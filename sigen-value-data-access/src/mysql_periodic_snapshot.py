import pymysql
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
import io
import os
import logging
import argparse
import schedule
import time
import re

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MySQL 配置
mysql_config = {
    'host': os.getenv('MYSQL_HOST', 'svc-dev.sigencloud.com'),
    'user': os.getenv('MYSQL_USER', 'developer'),
    'password': os.getenv('MYSQL_PASSWORD', 'mnbv!@#qaz'),
    'database': os.getenv('MYSQL_DATABASE', 'sigen_device')
}

# S3 配置
s3_config = {
    'bucket': os.getenv('S3_BUCKET', 'sigen-mysql-valued-data-snapshot'),
    'aws_access_key': os.getenv('AWS_ACCESS_KEY', 'AKIA2OWZZVYFXKB2XGY7'),
    'aws_secret_key': os.getenv('AWS_SECRET_KEY', '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'),
    'aws_region': os.getenv('AWS_REGION', 'cn-northwest-1')
}

# StarRocks 配置
starrocks_config = {
    'host': 'svc-dev.sigencloud.com',
    'port': 9030,
    'user': 'root',
    'password': 'ZxyzWMgw@WnhmThF',
    'database': 'sigen_ai',
    'charset': 'utf8mb4'
}


def get_timestamp():
    return datetime.now().strftime("%Y%m%d")


def export_table_data(table_name, selected_columns):
    """导出指定表和字段的数据"""
    connection = None
    try:
        logging.info(f"开始连接到MySQL数据库并导出表 {table_name}")
        connection = pymysql.connect(**mysql_config)

        query = f"SELECT {selected_columns} FROM {table_name};"
        logging.info(f"执行SQL查询: {query}")

        # 使用pandas读取数据
        df = pd.read_sql(query, connection)
        logging.info(f"获取到 {len(df)} 行数据")

        # 将pandas DataFrame转换为PyArrow Table
        table = pa.Table.from_pandas(df)

        return table
    except Exception as e:
        logging.error(f"导出数据时发生错误: {e}")
        raise
    finally:
        if connection:
            connection.close()


def upload_to_s3(table, table_name):
    """将数据表上传至S3"""
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=s3_config['aws_access_key'],
            aws_secret_access_key=s3_config['aws_secret_key'],
            region_name=s3_config['aws_region']
        )

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='gzip')
        buffer.seek(0)

        timestamp = get_timestamp()
        s3_key = f"{table_name}/{table_name}_snapshot_{timestamp}.parquet"

        s3.put_object(
            Bucket=s3_config['bucket'],
            Key=s3_key,
            Body=buffer.getvalue()
        )
        logging.info(f"Parquet 快照已上传至 s3://{s3_config['bucket']}/{s3_key}")
        return s3_key
    except Exception as e:
        logging.error(f"上传到S3时发生错误: {e}")
        raise


def pa_type_to_starrocks_type(pa_type):
    if pa.types.is_int64(pa_type):
        return 'BIGINT'
    elif pa.types.is_int32(pa_type):
        return 'INT'
    elif pa.types.is_int8(pa_type):
        return 'TINYINT'
    elif pa.types.is_float32(pa_type):
        return 'FLOAT'
    elif pa.types.is_float64(pa_type):
        return 'DOUBLE'
    elif pa.types.is_decimal(pa_type):
        return f'DECIMAL({pa_type.precision},{pa_type.scale})'
    elif pa.types.is_timestamp(pa_type):
        return 'DATETIME'
    elif pa.types.is_date32(pa_type):
        return 'DATE'
    elif pa.types.is_string(pa_type):
        return 'STRING'
    else:
        return 'STRING'  # 默认类型


def create_external_table(s3_key, table_name, schema):
    """在StarRocks创建外部表"""
    try:
        starrocks_connection = pymysql.connect(**starrocks_config)
        starrocks_cursor = starrocks_connection.cursor()

        table_archived = f"{table_name}_snapshot_{get_timestamp()}"

        drop_table_query = f"DROP TABLE IF EXISTS {table_archived};"
        starrocks_cursor.execute(drop_table_query)
        logging.info(f"已尝试删除可能存在的表 {table_archived}")

        columns_definition = []
        for field in schema:
            sr_type = pa_type_to_starrocks_type(field.type)
            columns_definition.append(f"{field.name} {sr_type}")

        columns_definition_str = ",\n".join(columns_definition)
        latest_full_data_path = f"s3://{s3_config['bucket']}/{s3_key}"

        create_table_query = f"""
        CREATE EXTERNAL TABLE {table_archived} (
            {columns_definition_str}
        )
        ENGINE=file
        PROPERTIES (
            "path" = "{latest_full_data_path}",
            "format" = "parquet",
            "aws.s3.access_key" = "{s3_config['aws_access_key']}",
            "aws.s3.secret_key" = "{s3_config['aws_secret_key']}",
            "aws.s3.region" = "{s3_config['aws_region']}"
        );
        """

        logging.info(f"创建外部表的SQL语句:\n{create_table_query}")
        starrocks_cursor.execute(create_table_query)
        logging.info(f"外部表 {table_archived} 创建成功")

        starrocks_cursor.close()
        starrocks_connection.close()

        return table_archived
    except Exception as e:
        logging.error(f"创建外部表时发生错误: {e}")
        raise


def main(table_name, selected_columns):
    """主流程：导出数据、上传到S3，并在StarRocks创建外部表"""
    table = export_table_data(table_name, selected_columns)
    s3_key = upload_to_s3(table, table_name)
    create_external_table(s3_key, table_name, table.schema)
    logging.info("数据导出、上传和外部表创建过程成功完成")


def scheduled_job():
    """定时任务，执行快照操作"""
    table_name = "sigen_device.station_info"  # 定时任务中的表名
    selected_columns = "station_id, pv_capacity, area_code, latitude, longitude, time_zone"  # 定时任务中的字段
    main(table_name, selected_columns)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="导出MySQL数据到S3")
    parser.add_argument("table_name", nargs="?", help="表名")
    parser.add_argument("selected_columns", nargs="?", help="要导出的字段，用逗号分隔")
    parser.add_argument("--schedule", action="store_true", help="以定时模式运行")

    args = parser.parse_args()

    if args.schedule:
        schedule.every().day.at("02:00").do(scheduled_job)
        logging.info("定时任务已设置，将在每天02:00执行")
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        if args.table_name and args.selected_columns:
            main(args.table_name, args.selected_columns)
        else:
            logging.error("请提供表名和字段列表，或者使用 --schedule 启用定时任务模式。")
