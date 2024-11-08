# -*- coding: utf-8 -*-
import sys
import pymysql
import os
import boto3
from datetime import datetime, timedelta
import re

# StarRocks 配置信息
starrocks_config = {
    'host': 'svc-dev.sigencloud.com',
    'port': 9030,
    'user': 'root',
    'password': 'ZxyzWMgw@WnhmThF',
    'database': 'sigen_ai',
    'charset': 'utf8mb4'  # 确保数据库连接使用UTF-8编码
}

# S3 配置信息
s3_config = {
    'bucket': 'sigen-starrocks-valued-data-archive',  # 归档桶名
    'aws_access_key': 'AKIA2OWZZVYFXKB2XGY7',
    'aws_secret_key': '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',
    'aws_region': 'cn-northwest-1'
}

def get_current_timestamp():
    return datetime.now().strftime('%Y%m%d_%H%M%S')

def get_table_columns(table_name):
    """获取指定表的所有列名"""
    try:
        connection = pymysql.connect(**starrocks_config)
        cursor = connection.cursor()

        cursor.execute(f"DESCRIBE {table_name};")
        columns = [row[0] for row in cursor.fetchall()]

        return columns
    except Exception as e:
        raise ValueError(f"无法获取表 {table_name} 的列信息: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def validate_columns(input_columns, table_columns):
    """验证用户输入的字段是否存在于表中"""
    invalid_columns = [col for col in input_columns if col not in table_columns]
    if invalid_columns:
        raise ValueError(f"无效的字段: {', '.join(invalid_columns)}。这些字段不在表中。")

def get_column_type(cursor, table_name, column_name):
    """使用 DESCRIBE 查询来获取字段类型"""
    cursor.execute(f"DESCRIBE {table_name};")
    columns_info = cursor.fetchall()

    # 遍历结果，找到对应的字段名及其类型
    for column_info in columns_info:
        col_name, col_type = column_info[0], column_info[1]
        if col_name == column_name:
            print(f"Matched column type for '{column_name}': {col_type}")
            return col_type  # 返回直接的字段类型

    raise ValueError(f"无法在表 {table_name} 中找到列 {column_name} 的类型信息")

def get_latest_full_data_path(table_name):
    """获取 S3 中指定表的最新 full_data 路径"""
    s3 = boto3.client('s3',
                      aws_access_key_id=s3_config['aws_access_key'],
                      aws_secret_access_key=s3_config['aws_secret_key'],
                      region_name=s3_config['aws_region'])

    bucket_name = s3_config['bucket']
    prefix = f"{table_name}/init/full_data_"

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        latest = max(response['Contents'], key=lambda x: x['Key'])
        # 返回目录路径，而不是具体文件路径
        latest_path = f"s3://{bucket_name}/{'/'.join(latest['Key'].split('/')[:-1])}/"
        print(f"Using latest S3 data path: {latest_path}")
        return latest_path
    else:
        raise ValueError("No full data found in S3")

def save_config_to_s3(table_name, partition_column, selected_columns, retention_partitions):
    s3 = boto3.client('s3',
                      aws_access_key_id=s3_config['aws_access_key'],
                      aws_secret_access_key=s3_config['aws_secret_key'],
                      region_name=s3_config['aws_region'])

    config_content = f"""表名: {table_name}
分区字段名: {partition_column}
选择的价值数据字段名: {', '.join(selected_columns)}
保留分区数: {retention_partitions}
"""

    config_key = f"{table_name}/config.txt"
    s3.put_object(Bucket=s3_config['bucket'], Key=config_key, Body=config_content)
    print(f"配置信息已保存到 s3://{s3_config['bucket']}/{config_key}")

def create_external_table(cursor, table_name, selected_columns):
    try:
        table_archived = f"{table_name}_archived"
        print(f"Creating external table: {table_archived}")

        # 删除外部表（如果存在）
        drop_table_sql = f"DROP TABLE IF EXISTS {table_archived};"
        cursor.execute(drop_table_sql)
        print(f"表 {table_archived} 已删除（如果存在）。")

        # 获取最新的 S3 数据路径
        latest_full_data_path = get_latest_full_data_path(table_name)

        # 使用 DESCRIBE 查询列定义
        columns_definition = []
        for col in selected_columns:
            col_type = get_column_type(cursor, table_name, col)
            columns_definition.append(f"{col} {col_type}")

        columns_definition_str = ', '.join(columns_definition)

        # 创建外部表的 SQL
        create_table_sql = f"""
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

        print("SQL statement for creating external table:")
        print(create_table_sql)

        # 执行创建外部表
        cursor.execute(create_table_sql)
        print(f"外部表 {table_archived} 已创建，数据路径为 {latest_full_data_path}.")

    except Exception as e:
        print(f"在创建外部表时发生错误: {e}")
        raise

def archive_table_to_s3(table_name, selected_columns, partition_column):
    try:
        print("\n你选择的表名是:", table_name)
        print("你选择的字段是:", ', '.join(selected_columns))
        print("分区字段是:", partition_column)

        connection = pymysql.connect(**starrocks_config)
        cursor = connection.cursor()

        cursor.execute(f"SELECT DISTINCT {partition_column} FROM {table_name};")
        partitions = [row[0] for row in cursor.fetchall()]

        s3_base_path = f's3://{s3_config["bucket"]}/{table_name}/init/'

        for partition_value in partitions:
            partition_path = f"{partition_column}-{partition_value}"
            s3_path = f'{s3_base_path}{partition_path}/'

            columns_str = ', '.join(selected_columns)
            sql = f"""
                        INSERT INTO FILES
                        (
                            "path" = "{s3_path}",
                            "format" = "parquet",
                            "compression" = "zstd",
                            "target_max_file_size" = "209715200",
                            "aws.s3.access_key" = "{s3_config['aws_access_key']}",
                            "aws.s3.secret_key" = "{s3_config['aws_secret_key']}",
                            "aws.s3.region" = "{s3_config['aws_region']}"
                        )
                        SELECT {columns_str}
                        FROM {table_name}
                        WHERE {partition_column} = '{partition_value}';
                        """
            print("生成的 SQL 语句:")
            print(sql)

            cursor.execute(sql)
            connection.commit()

            print(f"分区 {partition_value} 的数据成功归档到 {s3_path}!")

        current_timestamp = get_current_timestamp()
        full_data_s3_path = f'{s3_base_path}full_data_{current_timestamp}/'
        os.system(f"aws s3 rm {s3_base_path}full_data_ --recursive")

        full_data_sql = f"""
            INSERT INTO FILES
            (
                "path" = "{full_data_s3_path}",
                "format" = "parquet",
                "compression" = "zstd",
                "target_max_file_size" = "209715200",
                "aws.s3.access_key" = "{s3_config['aws_access_key']}",
                "aws.s3.secret_key" = "{s3_config['aws_secret_key']}",
                "aws.s3.region" = "{s3_config['aws_region']}"
            )
            SELECT {columns_str}
            FROM {table_name};
        """
        cursor.execute(full_data_sql)
        connection.commit()
        print(f"全量数据成功归档到 {full_data_s3_path}!")

        create_external_table(cursor, table_name, selected_columns)

        retention_partitions = 180
        save_config_to_s3(table_name, partition_column, selected_columns, retention_partitions)

    except Exception as e:
        print(f"发生错误: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python initial_archive.py <table_name> <partition_column> <selected_columns>")
        sys.exit(1)

    table_name = sys.argv[1]
    partition_column = sys.argv[2]
    selected_columns = sys.argv[3].split(',')

    archive_table_to_s3(table_name, selected_columns, partition_column)
