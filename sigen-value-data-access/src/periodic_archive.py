import boto3
import pymysql
import schedule
import time
from datetime import datetime, timedelta
import os
import logging
import sys

# StarRocks 配置信息
starrocks_config = {
    'host': 'svc-dev.sigencloud.com',
    'port': 9030,
    'user': 'root',
    'password': 'ZxyzWMgw@WnhmThF',
    'database': 'sigen_ai'
}

# S3 配置信息
s3_config = {
    'bucket': 'sigen-starrocks-valued-data-archive',
    'aws_access_key': 'AKIA2OWZZVYFXKB2XGY7',
    'aws_secret_key': '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',
    'aws_region': 'cn-northwest-1'
}

# 配置日志
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 文件日志
file_handler = logging.FileHandler('periodic_archive.log', encoding='gbk')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# 控制台日志
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

def get_tables_to_archive():
    logging.info("开始获取需要归档的表列表")
    s3 = boto3.client('s3',
                      aws_access_key_id=s3_config['aws_access_key'],
                      aws_secret_access_key=s3_config['aws_secret_key'],
                      region_name=s3_config['aws_region'])

    response = s3.list_objects_v2(Bucket=s3_config['bucket'], Delimiter='/')
    tables = [prefix['Prefix'].rstrip('/') for prefix in response.get('CommonPrefixes', [])]
    logging.info(f"获取到的表列表：{tables}")
    return tables


def read_config(table_name):
    logging.info(f"正在读取表 {table_name} 的配置信息")
    s3 = boto3.client('s3',
                      aws_access_key_id=s3_config['aws_access_key'],
                      aws_secret_access_key=s3_config['aws_secret_key'],
                      region_name=s3_config['aws_region'])

    config_key = f"{table_name}/config.txt"

    try:
        response = s3.get_object(Bucket=s3_config['bucket'], Key=config_key)
        config_content = response['Body'].read().decode('utf-8')

        config = {}
        for line in config_content.split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                config[key.strip()] = value.strip()

        retention_days = int(config.get('保留天数', 180))
        partition_column = config.get('分区字段名', '')

        if not partition_column:
            logging.warning(f"警告：表 {table_name} 的配置文件中未指定分区字段名，将使用默认值 'date'。")
            partition_column = 'date'  # 设置一个默认值

        logging.info(f"表 {table_name} 的配置：保留天数 = {retention_days}, 分区字段 = {partition_column}")
        return retention_days, partition_column

    except Exception as e:
        logging.warning(f"警告：无法读取表 {table_name} 的配置或配置无效。使用默认值：保留180天，分区字段为 'date'。")
        return 180, 'date'  # 默认值


def get_partition_count(cursor, table_name, partition_column):
    logging.info(f"正在获取表 {table_name} 的分区数量")
    cursor.execute(f"SELECT COUNT(DISTINCT {partition_column}) FROM {table_name}")
    count = cursor.fetchone()[0]
    logging.info(f"表 {table_name} 当前的分区数量：{count}")
    return count


def archive_oldest_partition(cursor, table_name, partition_column):
    logging.info(f"开始归档表 {table_name} 的最旧分区")
    cursor.execute(f"SELECT MIN({partition_column}) FROM {table_name}")
    oldest_partition = cursor.fetchone()[0]

    if oldest_partition:
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
        s3_archive_path = f"s3://{s3_config['bucket']}/{table_name}/init/full_data_{current_time}/"

        logging.info(f"正在将表 {table_name} 的最旧分区 {oldest_partition} 归档到 {s3_archive_path}")

        archive_sql = f"""
        INSERT INTO FILES
        (
            "path" = "{s3_archive_path}",
            "format" = "parquet",
            "compression" = "zstd",
            "target_max_file_size" = "209715200",
            "aws.s3.access_key" = "{s3_config['aws_access_key']}",
            "aws.s3.secret_key" = "{s3_config['aws_secret_key']}",
            "aws.s3.region" = "{s3_config['aws_region']}"
        )
        SELECT *
        FROM {table_name}
        WHERE {partition_column} = '{oldest_partition}';
        """
        cursor.execute(archive_sql)

        logging.info(f"正在从 StarRocks 中删除表 {table_name} 的已归档分区 {oldest_partition}")
        delete_sql = f"DELETE FROM {table_name} WHERE {partition_column} = '{oldest_partition}';"
        cursor.execute(delete_sql)

        logging.info(f"已成功归档并删除表 {table_name} 中的分区 {oldest_partition}")
        return True
    logging.info(f"表 {table_name} 没有可归档的分区")
    return False


def check_and_archive_table(table_name):
    logging.info(f"开始检查并归档表 {table_name}")
    try:
        retention_days, partition_column = read_config(table_name)

        logging.info(f"正在连接到 StarRocks 数据库")
        connection = pymysql.connect(**starrocks_config)
        cursor = connection.cursor()

        current_partition_count = get_partition_count(cursor, table_name, partition_column)

        while current_partition_count > retention_days:
            logging.info(
                f"表 {table_name} 的当前分区数 {current_partition_count} 超过保留天数 {retention_days}，开始归档")
            if archive_oldest_partition(cursor, table_name, partition_column):
                current_partition_count -= 1
            else:
                break

        if current_partition_count <= retention_days:
            logging.info(f"表 {table_name} 归档完成。当前分区数量：{current_partition_count}")
        else:
            logging.warning(f"警告：表 {table_name} 的分区数量仍然超过保留天数。当前分区数量：{current_partition_count}")

        connection.commit()
    except Exception as e:
        logging.error(f"处理表 {table_name} 时出错：{str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()


def archive_all_tables():
    current_date = datetime.now().strftime("%Y-%m-%d")
    logging.info(f"{current_date} 开始归档所有表")
    tables = get_tables_to_archive()
    logging.info(f"待归档的表：{tables}")
    for table in tables:
        check_and_archive_table(table)
    logging.info(f"{current_date} 所有表的归档过程完成")



def main():
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        logging.info("开始手动检查和归档")
        archive_all_tables()
    else:
        logging.info("启动定时归档任务")
        # 立即执行一次归档
        archive_all_tables()

        # 设置定时任务
        schedule.every().day.at("00:30").do(archive_all_tables)

        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次


if __name__ == "__main__":
    main()
