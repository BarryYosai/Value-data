from flask import Flask, request, jsonify, send_from_directory
import subprocess
import sys
import os
import re
import boto3
import json
from flask_cors import CORS
from datetime import datetime
import logging

app = Flask(__name__)
CORS(app)  # 启用CORS，允许跨域请求

# 配置日志
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# S3 配置信息
s3_config = {
    'bucket': 'sigen-starrocks-valued-data-archive',
    'aws_access_key': 'AKIA2OWZZVYFXKB2XGY7',
    'aws_secret_key': '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',
    'aws_region': 'cn-northwest-1'
}

# S3 客户端初始化
s3_client = boto3.client(
    's3',
    aws_access_key_id=s3_config['aws_access_key'],
    aws_secret_access_key=s3_config['aws_secret_key'],
    region_name=s3_config['aws_region']
)

@app.route('/')
@app.route('/sigen_archive_platform.html')
def serve_html():
    return send_from_directory('static', 'sigen_archive_platform.html')

@app.route('/start_archive', methods=['POST'])
def start_archive():
    app.logger.info('Received start_archive request')
    try:
        data = request.json
        if not data:
            raise ValueError("No JSON data received")

        table_name = data.get('table_name')
        partition_column = data.get('partition_column')
        selected_columns = data.get('selected_columns')

        if not all([table_name, partition_column, selected_columns]):
            raise ValueError("Missing required parameters")

        app.logger.info(f"Received parameters: table_name={table_name}, partition_column={partition_column}, selected_columns={selected_columns}")

        process = subprocess.Popen(
            [sys.executable, 'initial_archive.py', table_name, partition_column, selected_columns],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8',
            errors='replace',
            env={**os.environ, 'PYTHONIOENCODING': 'utf-8'}
        )

        stdout, stderr = process.communicate()

        if process.returncode != 0:
            raise Exception(f"Script execution failed: {stderr}")

        logs = stdout.split('\n') + stderr.split('\n')
        logs = [log for log in logs if log]

        app.logger.info('Archive process completed successfully')
        return jsonify({'status': 'success', 'logs': logs})

    except Exception as e:
        app.logger.error(f'Error in start_archive: {str(e)}')
        return jsonify({'status': 'error', 'message': str(e), 'logs': []}), 500

@app.route('/start_periodic_snapshot', methods=['POST'])
def start_periodic_snapshot():
    app.logger.info('Received start_periodic_snapshot request')
    try:
        data = request.json
        if not data:
            raise ValueError("No JSON data received")

        table_name = data.get('table_name')
        selected_columns = data.get('selected_columns')

        if not all([table_name, selected_columns]):
            raise ValueError("Missing required parameters")

        app.logger.info(f"Received parameters: table_name={table_name}, selected_columns={selected_columns}")

        process = subprocess.Popen(
            [sys.executable, 'mysql_periodic_snapshot.py', table_name, selected_columns],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8',
            errors='replace',
            env={**os.environ, 'PYTHONIOENCODING': 'utf-8'}
        )

        stdout, stderr = process.communicate()

        if process.returncode != 0:
            raise Exception(f"Script execution failed: {stderr}")

        logs = stdout.split('\n') + stderr.split('\n')
        logs = [log for log in logs if log]

        starrocks_table_name = None
        for log in logs:
            if "创建成功" in log and "外部表" in log:
                table_name_match = re.search(r'外部表\s+(\S+)\s+创建成功', log)
                if table_name_match:
                    starrocks_table_name = table_name_match.group(1)
                    break

        app.logger.info('Snapshot process completed successfully')
        return jsonify({
            'status': 'success',
            'logs': logs,
            'starrocks_table_name': starrocks_table_name
        })

    except Exception as e:
        app.logger.error(f'Error in start_periodic_snapshot: {str(e)}')
        return jsonify({'status': 'error', 'message': str(e), 'logs': []}), 500

@app.route('/get_archive_dates')
def get_archive_dates():
    log_file = 'periodic_archive.log'
    dates = set()
    if os.path.exists(log_file):
        encodings = ['utf-8', 'gbk', 'gb18030', 'iso-8859-1']
        for encoding in encodings:
            try:
                with open(log_file, 'r', encoding=encoding) as f:
                    for line in f:
                        if '所有表的归档过程完成' in line:
                            date = line.split()[0]
                            dates.add(date)
                break  # 如果成功读取，跳出循环
            except UnicodeDecodeError:
                continue  # 如果当前编码失败，尝试下一个
    return jsonify(sorted(dates, reverse=True))

@app.route('/get_log/<date>')
def get_log(date):
    log_file = 'periodic_archive.log'
    log_content = []
    if os.path.exists(log_file):
        encodings = ['utf-8', 'gbk', 'gb18030', 'iso-8859-1']
        for encoding in encodings:
            try:
                with open(log_file, 'r', encoding=encoding) as f:
                    recording = False
                    for line in f:
                        if f"{date} 开始归档所有表" in line:
                            recording = True
                        if recording:
                            cleaned_line = line.split(':', 2)[-1].strip() if ':' in line else line.strip()
                            log_content.append(cleaned_line)
                        if f"{date} 所有表的归档过程完成" in line and recording:
                            break
                break  # 如果成功读取，跳出循环
            except UnicodeDecodeError:
                continue  # 如果当前编码失败，尝试下一个
    return '\n'.join(log_content)

@app.route('/get_archive_data', methods=['GET'])
def get_archive_data():
    app.logger.info("Starting to fetch archive data")
    archive_data = []
    try:
        subdirectories = list_s3_subdirectories()
        app.logger.info(f"Found {len(subdirectories)} subdirectories")

        for subdir in subdirectories:
            config_file_key = f"{subdir}config.txt"
            app.logger.info(f"Attempting to read config file: {config_file_key}")
            config = read_config_file_from_s3(config_file_key)
            if config:
                archive_data.append({
                    'table_name': config['table_name'],
                    'value_columns': config['selected_columns'],
                    'partition_column': config['partition_column'],
                    'external_table_name': f"{config['table_name']}_archived",
                    'retention_days': config['retention_days'],
                    'archive_date': config['archive_date']
                })
                app.logger.info(f"Successfully processed config for {config['table_name']}")
            else:
                app.logger.warning(f"Failed to read or parse config for {subdir}")

        app.logger.info(f"Returning {len(archive_data)} archive records")
        return jsonify(archive_data)
    except Exception as e:
        app.logger.error(f"Error in get_archive_data: {str(e)}")
        return jsonify({'error': str(e)}), 500

def list_s3_subdirectories():
    try:
        response = s3_client.list_objects_v2(Bucket=s3_config['bucket'], Delimiter='/')
        return [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]
    except Exception as e:
        app.logger.error(f"Error listing S3 subdirectories: {str(e)}")
        raise

def read_config_file_from_s3(file_key):
    try:
        response = s3_client.get_object(Bucket=s3_config['bucket'], Key=file_key)
        config_content = response['Body'].read().decode('utf-8')

        config = {}
        lines = config_content.strip().split('\n')
        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                config[key.strip()] = value.strip()

        file_info = s3_client.head_object(Bucket=s3_config['bucket'], Key=file_key)
        archive_date = file_info['LastModified'].strftime('%Y-%m-%d')

        return {
            'table_name': config.get('表名', '未知'),
            'partition_column': config.get('分区字段名', '未知'),
            'selected_columns': config.get('选择的价值数据字段名', '').replace(' ', ''),
            'retention_days': config.get('保留分区数', '未知'),
            'archive_date': archive_date
        }
    except Exception as e:
        app.logger.error(f"Error reading or parsing file {file_key}: {str(e)}")
        return None

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
