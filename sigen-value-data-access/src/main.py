import threading
import subprocess
import logging
from queue import Queue, Empty

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('main')

def enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        queue.put(line)
    out.close()

def run_script(script_name):
    process = subprocess.Popen(['python', script_name],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               universal_newlines=True,
                               bufsize=1)

    q = Queue()
    t = threading.Thread(target=enqueue_output, args=(process.stdout, q))
    t.daemon = True
    t.start()

    while True:
        try:
            line = q.get(timeout=0.1)  # 等待0.1秒
            if line.strip():  # 只记录非空行
                logger.info(f"{script_name}: {line.strip()}")
        except Empty:
            if process.poll() is not None:
                break

def create_script_runner(script_name):
    return lambda: run_script(script_name)

if __name__ == "__main__":
    scripts = ['app.py', 'periodic_archive.py', 'mysql_periodic_snapshot.py']
    threads = []

    for script in scripts:
        thread = threading.Thread(target=create_script_runner(script))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    logger.info("所有脚本执行完毕")
