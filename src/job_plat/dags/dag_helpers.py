import sys
import subprocess
import time
import os


def spark_app(path: str) -> str:
    base = os.getenv("SPARK_APP_PATH", "/app/src/job_plat/runners")
    return f"{base}/{path}"

def run_command(cmd: list) -> None:
    
    start = time.time()
    print(f"Running command: {cmd}")
    
    subprocess.run([sys.executable, *cmd], check=True)
    
    duration = time.time() - start
    print(f"Finished in {duration:.2f}s")
    

