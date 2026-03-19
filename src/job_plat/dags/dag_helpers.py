import sys
import subprocess
import time

def run_command(cmd: list) -> None:
    
    start = time.time()
    print(f"Running command: {cmd}")
    
    subprocess.run([sys.executable, *cmd], check=True)
    
    duration = time.time() - start
    print(f"Finished in {duration:.2f}s")
    

