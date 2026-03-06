from pathlib import Path
from datetime import date

def list_bronze_partitions(bronze_root: Path):
    
    partitions = []
    
    for path in bronze_root.glob("source=*/ingestion_date=*"):
        date_str = path.name.split("=")[1]
        partitions.append(date.fromisoformat(date_str))
    
    return sorted(set(partitions))

def get_new_partitions(bronze_root: Path, watermark: date | None):
    
    all_partitions = list_bronze_partitions(bronze_root)
    
    if watermark is None:
        return all_partitions
    
    return [p for p in all_partitions if p > watermark]
