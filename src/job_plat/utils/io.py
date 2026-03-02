import json
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Iterable, Iterator
from pathlib import Path
from job_plat.config.config_loader import ConfigLoader

def load_params(
    config_path: str | None = None, 
    env: str | None = None
    ) -> Dict:
    """
    Read a configuration yaml file and return its data.

    Args:
            path (str): filepath of the configuration file.

    Returns:
            Dict (Python object that best fits the data): configuration data in a nested structure
    """
    return ConfigLoader(config_path=config_path, env=env).as_dict()

# def write_jsonl(
    # records: Iterable[Dict], 
    # output_path: str | Path ) -> None:
    
    # output_path = Path(output_path)
    # output_path.mkdir(parents=True, exist_ok=True)
        
    # with output_path.open("w", encoding="utf-8") as file:
        # for record in records:
            # file.write(json.dumps(record, ensure_ascii=False) + "\n")


def iter_jsonl(
    input_path: str | Path) -> Iterator[Dict]:
    
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def load_jsonl( 
   input_path: str | Path,
    n_samples: int | None = None ) -> List[Dict]:
    
    if n_samples:
        return list(iter_jsonl(input_path=input_path))
    else:
        return list(islice(iter_jsonl(input_path=input_path), n_samples))
    
    
def create_spark(
    app_name: str,
    master: str = "local[*]"
) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .getOrCreate()
    )

def jsonl_to_dataframe(
    spark: SparkSession,
    input_path: str | Path
) -> DataFrame:
    return (
        spark.read
        .json(input_path)
        )


def write_parquet(
    df: DataFrame,
    output_path: str | Path
) -> None:
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
