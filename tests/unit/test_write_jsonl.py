from job_plat.utils.storage import LocalStorage

def test_write_jsonl(tmp_path):
    storage = LocalStorage()
    records = [{"title": "Data Engineer"}]
    
    path = tmp_path / "jobs.jsonl"
    count = storage.write_jsonl(records, path)
    
    assert count == 1
    assert path.exists()
