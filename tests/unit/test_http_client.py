from unittest.mock import patch, MagicMock
from job_plat.bronze.ingestion.http_client import HttpClient

def test_http_client_success():
    client = HttpClient()
    
    with patch.object(client.session, "get") as mock_get:
        mock_response = MagicMock()
        mock_response.text = "<html></html>"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = client.get_text("http://fake.com")
        assert result == "<html></html>"
