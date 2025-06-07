import requests
from pathlib import Path
import os
import traceback
import requests

def import_to_graphdb(name):
    try:
        # Configuration
        GRAPHDB_SERVER = "http://localhost:7200"
        REPOSITORY_ID = "letstalk"
        CREDENTIALS = ("admin", "root")
        FILE_FORMAT = "text/turtle"

        # File path resolution (works in both notebooks and scripts)
        current_dir = Path(os.getcwd())  
        file_path = current_dir / "docker-import" / name
        
        # Verify file
        if not file_path.exists():
            raise FileNotFoundError(f"File not found at {file_path}")
        if not file_path.is_file():
            raise ValueError(f"Path {file_path} is not a file")
        if file_path.stat().st_size == 0:
            raise ValueError("File is empty")

        # Read and send
        with open(file_path, 'rb') as file:
            response = requests.post(
                f"{GRAPHDB_SERVER}/repositories/{REPOSITORY_ID}/statements",
                headers={"Content-Type": FILE_FORMAT},
                data=file,
                auth=CREDENTIALS,
                timeout=60
            )

        if response.status_code == 204:
            return "Data imported successfully!"
        else:
            return f"Error {response.status_code}: {response.text}"

    except Exception as e:
        error_msg = [
            f"Error: {type(e).__name__}",
            f"Message: {str(e)}",
            "\nTroubleshooting:"
        ]
        
        if isinstance(e, FileNotFoundError):
            error_msg.extend([
                f"- Check file exists at: {file_path}",
                f"- Current working directory: {os.getcwd()}",
                "- For Docker: verify volume mount exists",
                "- Try running this in a terminal:",
                f"  ls -l {file_path}"
            ])
        elif isinstance(e, requests.exceptions.RequestException):
            error_msg.extend([
                f"- Check GraphDB is running at {GRAPHDB_SERVER}",
                "- For Docker: ensure ports are properly mapped (7200:7200)",
                "- Try this test:",
                f"  curl -v http://localhost:7200/rest/repositories"
            ])
        
        error_msg.append("\nStack trace:\n" + traceback.format_exc())
        return "\n".join(error_msg)


print("Importing TBox to GraphDB...")
import_to_graphdb("tbox.ttl")
print("------------------------------------\n")
print("Importing ABox to GraphDB...")
import_to_graphdb("abox.ttl")
