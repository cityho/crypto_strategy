import sys
from pathlib import Path

BASE_DIR = Path("data/futures")

def delete_files_by_keyword(keyword: str):
    if not BASE_DIR.exists():
        print(f"Path does not exist: {BASE_DIR}")
        return

    for file in BASE_DIR.rglob("*"):
        if file.is_file() and keyword in file.name:
            file.unlink()
            print(f"Deleted: {file}")

if __name__ == "__main__":
    keyword = sys.argv[1] if len(sys.argv) > 1 else "12h"
    delete_files_by_keyword(keyword)

# export PYTHONPATH=$(pwd) && python src/data/remove_used_file.py 12h 와 같이 처리함.