import json
import os
from typing import Union, List

def load_json(path: Union[str, List[str]]) -> Union[dict, list, List[Union[dict, list]]]:
    """
    Load JSON from a single file path or list of file paths.

    Returns:
    - dict or list (for single file)
    - list of dicts/lists (for multiple files)
    
    """

    if isinstance(path, str):
        if os.path.exists(path) and path.endswith(".json"):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            print(f"[WARN] File not found or not a .json: {path}")
            return None

    elif isinstance(path, list):
        loaded = []
        for p in path:
            if os.path.exists(p) and p.endswith(".json"):
                with open(p, "r", encoding="utf-8") as f:
                    loaded.append(json.load(f))
            else:
                print(f"[WARN] Skipping invalid file: {p}")
        return loaded


# if __name__ == "__main__":
#     data = load_json("D:\\PL\\Extracted Data\\ACC\\board_outcome\\Board Outcome FY25Q1.json")
#     print(data)