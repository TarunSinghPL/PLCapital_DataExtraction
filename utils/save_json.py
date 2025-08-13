import os
import json

def save_to_file(output_data, file_path, file_type="txt"):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if file_type == "json":
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, ensure_ascii=False, indent=4)
        elif file_type == "txt":
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output_data)
        else:
            print(f"Unsupported file type: {file_type}")
    except Exception as e:
        print(f"Error saving file {file_path}: {e}")
