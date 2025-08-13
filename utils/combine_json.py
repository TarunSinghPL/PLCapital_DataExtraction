import os
import json
from utils.save_json import save_to_file


def final_json(board_data_map, investor_data_map, company_name ):
    # Step 7: Merge and save final combined JSON

    final_output_dir = f"outputs/final_jsons/{company_name}"
    os.makedirs(final_output_dir, exist_ok=True)
    combined_files = []
    for board_filename, board_response in board_data_map.items():
        try:
            quarter = None
            for q in ["Q1", "Q2", "Q3", "Q4"]:
                if q.lower() in board_filename.lower():
                    quarter = q
                    break
            if not quarter:
                continue

            matching_investor_file = next(
                (inv_file for inv_file in investor_data_map if quarter.lower() in inv_file.lower()), None
            )
            investor_response = investor_data_map.get(matching_investor_file, {})

            if isinstance(board_response, str):
                try:
                    board_response = json.loads(board_response)
                except:
                    board_response = {}
            if isinstance(investor_response, str):
                try:
                    investor_response = json.loads(investor_response)
                except:
                    investor_response = {}

            final_combined = {**board_response, **investor_response}
            combined_filename = f"combined_{quarter}.json"
            combined_path = os.path.join(final_output_dir, combined_filename)
            save_to_file(final_combined, combined_path, file_type="json")
            print(f"Saved combined data to {combined_path}")
            combined_files.append(combined_path)
        except Exception as e:
            print(f"Error merging data for {board_filename}: {e}")

    return combined_files