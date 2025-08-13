import os



def save_prompt_to_file(prompt_text, filename, company_name, year, quarter, category, output_dir="output_prompts"):
    # Build folder path: output_prompts/{company_name}/{year}/{quarter}
    company_name = company_name.upper()
    year = year.upper()
    quarter = quarter.upper()
    folder_path = os.path.join(output_dir, company_name, year, quarter, category)
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(prompt_text)

    print(f"Prompt saved to {file_path}")

    return file_path