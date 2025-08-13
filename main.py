import os
import time
import json
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse

from src.data_upload import S3Uploader
from src.data_extraction import PDFTextractProcessor
from src.prompt import PromptBuilder
from src.llm import LLMCaller
from utils.load_json import load_json
from utils.save_json import save_to_file
from utils.save_prompt import save_prompt_to_file
from utils.combine_json import final_json




def main():
    print("Main Function")

    company_name = input("Enter the company name: ").strip()
    year  = input("Enter the Year (eg.. FY25): ")
    quater = input("Enter the quater (eg.. Q1): ")
    boardoucome_terms = input("Enters the Board Oucome terms: ").split(",")
    invester_terms = input("Enter the Investers terms: ").split(",")

    board_outcome_path = input("Enter the Board Outcome path: ").strip()
    investor_presentation_path = input("Enter the Investor Presentation path: ").strip()

    print(company_name)
    print(year)
    print(quater)
    print(boardoucome_terms)
    print(invester_terms)
    print(board_outcome_path)
    print(investor_presentation_path)

    # Initialize Components
    uploader = S3Uploader(
        bucket_name="plcapital-dataextraction"
    )
    extractor = PDFTextractProcessor(
        bucket_name="plcapital-dataextraction"
    )
    prompt_builder = PromptBuilder()
    llm = LLMCaller()


    # Uplode File to S3

    if os.path.isfile(board_outcome_path):
        upload_files_board_outcome = [
            uploader.upload_single_pdf(company_name=company_name, year=year, quarter=quater, category="Board Outcome", pdf_path=board_outcome_path)
        ]
    else:
        upload_files_board_outcome = uploader.upload_category_folder(category_folder_path=board_outcome_path)

    if os.path.isfile(investor_presentation_path):
        upload_files_investor_presentations = [
            uploader.upload_single_pdf(company_name=company_name, year=year, quarter=quater, category="Investor Presentation", pdf_path=investor_presentation_path)
        ]
    else:
        upload_files_investor_presentations = uploader.upload_category_folder(category_folder_path=investor_presentation_path)


    print("Board file: ", upload_files_board_outcome)
    print("Invester file: ", upload_files_investor_presentations)


    # Data Extraction

    if os.path.isfile(board_outcome_path):
        extracted_boardoutcome_data_path = [
            extractor.run_textract_on_single_pdf(upload_files_board_outcome[0])
        ]
    else:
        extracted_boardoutcome_data_path = extractor.run_textract_on_uploaded_pdfs(upload_files_board_outcome)

    if os.path.isfile(investor_presentation_path):
        extracted_investor_data_path = [
            extractor.run_textract_on_single_pdf(upload_files_investor_presentations[0])
        ]
    else:
        extracted_investor_data_path = extractor.run_textract_on_uploaded_pdfs(upload_files_investor_presentations)

    print("Data Path board outcome: ", extracted_boardoutcome_data_path)
    print("Data Path investor outcome: ", extracted_investor_data_path)


    # Process Board Outcome

    board_data_map = {}
    for json_file in extracted_boardoutcome_data_path:
        try:
            pdf_data = load_json(json_file)
            prompt = prompt_builder.build_prompt(pdf_data, boardoucome_terms)
            filename = os.path.splitext(os.path.basename(json_file))[0] + "_prompt.txt"
            prompt_path = save_prompt_to_file(prompt, filename, company_name, year, quater, category="Board Outcome")

            response = llm.llm_call(prompt)
            board_data_map[filename] = response if response else {}
            time.sleep(5)
        except Exception as e:
            print(f"Error processing {json_file}: {e}")

    
    # Process Investor Presentations

    investor_data_map = {}
    for json_file in extracted_investor_data_path:
        try:
            pdf_data = load_json(json_file)
            prompt = prompt_builder.build_prompt(pdf_data, invester_terms)
            filename = os.path.splitext(os.path.basename(json_file))[0] + "_prompt.txt"
            prompt_path = save_prompt_to_file(prompt, filename, company_name, year, quater, category="Investor Presentation")

            response = llm.llm_call(prompt)
            investor_data_map[filename] = response if response else {}
            time.sleep(5)
        except Exception as e:
            print(f"Error processing {json_file}: {e}")

    final_output_path = final_json(board_data_map, investor_data_map, company_name)

    print("*******************************")
    print(final_output_path)


    final_output = load_json(final_output_path)

    if isinstance(final_output, dict):
        # Single dictionary
        final_output_string = "; ".join([f"{key}: {value}" for key, value in final_output.items()])
    elif isinstance(final_output, list) and all(isinstance(item, dict) for item in final_output):
        # List of dictionaries
        all_items = []
        for item in final_output:
            all_items.extend([f"{key}: {value}" for key, value in item.items()])
        final_output_string = "; ".join(all_items)
    else:
        # Anything else (None, empty, unexpected format)
        final_output_string = "No data extracted"

    print(final_output_string)


    

if __name__ == "__main__":
    main()