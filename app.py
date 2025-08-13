import os
import uuid
import tempfile
import shutil
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
from typing import Optional
from src.data_upload import S3Uploader
from src.data_extraction import PDFTextractProcessor
import boto3
import time
from src.prompt import PromptBuilder
from src.llm import LLMCaller
from utils.load_json import load_json
from utils.save_json import save_to_file
from utils.save_prompt import save_prompt_to_file
from utils.combine_json import final_json

app = FastAPI()

uploader = S3Uploader(bucket_name="plcapital-dataextraction")
extractor = PDFTextractProcessor(bucket_name="plcapital-dataextraction")
s3_client = boto3.client("s3", region_name="ap-south-1")
prompt_builder = PromptBuilder()
llm = LLMCaller()

# Temp storage base path
BASE_TEMP_DIR = os.path.join(tempfile.gettempdir(), "pdf_uploads")

def get_safe_filename(filename: str) -> str:
    return os.path.basename(filename).replace(" ", "_")

def s3_key_exists(bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False

@app.post("/upload-pdf")
async def upload_pdf(
    company_name: str = Form(...),
    year: str = Form(...),
    qtr: str = Form(...),
    file_type: str = Form(...),  # "BO" or "IP"
    pdf_file: UploadFile = File(...),
    session_id: Optional[str] = Form(None)
):
    # Always use a deterministic session id: company_year_qtr
    session_id = f"{company_name}_{year}_{qtr}".lower().replace(" ", "_")

    # Create folder: temp/session_id/file_type/
    temp_dir = os.path.join(BASE_TEMP_DIR, session_id, file_type)
    os.makedirs(temp_dir, exist_ok=True)

    # Save locally
    local_pdf_path = os.path.join(temp_dir, get_safe_filename(pdf_file.filename))
    with open(local_pdf_path, "wb") as f:
        f.write(await pdf_file.read())

    # Upload to S3 immediately using your uploader
    s3_uri = uploader.upload_single_pdf(
        company_name=company_name,
        year=year,
        quarter=qtr,
        category="Board Outcome" if file_type.upper() == "BO" else "Investor Presentation",
        pdf_path=local_pdf_path
    )

    # Delete this file after uploading
    try:
        os.remove(local_pdf_path)
        if not os.listdir(temp_dir):  # If file_type folder empty, remove it
            shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception as e:
        print(f"Cleanup failed: {e}")

    return JSONResponse({
        "status": "success",
        "session_id": session_id,
        "uploaded_file": pdf_file.filename,
        "s3_uri": s3_uri
    })

@app.post("/process-session")
async def process_session(
    company_name: str = Form(...),
    year: str = Form(...),
    qtr: str = Form(...),
    boardoutcome_terms: str = Form(...),
    investor_presentation_terms: str = Form(...),
):
    # session id from company/year/qtr
    session_id = f"{company_name}_{year}_{qtr}"
    
    # Expected S3 paths (adjust if uploader uses different folder structure)
    bo_key = f"{company_name.upper()}/{year.upper()}/{qtr.upper()}/board_outcome/Board_Outcome_{year.upper()}{qtr.upper()}.pdf"
    ip_key = f"{company_name.upper()}/{year.upper()}/{qtr.upper()}/investor_presentation/Investor_Presentation_{year.upper()}{qtr.upper()}.pdf"

    print(bo_key)
    print(ip_key)

    # Check file existence
    bo_exists = s3_key_exists(bucket="plcapital-dataextraction", key=bo_key)
    ip_exists = s3_key_exists(bucket="plcapital-dataextraction", key=ip_key)


    # if bo_exists and ip_exists:
    #     return "Both the files exist"
    # else:
    #     return "File not exist"

    if not bo_exists or not ip_exists:
        return JSONResponse({
            "status": "error",
            "message": "Missing file(s)",
            "missing": {
                "boardoutcome": not bo_exists,
                "investor_presentation": not ip_exists
            }
        }, status_code=400)

    # Both files exist → Run processing pipeline
    try:
        # Step 1: Data Extraction
        # bo_data = extract_data(f"s3://{BUCKET_NAME}/{bo_key}", boardoutcome_terms)
        # ip_data = extract_data(f"s3://{BUCKET_NAME}/{ip_key}", investor_presentation_terms)

        extracted_bo_data_path = [extractor.run_textract_on_single_pdf(bo_key)]
        extracted_ip_data_path = [extractor.run_textract_on_single_pdf(ip_key)]

        board_data_map = {}
        for json_file in extracted_bo_data_path:
            try:
                pdf_data = load_json(json_file)
                prompt = prompt_builder.build_prompt(pdf_data, boardoutcome_terms)
                filename = os.path.splitext(os.path.basename(json_file))[0] + "_prompt.txt"
                prompt_path = save_prompt_to_file(prompt, filename, company_name, year, qtr, category="Board Outcome")

                response = llm.llm_call(prompt)
                board_data_map[filename] = response if response else {}
                time.sleep(5)
            except Exception as e:
                print(f"Error processing {json_file}: {e}")


        investor_data_map = {}
        for json_file in extracted_ip_data_path:
            try:
                pdf_data = load_json(json_file)
                prompt = prompt_builder.build_prompt(pdf_data, investor_presentation_terms)
                filename = os.path.splitext(os.path.basename(json_file))[0] + "_prompt.txt"
                prompt_path = save_prompt_to_file(prompt, filename, company_name, year, qtr, category="Investor Presentation")

                response = llm.llm_call(prompt)
                investor_data_map[filename] = response if response else {}
                time.sleep(5)
            except Exception as e:
                print(f"Error processing {json_file}: {e}")

        final_output_path = final_json(board_data_map, investor_data_map, company_name)

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

        return final_output_string



    except Exception as e:
        return JSONResponse({
            "status": "error",
            "message": f"Processing failed: {str(e)}"
        }, status_code=500)