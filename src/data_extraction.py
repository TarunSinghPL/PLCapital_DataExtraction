# This file do the data extraction from the pdf using AWS Textract.
import os
import json
import time
import boto3
from collections import defaultdict
from urllib.parse import urlparse

class PDFTextractProcessor:
    def __init__(self, bucket_name, region="ap-south-1", local_output_base="D:\PL\Extracted Data"):
        self.bucket_name = bucket_name
        self.region = region
        self.local_output_base = local_output_base
        self.textract = boto3.client('textract', region_name=region)


    def extract_text_from_pdf_s3_async(self, s3_key):

        print("***********************************************************************************")
        print("Bucket:", self.bucket_name)
        print("S3 key:", s3_key)

        response = self.textract.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': self.bucket_name, 'Name': s3_key}},
            FeatureTypes=["TABLES", "FORMS"]
        )

        job_id = response['JobId']
        print(f"Textract Job started with ID: {job_id}")

        while True:
            result = self.textract.get_document_analysis(JobId=job_id)
            status = result['JobStatus']
            if status in ['SUCCEEDED', 'FAILED']:
                break
            print("Waiting for job to complete...")
            time.sleep(5)

        if status == 'FAILED':
            # raise Exception("Textract job failed")
            raise RuntimeError(f"Textract job failed. AWS message: {result.get('StatusMessage', 'No message provided')}")

        print("Textract job completed")

        blocks = result['Blocks']
        next_token = result.get('NextToken')

        while next_token:
            result = self.textract.get_document_analysis(JobId=job_id, NextToken=next_token)
            blocks.extend(result['Blocks'])
            next_token = result.get('NextToken')

        page_chunks = defaultdict(list)
        for block in blocks:
            if block["BlockType"] == "LINE":
                page_number = block["Page"]
                text = block["Text"]
                page_chunks[page_number].append(text)

        page_text_chunks = [
            {"page_no": str(page), "content": "\n".join(lines)}
            for page, lines in sorted(page_chunks.items())
        ]

        return page_text_chunks


    def save_extracted_data_to_json(self, s3_uri, extracted_data):
        # Parse S3 path: s3://bucket/company/category/filename.pdf
        parsed = urlparse(s3_uri)
        path_parts = parsed.path.strip("/").split("/")  # ['company', 'category', 'file.pdf']
        
        if len(path_parts) < 3:
            raise ValueError("S3 path is too short to extract folder structure.")

        # company, category, filename = path_parts[-3], path_parts[-2], path_parts[-1]
        company = path_parts[0]
        year = path_parts[1]
        quarter = path_parts[2]
        category = path_parts[3]
        filename = path_parts[-1]
        file_stem = os.path.splitext(filename)[0]

        output_folder = os.path.join(self.local_output_base, company, year, quarter, category)
        os.makedirs(output_folder, exist_ok=True)

        output_path = os.path.join(output_folder, f"{file_stem}.json")
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(extracted_data, f, indent=4, ensure_ascii=False)
        
        print(f"Extracted data saved to {output_path}")
        return output_path


    def run_textract_on_uploaded_pdfs(self, s3_uris):
        print(s3_uris)
        save_paths = []
        for s3_uri in s3_uris:
            try:
                s3_key = urlparse(s3_uri).path.lstrip("/")
                print(f"\nProcessing: {s3_uri}")
                print(s3_key)   
                extracted = self.extract_text_from_pdf_s3_async(s3_key)
                output_path = self.save_extracted_data_to_json(s3_uri, extracted)
                save_paths.append(output_path)
            except Exception as e:
                print(f"Error processing {s3_uri}: {e}")
                save_paths.append(None)
        return save_paths
    
    def run_textract_on_single_pdf(self, s3_uri):
        """
        New method for single PDF extraction
        """
        try:
            s3_key = urlparse(s3_uri)
            s3_key_parsed = s3_key.path.lstrip("/")
            print(f"\nProcessing single PDF: {s3_key_parsed}")
            extracted = self.extract_text_from_pdf_s3_async(s3_key_parsed)
            output_path = self.save_extracted_data_to_json(s3_uri, extracted)
            return output_path
        except Exception as e:
            print(f"Error processing {s3_uri}: {e}")
            return None




# if __name__=="__main__":
#     print("Running Textract")

#     extractor = PDFTextractProcessor(
#         bucket_name="plcapital-dataextraction"
#     )
    
#     output_path = extractor.run_textract_on_single_pdf(
#         s3_uri="s3://plcapital-dataextraction/TARUN/FY25/Q1/board_outcome/Board_Outcome_FY25Q4.pdf"
#     )

#     print("*********************************************************")
#     print("Data Path: ", output_path)

