# This file does the data extraction from the pdf using AWS Textract with caching in S3.
import os
import json
import time
import boto3
from collections import defaultdict
from urllib.parse import urlparse
from botocore.exceptions import ClientError

class PDFTextractProcessor:
    def __init__(self, bucket_name, region="ap-south-1", local_output_base="D:\PL\Extracted Data"):
        self.bucket_name = bucket_name
        self.region = region
        self.local_output_base = local_output_base
        self.textract = boto3.client('textract', region_name=region)
        self.s3 = boto3.client('s3', region_name=region)

    def extract_text_from_pdf_s3_async(self, s3_key):
        response = self.textract.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': self.bucket_name, 'Name': s3_key}},
            FeatureTypes=["TABLES", "FORMS"]
        )
        job_id = response['JobId']
        print(f"Textract Job started with ID: {job_id}")

        # Poll until job completes
        while True:
            result = self.textract.get_document_analysis(JobId=job_id)
            status = result['JobStatus']
            if status in ['SUCCEEDED', 'FAILED']:
                break
            print("Waiting for job to complete...")
            time.sleep(5)

        if status == 'FAILED':
            raise RuntimeError(f"Textract job failed. AWS message: {result.get('StatusMessage', 'No message provided')}")

        print("Textract job completed")

        # Collect blocks
        blocks = result['Blocks']
        next_token = result.get('NextToken')
        while next_token:
            result = self.textract.get_document_analysis(JobId=job_id, NextToken=next_token)
            blocks.extend(result['Blocks'])
            next_token = result.get('NextToken')

        # Convert to page chunks
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

    def _json_s3_key(self, s3_uri):
        """Generate JSON S3 key based on original PDF S3 path"""
        parsed = urlparse(s3_uri)
        path_parts = parsed.path.strip("/").split("/")  # e.g. ['Company','FY25','Q1','Board Outcome','file.pdf']
        file_stem = os.path.splitext(path_parts[-1])[0]
        json_key = "/".join(path_parts[:-1] + [f"{file_stem}.json"])
        return json_key

    def _local_json_path(self, s3_uri):
        """Generate local path to store JSON"""
        parsed = urlparse(s3_uri)
        path_parts = parsed.path.strip("/").split("/")
        file_stem = os.path.splitext(path_parts[-1])[0]
        output_folder = os.path.join(self.local_output_base, *path_parts[:-1])
        os.makedirs(output_folder, exist_ok=True)
        return os.path.join(output_folder, f"{file_stem}.json")

    def run_textract_with_cache(self, s3_uri):
        """
        Check if extracted JSON exists on S3.
        If yes -> download it locally and return path.
        If no  -> run Textract, save JSON locally + upload to S3.
        """
        json_key = self._json_s3_key(s3_uri)
        local_json_path = self._local_json_path(s3_uri)

        # 1. Check if JSON exists in S3
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=json_key)
            print(f"JSON already exists on S3: s3://{self.bucket_name}/{json_key}")
            self.s3.download_file(self.bucket_name, json_key, local_json_path)
            return local_json_path
        except ClientError:
            print("JSON not found on S3, running Textract...")

        # 2. Run Textract
        s3_key = urlparse(s3_uri).path.lstrip("/")
        extracted = self.extract_text_from_pdf_s3_async(s3_key)

        # 3. Save locally
        with open(local_json_path, "w", encoding="utf-8") as f:
            json.dump(extracted, f, indent=4, ensure_ascii=False)

        # 4. Upload to S3
        self.s3.upload_file(local_json_path, self.bucket_name, json_key)
        print(f"JSON uploaded to S3: s3://{self.bucket_name}/{json_key}")

        return local_json_path


# if __name__=="__main__":
#     print("Running Textract")

#     extractor = PDFTextractProcessor(
#         bucket_name="plcapital-dataextraction"
#     )
    
#     output_path = extractor.run_textract_with_cache(
#         s3_uri="s3://plcapital-dataextraction/TARUN/FY25/Q1/BOARD_OUTCOME/BOARD_OUTCOME_FY25Q4.PDF"
#     )

#     print("*********************************************************")
#     print("Data Path: ", output_path)