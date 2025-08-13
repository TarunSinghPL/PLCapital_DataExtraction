
import os
import boto3

class S3Uploader:
    def __init__(self, bucket_name, region="ap-south-1"):
        self.bucket_name = bucket_name
        self.region = region
        self.s3 = boto3.client("s3", region_name=region)

    def upload_pdf_to_s3(self, pdf_path):
        """Uploads a PDF to S3 by inferring company/category from the file path."""
        if not os.path.isfile(pdf_path):
            raise FileNotFoundError(f"File not found: {pdf_path}")

        abs_path_parts = os.path.abspath(pdf_path).split(os.sep)

        if len(abs_path_parts) < 3:
            raise ValueError("PDF path too shallow to extract company/category.")

        filename = abs_path_parts[-1]
        category_folder = abs_path_parts[-2].replace(" ", "_").lower()
        company_name = abs_path_parts[-3]

        s3_key = f"{company_name}/{category_folder}/{filename}"
        s3_uri = f"s3://{self.bucket_name}/{s3_key}"

        try:
            self.s3.upload_file(pdf_path, self.bucket_name, s3_key)
            print(f"✅ Uploaded to {s3_uri}")
            return s3_uri

        except Exception as e:
            print(f"❌ Failed to upload {pdf_path}: {e}")
            return None

    def upload_category_folder(self, category_folder_path):
        """Uploads all PDFs from a category folder to S3."""
        if not os.path.isdir(category_folder_path):
            raise NotADirectoryError(f"Not a directory: {category_folder_path}")

        uploaded_paths = []
        for file in os.listdir(category_folder_path):
            if file.lower().endswith(".pdf"):
                full_path = os.path.join(category_folder_path, file)
                s3_uri = self.upload_pdf_to_s3(full_path)
                if s3_uri:
                    uploaded_paths.append(s3_uri)
        return uploaded_paths

    
    def upload_single_pdf(self, company_name, year, quarter, category, pdf_path):

        if not os.path.isfile(pdf_path):
            raise FileNotFoundError(f"File not found: {pdf_path}")

        filename = os.path.basename(pdf_path)

        # Sanitize folder names
        company_folder = company_name.strip().upper().replace(" ", "_")
        year_folder = year.strip().upper()  # Ensure format like 'FY25'
        quarter_folder = quarter.strip().upper()  # Ensure format like 'Q1'
        category_folder = category.strip().replace(" ", "_").lower()
        filename = os.path.basename(pdf_path).replace(" ", "_")

        # S3 path
        s3_key = f"{company_folder}/{year_folder}/{quarter_folder}/{category_folder}/{filename}"
        s3_uri = f"s3://{self.bucket_name}/{s3_key}"

        try:
            self.s3.upload_file(pdf_path, self.bucket_name, s3_key)
            print(f"✅ Uploaded to {s3_uri}")
            return s3_uri

        except Exception as e:
            print(f"❌ Failed to upload {pdf_path}: {e}")
            return None




# if __name__ == "__main__":

#     uploader = S3Uploader(
#         bucket_name="plcapital-dataextraction"
#     )

#     s3_path = uploader.upload_single_pdf(
#         company_name="TARUN",
#         year="FY25",
#         quarter="Q1",
#         category="Board Outcome",
#         pdf_path="D:\PL\Data\ACC\Board Outcome\Board Outcome FY25Q4.pdf"
#     )

#     print("***********************************************")
#     print("pdf path: ", s3_path)
