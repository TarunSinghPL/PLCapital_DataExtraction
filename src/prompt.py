from langchain.prompts import PromptTemplate


class PromptBuilder:
    def __init__(self):
        self.template = """
        Instructions:
    ------------------------------------
    You are a specialized data analyst. Your task is to extract specific financial and operational data from a provided document.Carefully review the entire extracted data provided below, 
    which may be complex and contain interrelated information across multiple entries. For each of the terms listed, analyze the data using your pattern recognition and reasoning abilities and Use your best judgment to resolve interrelationships to accurately extract.

    Extraction Rules:

        1) Source Data: Prioritize data exclusively from the table titled "{table_name} Financial Results" Ignore all other tables if this not present look from all data.

        2) Value Search: For each term listed below, find its corresponding value in the source data.

        3) Numeric Values:

            Copy numeric values exactly. Do not round, truncate, or scale them.

            If a value contains a comma (e.g., 132,230), remove it to get a continuous number (e.g., 132230).

            If a value is a hyphen (-) or empty, use '0'.

            If shown as parentheses (e.g., (1,234) or (12.3)), treat as negative (e.g., -1234, -12.3).

            If a term has multiple values, use only the first one you find.

        4) Unit Conversion (Financial Data Only):

            Carefully check the document to determine the unit of the monetary financial value under investigation (e.g., "Rs. in Lakhs", "Rs. in Crores", "₹ in Crores").

            If the unit is Lakhs, divide every financial value by 10 to convert into millions.

            If the unit is crores, multiply every numeric financial value(but are not limited to) by exactly 10 to convert to millions and Do not apply any further scaling(like: (never ×100, ×1000, or ×10000)). Values are like [like: Revenue, Taxes, Profit, Interest] and This applies equally to whole numbers, decimals, and negatives (e.g., 5 → 50, (3.2) → -32, 0.7 → 7)

            Never apply this conversion to quantities or non-monetary values (e.g., Tonnes, Kg, MT, MnT, Cement sales volume, units of production/volume/capacity, headcount, units sold).

            Never multiply by 100 for any reason. Do not convert percentages, rates, margins, ratios, EPS, or per-share metrics.
        
        5) Special Rule for MnT and quantities:

            If a value includes units like "MnT", "MT", "Tonnes", "Kg", "capacity", "utilization", "volume", etc., treat as **quantities only**.

            Return only the numeric part (e.g., "29.4 MnT" → "29.4"). Do NOT expand it into millions.


        6) Output Format:

            Return the extracted data as a JSON object.

            The keys of the JSON object must be the terms provided.

            Each key must appear only once.

            Value of each key should be in "".

            Do not include any surrounding text or markdown syntax (e.g., no ```json).

            If a value cannot be found with confidence, return null.
        
    Extracted Data:
    ------------------------------------
    {extracted_data}
    ------------------------------------


    Terms:
    ------------------------------------
    {terms}
    ------------------------------------    
    """

        self.prompt = PromptTemplate.from_template(self.template)

    def build_prompt(self, table_name: str, extracted_data: str, terms: str) -> str:
        return self.prompt.format(table_name=table_name, extracted_data=extracted_data, terms=terms)
