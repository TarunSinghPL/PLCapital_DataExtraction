from langchain.prompts import PromptTemplate


class PromptBuilder:
    def __init__(self):
        self.template = """
    Instructions:
    ------------------------------------
    You are an expert data analyst and information extractor. Carefully review the entire extracted data provided below, 
    which may be complex and contain interrelated information across multiple entries. For each of the terms listed, 
    analyze the data using your pattern recognition and reasoning abilities to accurately extract or, if necessary, deduce the corresponding value.

    When extracting numeric values, copy them exactly as they appear in the data, including any leading or trailing zeros. Do not round, truncate, or scale.
    If the value has a comma (e.g. 132,230), remove only the comma but keep all digits. For example, 132,230 should be returned as 132230, not 13223 or any other shortened form.

    1) Use your best judgment to resolve interrelationships (such as references to totals, subcomponents, or calculations spread across the data).
    2) Consider all relevant information in the text, not just exact matches; be attentive to synonyms, abbreviations, or related descriptions
    3) If calculating the value of a term requires interpreting values presented in a distributed or indirect way, do so.
    4) If a value cannot be found or inferred with confidence, return null for that term.
    5) Return the result as a structured JSON object where each key is the term and each value is the extracted or computed value.
    6) Only find the values of Terms which are given below do not add terms on your own just stick to the terms which are given in Terms section.
    7) Return only the JSON object — do not include any explanation or surrounding text (e.g., avoid ```json or ``` or any markdown syntax).
    8) Ensure each key appears **only once** in the JSON. Do not repeat or duplicate any terms.
    9) do not calculate values by your self and mostely try to find the values from first table "Consolidated Financial Results" and if you find '-' for a value do not pick the next value for that terms just give '0'.
    10) Most Important only pick the first value of evry Terms if not find return '0'.
    11) Before returning the final JSON, check if the document or data mentions whether the financial values are presented in crores or millions. 
        - If the values are in **crores**, convert all numeric values to **millions** by multiplying each by 10 and make sure convert every value weather its amount of intrust rate but not in the case of quantities like(Tonnes, Kg, MT, MnT, etc) .
        - If the values are already in **millions**, leave them unchanged.
        - Do **not** apply any conversion to quantities or units like Tonnes, Kg, MT, MnT, etc
        - Perform this step only if you can confidently detect the unit.
        - And perform these things for every value.
    ------------------------------------

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

    def build_prompt(self, extracted_data: str, terms: str) -> str:
        return self.prompt.format(extracted_data=extracted_data, terms=terms)
