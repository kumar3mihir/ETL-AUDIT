def analyze_etl_script(script_content, file_name=None, additional_questions=None):
    """Sends the cleaned script to GenAI for audit analysis and extracts structured JSON output."""
    
    print("Entering analyze_etl_script() function...")
    print(f"Script Content (first 100 chars): {script_content[:100]}")

    # Construct the prompt
    prompt = f"""
    You are an ETL audit expert. Analyze the following ETL script for compliance.
    
    Ignore comments and documentation. Only check executable code.
    
    File Name: {file_name if file_name else "Unknown"}
    
    {script_content}

    **Check for compliance on the following aspects:**
    1. **Auditability** (Start/End timestamps, row counts, logs)
    2. **Reconcilability** (ETL should have data reconcilability checks)
    3. **Restartability** (Resumes from the point of failure?)
    4. **Exception Handling** (Errors, alerts)

    **For each category, return structured JSON with:**
    - **Audit Result** → (Pass/Fail)
    - **ETL Audit Type** → (Auditability, Reconcilability, Restartability, Exception Handling)
    - **Audit Details/Evidence** → Provide a **detailed justification** with:
    - **Detailed Analysis**: Observations from the script.

    """

    # If additional questions are provided, add them
    if additional_questions:
        prompt += f"\n\nAdditionally, answer the following questions:\n{additional_questions}"

    # Enforce structured JSON format
    prompt += """
    
    **Provide structured JSON output at the end in this format:**
    'so i am using the below to extract output so be serious about generating this output'
    structured_match = re.search(r"```structured-results\n({.*?})\n```", audit_report, re.DOTALL)
    ```structured-results
    {
        "File Name Full Path": "{file_name}",
        "Audit Results": [
            {
                "ETL Audit Type": "Auditability",
                "Audit Result": "Pass/Fail",
                "Audit Details/Evidence": {
                    "Detailed Analysis": "Explain in detail how auditability is implemented or not and what it's lacking."
                }
            },
            {
                "ETL Audit Type": "Reconcilability",
                "Audit Result": "Pass/Fail",
                "Audit Details/Evidence": {
                    "Detailed Analysis": "Explain in detail how reconcilability is implemented or not and what it's lacking."
                }
            },
            {
                "ETL Audit Type": "Restartability",
                "Audit Result": "Pass/Fail",
                "Audit Details/Evidence": {
                    "Detailed Analysis": "Explain in detail how restartability is implemented or not and what it's lacking."
                }
            },
            {
                "ETL Audit Type": "Exception Handling",
                "Audit Result": "Pass/Fail",
                "Audit Details/Evidence": {
                    "Detailed Analysis": "Explain in detail how exception handling is implemented or not and what it's lacking."
                }
            }
        ]
    }
    ```
    **DO NOT** include any extra text outside of the JSON output.
    """

    print("Generated Prompt:")
    print(prompt[:300])  # Print only the first 300 characters for debugging

    try:
        completion = call_genai_api(prompt)  # Call API with retry logic
        print("Received response from GenAI model.")

        audit_report = ""
        for chunk in completion:
            if chunk.choices[0].delta.content is not None:
              print(chunk.choices[0].delta.content, end="")
              audit_report += chunk.choices[0].delta.content
                

        # Extract structured JSON using regex
        structured_match = re.search(r"```structured-results\n({.*?})\n```", audit_report, re.DOTALL)

        if structured_match:
            structured_json_str = structured_match.group(1)
            try:
                structured_results = json.loads(structured_json_str)
                return structured_results  # Return structured JSON
            except json.JSONDecodeError:
                print("[ERROR] Failed to parse structured JSON.")
                return {"error": "Failed to parse structured JSON."}

        print("[WARNING] No structured JSON found! Returning full text instead.")
        return {"error": "AI did not return structured JSON", "raw_text": audit_report}

    except Exception as e:
        print("ERROR OCCURRED in analyze_etl_script:", str(e))
        return {"error": str(e) + "\n\nPlease check the input and try again."}





# output

# ```structured-results
# {
#     "File Name Full Path": "uploads/74611185-19a6-4b9e-82e4-1d5d4467fa21/compliant_python.py",
#     "Audit Results": [
#         {
#             "ETL Audit Type": "Auditability",
#             "Audit Result": "Pass",
#             "Audit Details/Evidence": {
#                 "Detailed Analysis": "The script implements auditability by logging start and end timestamps, row counts, and other relevant information. The logging.basicConfig function is used to configure the logging module, and the logging.info function is used to log important events throughout the ETL process. The script logs the number of rows extracted, transformed, and loaded, providing a clear audit trail. Additionally, the script logs the time taken to complete the ETL process, providing further insight into the performance of the process."
#             }
#         },
#         {
#             "ETL Audit Type": "Reconcilability",
#             "Audit Result": "Fail",
#             "Audit Details/Evidence": {
#                 "Detailed Analysis": "The script does not implement reconcilability checks. Reconcilability checks are used to verify that the data has been accurately transformed and loaded. The script does not compare the number of rows extracted to the number of rows loaded, nor does it check for any data inconsistencies. To implement reconcilability, the script could compare the row counts at each stage of the ETL process and log any discrepancies."
#             }
#         },
#         {
#             "ETL Audit Type": "Restartability",
#             "Audit Result": "Fail",
#             "Audit Details/Evidence": {
#                 "Detailed Analysis": "The script does not implement restartability. Restartability allows the ETL process to resume from the point of failure in the event of an error. The script does not store any state information or checkpoints, and it does not have the ability to resume from a previous point of failure. To implement restartability, the script could store the current state of the ETL process and use this information to resume the process in the event of an error."
#             }
#         },
#         {
#             "ETL Audit Type": "Exception Handling",
#             "Audit Result": "Pass",
#             "Audit Details/Evidence": {
#                 "Detailed Analysis": "The script implements exception handling by catching and logging any exceptions that occur during the ETL process. The script uses try-except blocks to catch exceptions and logs the error message using the logging.error function. This provides a clear audit trail of any errors that occur during the ETL process. However, the script could be improved by providing more detailed error messages and by implementing alerts or notifications in the event of an error."
#             }
#         }
#     ]
# }



