import os
import sys
import markdown
from xhtml2pdf import pisa

def convert_html_to_pdf(html_string, pdf_path):
    # open output file for writing (mode b)
    with open(pdf_path, "wb") as result_file:
        # convert HTML to PDF
        pisa_status = pisa.CreatePDF(
            html_string,                # the HTML to convert
            dest=result_file            # file handle to recieve result
        )
    return pisa_status.err

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    
    md_path = os.path.join(parent_dir, "complete_data_manifest.md")
    pdf_path = os.path.join(parent_dir, "complete_data_manifest.pdf")
    
    if not os.path.isfile(md_path):
        print(f"Error: {md_path} not found!")
        sys.exit(1)
        
    print(f"Reading markdown from: {md_path}")
    with open(md_path, "r", encoding="utf-8") as f:
        md_text = f.read()
        
    # Convert markdown to html with table support extension
    print("Converting markdown to HTML...")
    html_content = markdown.markdown(md_text, extensions=['tables', 'fenced_code'])
    
    # Simple CSS styling to make the PDF look highly professional
    html_template = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
    @page {{
        size: A4 portrait;
        margin: 2.2cm 2cm 2.2cm 2cm;
    }}
    body {{
        font-family: Helvetica, Arial, sans-serif;
        font-size: 9.5pt;
        line-height: 1.45;
        color: #2d3748;
    }}
    h1, h2, h3, h4, h5 {{
        color: #1a365d;
        font-family: Helvetica, Arial, sans-serif;
        page-break-after: avoid;
        margin-top: 18pt;
        margin-bottom: 8pt;
    }}
    h1 {{
        font-size: 20pt;
        border-bottom: 2px solid #2b6cb0;
        padding-bottom: 8px;
        margin-top: 0;
    }}
    h2 {{
        font-size: 14pt;
        border-bottom: 1px solid #e2e8f0;
        padding-bottom: 4px;
    }}
    h3 {{
        font-size: 11pt;
    }}
    p {{
        margin-top: 0;
        margin-bottom: 8pt;
    }}
    ul, ol {{
        margin-top: 0;
        margin-bottom: 8pt;
        padding-left: 20px;
    }}
    li {{
        margin-bottom: 4pt;
    }}
    table {{
        width: 100%;
        border-collapse: collapse;
        margin-top: 12pt;
        margin-bottom: 12pt;
        page-break-inside: avoid;
    }}
    th, td {{
        border: 1px solid #cbd5e0;
        padding: 6px 8px;
        text-align: left;
        font-size: 8pt;
    }}
    th {{
        background-color: #ebf8ff;
        font-weight: bold;
        color: #2b6cb0;
    }}
    tr:nth-child(even) {{
        background-color: #f7fafc;
    }}
    code {{
        font-family: Courier, monospace;
        font-size: 8pt;
        background-color: #edf2f7;
        padding: 1px 3px;
        border-radius: 3px;
    }}
    pre {{
        font-family: Courier, monospace;
        font-size: 8pt;
        background-color: #edf2f7;
        padding: 10px;
        border: 1px solid #e2e8f0;
        margin-top: 10pt;
        margin-bottom: 10pt;
        page-break-inside: avoid;
    }}
    pre code {{
        background-color: transparent;
        padding: 0;
    }}
    blockquote {{
        margin: 12pt 0;
        padding: 10px 15px;
        background-color: #ebf8ff;
        border-left: 4px solid #3182ce;
        font-size: 9pt;
        color: #2b6cb0;
    }}
</style>
</head>
<body>
    {html_content}
</body>
</html>
"""
    
    print(f"Writing PDF to: {pdf_path}")
    err = convert_html_to_pdf(html_template, pdf_path)
    
    if err:
        print(f"Error converting to PDF: {err}")
        sys.exit(1)
        
    print("PDF conversion completed successfully!")

if __name__ == "__main__":
    main()
