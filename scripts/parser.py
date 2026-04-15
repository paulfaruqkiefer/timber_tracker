import pdfplumber
import re
import pandas as pd
from pathlib import Path

# --- CONFIGURATION ---
# This ensures that when the PDF text is "clipped", we map it back to the full name
CLEANUP_MAP = {
    "05 R5, Pacific S": "05 R5, Pacific Southwest Region",
    "08 R8, Southern Region Forest": "08 R8, Southern Region",
    "11 NFS in N": "11 NFS in North Carolina",
    "15 Helena": "15 Helena-Lewis and Clark National Forest",
    "17 Nez Perce": "17 Nez Perce-Clearwater National Forest",
    "01 R1, Northern Region Forest": "01 R1, Northern Region",
    "02 R2, Rocky Mountain Region Forest": "02 R2, Rocky Mountain Region",
    "03 R3, Southwestern Region Forest": "03 R3, Southwestern Region",
    "04 R4, Intermountain Region Forest": "04 R4, Intermountain Region",
    "06 R6, Pacific N": "06 R6, Pacific Northwest Region",
    "09 R9, Eastern Region Forest": "09 R9, Eastern Region",
    "10 R10, Alaska Region Forest": "10 R10, Alaska Region",
    "05 Mt": "05 Mt. Baker-Snoqualmie National Forest",
    "06 Mt": "06 Mt. Hood National Forest",
    "03 Bitterroot N": "03 Bitterroot National Forest",
    "07 NF Mississippi": "07 National Forests in Mississippi",
    "10 Ozark St": "10 Ozark-St. Francis National Forest",
    "11 NF N": "11 National Forests in North Carolina",
    "12 Pike": "12 Pike-San Isabel National Forest",
    "14 Shasta": "14 Shasta-Trinity National Forest",
    "17 Nez Perce": "17 Nez Perce-Clearwater National Forest",
    "17 Okanogan": "17 Okanogan-Wenatchee National Forest"
}

def parse_ptsar_file(pdf_path):
    results = []
    current_region = "Unknown"
    current_forest = "Region Total"
    num_pattern = r"([\d,]+\.\d{2})"

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Get words with their coordinates to handle messy layouts
            words = page.extract_words()
            
            # Reconstruct lines more intelligently
            lines = page.extract_text().split('\n')
            
            for line in lines:
                # 1. Update Context (Region)
                if "Region:" in line:
                    reg_match = re.search(r"Region:\s*(.+)", line)
                    if reg_match:
                        raw_reg = reg_match.group(1).split("Forest:")[0].strip()
                        current_region = CLEANUP_MAP.get(raw_reg, raw_reg)
                
                # 2. Update Context (Forest) - Improved Regex
                if "Forest:" in line:
                    # We capture EVERYTHING after "Forest:"
                    for_match = re.search(r"Forest:\s*(.+)", line)
                    if for_match:
                        # Split by common trailing labels to avoid grabbing too much
                        raw_forest = for_match.group(1).split("Report:")[0].split("Page:")[0].strip()
                        
                        # Apply Cleanup Map
                        current_forest = CLEANUP_MAP.get(raw_forest, raw_forest)
                
                elif "REGION TOTALS" in line:
                    current_forest = "Region Total"

                # 3. Targeted Data Extraction
                if "TOTAL FY ATTAINMENT:" in line or "TOTAL OFFER VOLUME" in line:
                    label = "Attainment" if "ATTAINMENT" in line else "Offer"
                    matches = re.findall(num_pattern, line)
                    
                    if len(matches) >= 2:
                        results.append({
                            "Year": pdf_path.parent.name,
                            "Region": current_region,
                            "Forest": current_forest,
                            "Metric": label,
                            "MBF": float(matches[-2].replace(',', '')),
                            "CCF": float(matches[-1].replace(',', '')),
                            "Source": pdf_path.name
                        })
    return results

# --- EXECUTION ---
all_extracted_data = []
# Filter for 2006-2025 and ignore 'sw' files
years_of_interest = [str(y) for y in range(2006, 2026)]
target_files = [
    f for f in Path("PTSAR_Reports").rglob("*.pdf") 
    if f.parent.name in years_of_interest 
    and "sw" not in f.name.lower()
]

print(f"Starting extraction on {len(target_files)} files...")

for pdf in target_files:
    try:
        data = parse_ptsar_file(pdf)
        all_extracted_data.extend(data)
    except Exception as e:
        print(f"Error on {pdf}: {e}")

# --- DATA CLEANUP & EXPORT ---
if all_extracted_data:
    df = pd.DataFrame(all_extracted_data)
    
    # Pivot to align Offer and Attainment on one row
    df_pivot = df.pivot_table(
        index=["Year", "Region", "Forest"],
        columns="Metric",
        values=["MBF", "CCF"]
    ).reset_index()

    # Flatten the headers: ('MBF', 'Offer') -> 'MBF_Offer'
    df_pivot.columns = [
        f"{col[0]}_{col[1]}" if col[1] else col[0] 
        for col in df_pivot.columns.values
    ]

    # Sort logically by time and geography
    df_pivot = df_pivot.sort_values(by=["Year", "Region", "Forest"])

    # Final Save
    output_name = "data/PTSAR_2006_2025_master.csv"
    df_pivot.to_csv(output_name, index=False)
    print(f"\nSuccessfully created {output_name} with {len(df_pivot)} records.")
else:
    print("No data was found. Check your file paths.")