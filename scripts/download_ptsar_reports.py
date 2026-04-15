import requests
import os
import time
from pathlib import Path

# --- CONFIGURATION ---
BASE_URL = "https://www.fs.usda.gov/forestmanagement/documents/ptsar"
NEW_BASE_URL = "https://www.fs.usda.gov/sites/default/files"
REGIONS = ["R01", "R02", "R03", "R04", "R05", "R06", "R08", "R09", "R10", "SW"]
START_YEAR = 2000
END_YEAR = 2025
DOWNLOAD_DIR = Path("PTSAR_Reports")

def download_reports():
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    
    for year in range(START_YEAR, END_YEAR + 1):
        year_dir = DOWNLOAD_DIR / str(year)
        year_dir.mkdir(exist_ok=True)
        
        for region in REGIONS:
            short_region = region.replace("R0", "R") if "R10" not in region else region
            num_only = region.replace("R", "")
            
            filenames = []
            current_base = f"{BASE_URL}/{year}"

            if year == 2025:
                current_base = NEW_BASE_URL
                filenames.append(f"{year}-q4-ptsar-{region.lower()}.pdf")
                filenames.append(f"{year}-q4-ptsar-{short_region.lower()}.pdf")
                if region == "SW":
                    filenames.append(f"{year}-q4-ptsar-servicewide.pdf")
            
            elif year >= 2012:
                # Era 1: Modern
                filenames.append(f"{year}_Q1-Q4_PTSAR_{region}.pdf")
                filenames.append(f"{year}_Q4_PTSAR_{region}.pdf")
                filenames.append(f"{year}_Q1-Q4_PTSAR_{short_region}.pdf")
                filenames.append(f"{year}_Q4_PTSAR_{short_region}.pdf")
                # ADDED FIX for 2022/2023 Servicewide links
                if region == "SW":
                    filenames.append(f"{year}_Q1-Q4_PTSAR_Servicewide.pdf")
                    filenames.append(f"{year}_Q4_PTSAR_Servicewide.pdf")
            
            else:
                # Era 2: Legacy
                filenames.append(f"{year}_ptsar_{region}.pdf")
                filenames.append(f"{year}_ptsar_{short_region}.pdf")
                filenames.append(f"{year}_Q1-Q4_PTSAR_{region}.pdf")
                filenames.append(f"{year}_Q1-Q4_PTSAR_{short_region}.pdf")
                filenames.append(f"{year}_PTSR202R_{num_only}.pdf")
                filenames.append(f"{year}_PTSR202R-{num_only}.pdf")
                filenames.append(f"{year}_PTSR202S.pdf") 
                filenames.append(f"Q1_Q4_{year}_PTSAR_{short_region}.pdf")

            success = False
            for fname in filenames:
                url = f"{current_base}/{fname}"
                save_path = year_dir / fname
                
                if save_path.exists() and save_path.stat().st_size > 1000:
                    success = True
                    break
                
                try:
                    headers = {'User-Agent': 'Mozilla/5.0'}
                    response = requests.get(url, headers=headers, timeout=15)
                    
                    if response.status_code == 200 and b"PDF" in response.content[:10]:
                        with open(save_path, "wb") as f:
                            f.write(response.content)
                        print(f"Downloaded: {year} {region} -> {fname}")
                        success = True
                        time.sleep(0.3) 
                        break 
                except Exception:
                    pass
            
            if not success:
                print(f"FAILED to find report for {year} {region}")

if __name__ == "__main__":
    download_reports()