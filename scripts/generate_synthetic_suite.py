import os
import zipfile
import csv
import shutil
import time

def create_pipe_file(filepath, dataset, kind, feed, date, seq, headers, rows):
    with open(filepath, 'w') as f:
        if kind == 'PRC':
            f.write(f"{dataset}|PRC|{feed}|{date}|{seq}|1|{len(rows)}|\n")
        else:
            f.write(f"{dataset}|{kind}|{feed}|{date}|{seq}|{len(rows)}|\n")
        header_str = "|".join(headers) + "|"
        f.write(header_str + "\n")
        for row in rows:
            row_str = "|".join([str(v) if v is not None else "" for v in row]) + "|"
            f.write(row_str + "\n")

def create_zip(zip_path, file_to_zip):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.write(file_to_zip, os.path.basename(file_to_zip))
    os.remove(file_to_zip)

def create_csv(filepath, headers, rows):
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

def main(feed_suffix="99999"):
    root_dir = "/Users/sa/Downloads/LSEG/lseg-ingest/test-input"
    base_dir = os.path.join(root_dir, "synthetic")
    job2_dir = os.path.join(root_dir, "job2")
    
    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(job2_dir, exist_ok=True)
    
    date = "20260502"
    
    # --- ORGS ---
    org_headers = ["Action", "Entity_ID", "Issuer_Name", "Country_of_Incorporation"]
    create_pipe_file(os.path.join(base_dir, f"Organization.INT.{feed_suffix}.{date}.1.1.1.txt"), "Organization", "INT", feed_suffix, date, 1, 
                     org_headers, [["I", "ORG_1", "Base Org 1", "US"]])
    create_zip(os.path.join(base_dir, f"Organization.INT.{feed_suffix}.{date}.1.1.1.txt.zip"), os.path.join(base_dir, f"Organization.INT.{feed_suffix}.{date}.1.1.1.txt"))
    
    # --- ASSETS ---
    asset_headers = ["Action", "Asset_ID", "Entity_ID", "Security_Long_Description"]
    create_pipe_file(os.path.join(base_dir, f"EIS_INT_US_ASSETS.INT.{feed_suffix}.{date}.1.1.1.txt"), "EIS_INT_US_ASSETS", "INT", feed_suffix, date, 1,
                     asset_headers, [["I", "AST_1", "ORG_1", "Asset 1 Desc"]])
    create_zip(os.path.join(base_dir, f"EIS_INT_US_ASSETS.INT.{feed_suffix}.{date}.1.1.1.txt.zip"), os.path.join(base_dir, f"EIS_INT_US_ASSETS.INT.{feed_suffix}.{date}.1.1.1.txt"))

    # --- QUOTES ---
    quote_headers = ["Action", "Quote_ID", "Asset_ID", "RIC", "Currency_Code"]
    create_pipe_file(os.path.join(base_dir, f"EIS_INT_US_QUOTE.INT.{feed_suffix}.{date}.1.1.1.txt"), "EIS_INT_US_QUOTE", "INT", feed_suffix, date, 1,
                     quote_headers, [["I", "QT_3", None, "GOOG.O", "USD"]])
    create_zip(os.path.join(base_dir, f"EIS_INT_US_QUOTE.INT.{feed_suffix}.{date}.1.1.1.txt.zip"), os.path.join(base_dir, f"EIS_INT_US_QUOTE.INT.{feed_suffix}.{date}.1.1.1.txt"))
    create_pipe_file(os.path.join(base_dir, f"EIS_DELTA_US_QUOTE.REF.{feed_suffix}.{date}.2.1.1.txt"), "EIS_DELTA_US_QUOTE", "REF", feed_suffix, date, 2,
                     quote_headers, [["I", "QT_3", "AST_1", "GOOG.O", "USD"]])
    create_zip(os.path.join(base_dir, f"EIS_DELTA_US_QUOTE.REF.{feed_suffix}.{date}.2.1.1.txt.zip"), os.path.join(base_dir, f"EIS_DELTA_US_QUOTE.REF.{feed_suffix}.{date}.2.1.1.txt"))

    # --- PRICING ---
    pricing_headers = ["Quote_ID", "Trade_Date", "Close_Price"]
    
    # Small for interleaving
    rows_s = [[f"QT_S_{i}", "2026-05-02", "100.00"] for i in range(1000)]
    p_s = f"EIS_INT_ASIA_PRICING.PRC.S_{feed_suffix}.20260502.1.1.1.1.txt"
    create_pipe_file(os.path.join(base_dir, p_s), "EIS_INT_ASIA_PRICING", "PRC", f"S_{feed_suffix}", date, 1, pricing_headers, rows_s)
    create_zip(os.path.join(base_dir, p_s + ".zip"), os.path.join(base_dir, p_s))
    
    # Huge for stop-signal (500,000 rows)
    rows_h = [[f"QT_H_{i}", "2026-05-02", "100.00"] for i in range(500000)]
    p_h = f"EIS_INT_ASIA_PRICING.PRC.H_{feed_suffix}.20260502.1.1.1.1.txt"
    create_pipe_file(os.path.join(base_dir, p_h), "EIS_INT_ASIA_PRICING", "PRC", f"H_{feed_suffix}", date, 1, pricing_headers, rows_h)
    create_zip(os.path.join(base_dir, p_h + ".zip"), os.path.join(base_dir, p_h))

    # --- JOB 2 DATA ---
    create_pipe_file(os.path.join(job2_dir, f"Organization.INT.J2_{feed_suffix}.{date}.10.1.1.txt"), "Organization", "INT", f"J2_{feed_suffix}", date, 10, 
                     org_headers, [["I", "ORG_JOB2", "Job 2 Org", "DE"]])
    create_zip(os.path.join(job2_dir, f"Organization.INT.J2_{feed_suffix}.{date}.10.1.1.txt.zip"), os.path.join(job2_dir, f"Organization.INT.J2_{feed_suffix}.{date}.10.1.1.txt"))

    # --- DSS BONDS ---
    bond_headers = ["ISIN", "Instrument ID", "Instrument ID Type", "RIC", "Issuer Name"]
    create_csv(os.path.join(base_dir, f"SG_HK_Bonds_{date}_{feed_suffix}.csv"), bond_headers, [
        ["ISIN_B1", "INST_1", "RIC", "BOND1.HK", "Issuer HK 1"],
        ["ISIN_B1", "INST_1", "RIC", "BOND1.HK", "Issuer HK 1 UPDATED"]
    ])

    print(f"Synthetic suite updated with feed {feed_suffix} in {root_dir}")

if __name__ == "__main__":
    main()
