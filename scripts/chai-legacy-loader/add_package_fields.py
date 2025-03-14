#!/usr/bin/env pkgx +python@3.11 uv run

"""
For a csv generated from legacy chai, this script adds the id, created_at, and
updated_at fields to the csv.

Usage:
    chmod +x add-package-fields.py
    ./add-package-fields.py input.csv output.csv [package_manager]
"""

import csv
import sys
import uuid
from datetime import datetime


def process_csv(input_file, output_file, package_manager=None) -> None:
    now = datetime.now().isoformat()
    with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        # Process header row
        header = next(reader, None)
        if header:
            if package_manager:
                writer.writerow(header + [f"{package_manager}_id", f"{package_manager}_created_at", 
                               f"{package_manager}_updated_at", "package_manager"])
            else:
                writer.writerow(header + ["id", "created_at", "updated_at"])
        
        # Process data rows
        for row in reader:
            row_uuid = str(uuid.uuid4())
            if package_manager:
                writer.writerow(row + [row_uuid, now, now, package_manager])
            else:
                writer.writerow(row + [row_uuid, now, now])
    
    print(f"Processed {input_file} → {output_file}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: script.py input.csv output.csv [package_manager]")
        sys.exit(1)
    
    package_manager = sys.argv[3] if len(sys.argv) > 3 else None
    process_csv(sys.argv[1], sys.argv[2], package_manager)
