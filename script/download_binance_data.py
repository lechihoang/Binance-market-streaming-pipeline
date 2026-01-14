#!/usr/bin/env python3
"""Download 1-minute klines from Binance Public Data."""

import subprocess
import sys
import zipfile
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from util.constant import DEFAULT_SYMBOL  # noqa: E402

BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"
INTERVAL = "1m"
DATA_DIR = Path(__file__).parent.parent / "data" / "historical"


def get_download_month():
    # Default to November 2025 as requested
    return 2025, 11


def clear_data_dir():
    """Clear existing data to ensure clean training state."""
    print(f"Cleaning {DATA_DIR}...")
    for f in DATA_DIR.glob("*.csv"):
        f.unlink()
    for f in DATA_DIR.glob("*.zip"):
        f.unlink()


def download_symbol(symbol: str, year: int, month: int) -> bool:
    filename = f"{symbol}-{INTERVAL}-{year}-{month:02d}"
    url = f"{BASE_URL}/{symbol}/{INTERVAL}/{filename}.zip"
    zip_path = DATA_DIR / f"{filename}.zip"
    csv_path = DATA_DIR / f"{filename}.csv"

    if csv_path.exists():
        print(f"  {symbol}: Already exists, skipping")
        return True

    print(f"  {symbol}: Downloading...")
    try:
        result = subprocess.run(["curl", "-sL", "-o", str(zip_path), url], capture_output=True, text=True, timeout=120)

        if result.returncode != 0:
            print(f"  {symbol}: FAILED - curl error")
            return False

        if not zip_path.exists() or zip_path.stat().st_size < 1000:
            print(f"  {symbol}: FAILED - file too small or not found")
            if zip_path.exists():
                zip_path.unlink()
            return False

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(DATA_DIR)

        zip_path.unlink()
        size_mb = csv_path.stat().st_size / 1024 / 1024
        print(f"  {symbol}: OK ({size_mb:.1f} MB)")
        return True
    except Exception as e:
        print(f"  {symbol}: FAILED - {e}")
        if zip_path.exists():
            zip_path.unlink()
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Download Binance historical klines")
    parser.add_argument("--year", type=int, help="Year (default: previous month)")
    parser.add_argument("--month", type=int, help="Month (default: previous month)")
    parser.add_argument("--symbols", type=str, help="Comma-separated symbols")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    clear_data_dir()

    if args.year and args.month:
        year, month = args.year, args.month
    else:
        year, month = get_download_month()

    symbols = args.symbols.split(",") if args.symbols else DEFAULT_SYMBOL

    print(f"Downloading {len(symbols)} symbols for {year}-{month:02d}")
    print(f"Output: {DATA_DIR}\n")

    success = 0
    for symbol in symbols:
        if download_symbol(symbol.strip().upper(), year, month):
            success += 1

    print(f"\nDone: {success}/{len(symbols)} symbols downloaded")

    if success > 0:
        print("\nNext steps:")
        print("  1. Import to PostgreSQL: python3 script/import_historical_data.py")
        print("  2. Train model: jupyter notebook notebooks/train_price_predictor.ipynb")


if __name__ == "__main__":
    main()
