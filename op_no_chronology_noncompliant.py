import argparse
import re
from pathlib import Path

import pandas as pd


DEFAULT_INPUT = "/home/harold-coder/Desktop/compliance/categories NON comp (1).xlsx"
DEFAULT_SHEET = "Non compliant with OP"


def normalize_label(text: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", str(text).lower()).split())


def find_op_columns(columns: list[str]) -> tuple[str | None, str | None, str | None]:
    combined_col = None
    op_no_col = None
    name_col = None

    combined_target = normalize_label("OP No and Name")
    op_no_target = normalize_label("OP No")
    name_target = normalize_label("Name")

    for col in columns:
        n = normalize_label(col)
        if combined_col is None and (n == combined_target or ("op" in n and "no" in n and "name" in n)):
            combined_col = col
        if op_no_col is None and (n == op_no_target or ("op" in n and "no" in n)):
            op_no_col = col
        if name_col is None and n == name_target:
            name_col = col

    # fallback for name-like column
    if name_col is None:
        for col in columns:
            n = normalize_label(col)
            if "name" in n:
                name_col = col
                break

    return combined_col, op_no_col, name_col


def extract_op_no(text: object) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    # Capture OP token that starts with 218/ with optional OP prefix (OP., OP , OP-), e.g.:
    # OP.218/051/2003/049/2673
    # 218/051/2002/0126
    match = re.search(r"(?i)\b(?:OP[\.\s-]*)?218/\d+/[A-Z0-9][A-Z0-9/\-_.]*", raw)
    if match:
        return match.group(0).rstrip(".,;:")
    return ""


def extract_registration_year(op_no: str) -> int | None:
    raw = str(op_no or "").strip()
    if not raw:
        return None

    # Remove optional OP prefix and keep the token that starts with 218/.
    normalized = re.sub(r"(?i)^\s*op[\.\s-]*", "", raw)
    token_match = re.search(r"218/\d+/[A-Z0-9][A-Z0-9/\-_.]*", normalized, flags=re.IGNORECASE)
    token = token_match.group(0) if token_match else normalized
    if not token.startswith("218/"):
        return None

    parts = [p.strip() for p in token.split("/")]
    if len(parts) < 3:
        return None

    # Year comes after the second forward slash, and may be embedded like 2016-063.
    year_match = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", parts[2])
    if not year_match:
        return None
    year_token = year_match.group(1)
    if not re.fullmatch(r"(19|20)\d{2}", year_token):
        return None
    year = int(year_token)
    if year < 1900 or year > 2100:
        return None
    return year


def sort_op_rows(
    df: pd.DataFrame,
    combined_col: str | None,
    op_no_col: str | None,
    name_col: str | None,
) -> pd.DataFrame:
    keep_cols: list[str] = []
    if combined_col:
        keep_cols.append(combined_col)
    if op_no_col and op_no_col not in keep_cols:
        keep_cols.append(op_no_col)
    if name_col and name_col not in keep_cols:
        keep_cols.append(name_col)
    work = df[keep_cols].copy()

    if combined_col:
        work["op_no"] = work[combined_col].apply(extract_op_no)
    elif op_no_col:
        work["op_no"] = work[op_no_col].apply(extract_op_no)
    else:
        work["op_no"] = ""

    if name_col:
        work["name"] = work[name_col].astype(str).str.strip()
    else:
        work["name"] = ""

    # Keep the visible OP No column in sync with extracted/normalized OP values.
    # This ensures valid entries appear in output even when helper columns are dropped.
    if op_no_col:
        work[op_no_col] = work["op_no"]

    if not combined_col:
        work["OP No and Name"] = work["op_no"].astype(str).str.strip()
        has_name = work["name"].astype(str).str.strip() != ""
        work.loc[has_name, "OP No and Name"] = (
            work.loc[has_name, "op_no"].astype(str).str.strip()
            + " - "
            + work.loc[has_name, "name"].astype(str).str.strip()
        )

    work["registration_year"] = work["op_no"].apply(extract_registration_year)
    work["is_valid_op_no"] = work["registration_year"].notna()
    work["sort_invalid"] = ~work["is_valid_op_no"]
    work["sort_invalid_empty_op"] = work["op_no"].astype(str).str.strip().eq("")
    work["sort_year"] = work["registration_year"].fillna(9999).astype(int)
    work = work.sort_values(
        by=["sort_invalid", "sort_year", "sort_invalid_empty_op", "op_no"],
        kind="mergesort",
    ).reset_index(drop=True)
    out_cols = ["OP No and Name", "op_no", "registration_year", "is_valid_op_no"]
    if combined_col:
        work["OP No and Name"] = work[combined_col]
    if op_no_col and op_no_col not in out_cols:
        out_cols.insert(1, op_no_col)
    if name_col and name_col not in out_cols:
        out_cols.insert(2 if op_no_col else 1, name_col)
    return work[out_cols]


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Extract 'OP No and Name', sort by registration year from OP No "
            "(3rd slash-delimited segment), and push malformed/missing OP numbers to bottom."
        )
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input .xlsx file path")
    parser.add_argument(
        "--output",
        default="op_no_chronology_sorted_noncompliant.xlsx",
        help="Output .xlsx file path",
    )
    parser.add_argument(
        "--sheet",
        default=DEFAULT_SHEET,
        help=f"Sheet name to process (default: '{DEFAULT_SHEET}').",
    )
    args = parser.parse_args()

    input_path = Path(args.input).expanduser()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    sheet_name = args.sheet if args.sheet else DEFAULT_SHEET
    df = pd.read_excel(input_path, sheet_name=sheet_name, dtype=str, keep_default_na=False)
    combined_col, op_no_col, name_col = find_op_columns([str(c) for c in df.columns])
    if combined_col is None and op_no_col is None:
        raise ValueError("Neither 'OP No and Name' nor 'OP No' column was found.")
    sorted_df = sort_op_rows(df, combined_col, op_no_col, name_col)

    output_path = Path(args.output).expanduser()
    # User-requested export shaping: remove output columns A, D, E, F.
    # In 0-based index terms this is: 0, 3, 4, 5.
    drop_idx = {0, 3, 4, 5}
    keep_cols = [c for i, c in enumerate(sorted_df.columns) if i not in drop_idx]
    export_df = sorted_df[keep_cols].copy()

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="chronological")

    print(f"Done. Wrote sorted output to: {output_path}")
    if combined_col:
        print(f"Detected combined OP column: {combined_col}")
    else:
        print(f"Detected OP/Name columns: OP='{op_no_col}', Name='{name_col or ''}'")
    valid = int(sorted_df["is_valid_op_no"].sum())
    invalid = int((~sorted_df["is_valid_op_no"]).sum())
    print(f"Valid OP rows: {valid} | Invalid/malformed rows moved to bottom: {invalid}")


if __name__ == "__main__":
    main()
