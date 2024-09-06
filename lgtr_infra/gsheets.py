import contextlib
import logging
from collections.abc import Mapping
from dataclasses import asdict
from typing import Any, Tuple

import google.auth
import gspread
import numpy as np
import pandas as pd
from gspread.utils import rowcol_to_a1

from lgtr_infra.sheets import df_match_columns, normalize_column_name

logger = logging.getLogger(__name__)


def create_default_client():
    creds, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/spreadsheets'])
    return gspread.authorize(creds, http_client=gspread.BackOffHTTPClient)


def open_gsheet(ss_url_or_id: str, sheet_name: str = None):
    ss = open_gssheet(ss_url_or_id)

    if sheet_name:
        ws = ss.worksheet(sheet_name)
    else:
        ws = ss.get_worksheet_by_id(sheet_id)

    return ws


def open_gssheet(ss_url_or_id: str):
    logger.info(f'Opening Google Sheet: {ss_url_or_id}')
    gspread_client = create_default_client()
    ss_id, _ = parse_spreadsheet_and_sheet_url(ss_url_or_id)

    return gspread_client.open_by_key(ss_id)


def parse_spreadsheet_and_sheet_url(url: str) -> Tuple[str, str]:
    if not url.startswith("https://docs.google.com/spreadsheets/d/"):
        # Assume it's the spreadsheet ID if not a URL
        return url, None

    parts = url.split("/")
    ss_id, sheet_id = None, None

    with contextlib.suppress(Exception):
        ss_id = parts[5]

    with contextlib.suppress(Exception):
        sheet_id = parts[-1].split("=")[-1]

    return ss_id, sheet_id


def df_from_gsheet(ss_url_or_id: str | gspread.Worksheet, sheet_name: str = None, *, transpose=False) -> pd.DataFrame:
    if isinstance(ss_url_or_id, gspread.Worksheet):
        ws = ss_url_or_id
    else:
        ws = open_gsheet(ss_url_or_id, sheet_name)

    values = ws.get_all_values()

    if transpose:
        values = [list(x) for x in zip(*values)]

    df = pd.DataFrame(values[1:], columns=values[0])

    # For all string values, strip whitespace
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    return df


def append_to_gsheet(
    ws: gspread.Worksheet, records: pd.DataFrame | list[Any], *,
    match_columns: bool = True, columns_ws: list[str] = None,
):
    if isinstance(records, list):
        # List of dataclasses
        records = [asdict(record) for record in records]
        df = pd.DataFrame.from_records(records)

    # Values to append to the sheet
    values_append = df.values.tolist()

    # Match columns if required and if headers are present in the sheet
    if match_columns:
        if columns_ws is None:
            columns_ws = ws.row_values(1)

        if len(columns_ws) > 0:
            df = df_match_columns(df, columns_ws)
        else:
            # Create headers if they don't exist
            values_append = [columns_ws] + values_append

    ws.append_rows(values_append, value_input_option='USER_ENTERED')


def update_gsheet_rows(
    ws: gspread.Worksheet, data_rows_indices: list[int] | None, records: list[Mapping | Any | None], *,
    match_columns: bool = True, columns_ws: list[str] = None, columns_include: list[str] = None,
):
    """
        Update a single row in the sheet with the record data
        :param data_row_index: 0-based index of the row to update (not including headers)
    """
    # Remove None records
    if data_rows_indices is not None:
        data_rows_indices = [
            row_index for i, row_index in enumerate(data_rows_indices) 
            if records[i] is not None
        ]
    else:
        i_next_row = len(ws.get_all_values()) - 1
        assert i_next_row >= 0, "Sheet must have at least one row (headers)"
        data_rows_indices = [i_next_row + i for i in range(len(records)) if records[i] is not None]

    records = [r for r in records if r is not None]

    # Support for dataclasses
    for i, record in enumerate(records):
        if not isinstance(record, Mapping) and record is not None:
            records[i] = asdict(record)

    df_rows = pd.DataFrame.from_records(records)

    # If columns_include is provided, only include those columns
    if columns_include:
        df_rows = df_rows[columns_include]

    numeric_cols = df_rows.select_dtypes(include=np.number).columns
    for col in numeric_cols:
        df_rows[col] = pd.to_numeric(
            df_rows[col], errors='coerce', downcast='float')

    columns_include = columns_include or df_rows.columns
    columns_include = [normalize_column_name(c) for c in columns_include]
    if match_columns:
        if columns_ws is None:
            columns_ws = ws.row_values(1)
            columns_ws = [normalize_column_name(c) for c in columns_ws]

        if len(columns_ws) > 0:
            df_rows = df_match_columns(df_rows, columns_ws)

    # DataFrame must not contain NaNs, it will fail the batch update
    df_rows = df_rows.fillna('')

    # Prepare the batch update
    list_updates = []
    for i_row, row_index in enumerate(data_rows_indices):
        if records[i] is None:
            continue

        # list_updates += [
        #     {
        #         # One based index, skip headers (+2)
        #         'range': f'A{row_index + 2}',
        #         'values': [df_rows.iloc[i].values.tolist()]
        #     }
        # ]

        # Don't update entire row, only the columns that are being updated
        for i_col, col_current in enumerate(columns_ws):
            if col_current in columns_include:
                list_updates += [
                    {
                        'range': rowcol_to_a1(row_index + 2, i_col + 1),
                        'values': [[df_rows.iloc[i_row][col_current]]]
                    }
                ]

    # Batch update
    logger.info(f'Updating rows: {[r + 2 for r in data_rows_indices]}')
    ws.batch_update(list_updates)
