import dataclasses
import re
from collections.abc import Iterator
from typing import Any, Callable, Iterable, Optional, Tuple, Type, TypeVar

import dacite
import pandas as pd


T = TypeVar("T")


def normalize_column_name(name: str, *, keep_brackets=False) -> str:
    replace_chars = {
        "\n": " ",
        "\t": " ",
        "-": "_",
    }

    if not keep_brackets:
        # Remove contents of brackets [..] (..) - using Regex
        name = re.sub(r"\[.*?\]", "", name)
        name = re.sub(r"\(.*?\)", "", name)
    else:
        replace_chars.update({
            "[": "",
            "]": "",
            "(": "",
            ")": "",
        })

    for k, v in replace_chars.items():
        name = name.replace(k, v)

    # Replace multiple spaces with single space, and spaces with underscores
    name = " ".join(name.split())
    name = name.replace(" ", "_")
    name = name.lower()

    return name


def df_preprocess(df: pd.DataFrame, keep_brackets=False) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_column_name(c, keep_brackets=keep_brackets) for c in df.columns]
    return df


def df_match_columns(df: pd.DataFrame, columns_ref: list[str], normalize=True) -> pd.DataFrame:
    columns_df = df.columns.tolist()

    if normalize:
        # Get first row of sheet as header
        columns_df = [normalize_column_name(c) for c in columns_df]
        columns_ref = [normalize_column_name(c) for c in columns_ref]

    # Match the order of columns in df to the sheet
    columns_df_order = [
        columns_df.index(c) if c in columns_df else None
        for c in columns_ref
    ]

    # Create a new DataFrame with the columns in the order of the sheet,
    # fill missing columns with empty strings
    df = pd.DataFrame(
        [
            [row[i] if i is not None else "" for i in columns_df_order]
            for row in df.values
        ],
        columns=[columns_df[i] if i is not None else "Unknown" for i in columns_df_order]
    )

    return df


def df_from_csv(path_csv: str) -> list[T]:
    df = pd.read_csv(path_csv, na_filter=False)
    return df


def iterrows_typed(df: pd.DataFrame, cls: Type[T]) -> Iterator[Tuple[int, T]]:
    for i_row, row in df.iterrows():
        yield i_row, dacite.from_dict(cls, row.to_dict())


def get_basic_type(t: Type) -> Type:
    """Get the basic type from a type with optional or Union"""
    return t.__args__[0] if hasattr(t, "__args__") else t


def get_dataclass_types(cls: Type[T]) -> dict[str, Type]:
    return {field.name: get_basic_type(field.type) for field in dataclasses.fields(cls)}


def records_from_df(record_type: Type[T], df: pd.DataFrame) -> list[T]:
    df = df_preprocess(df)

    # Change types of columns in the DataFrame to match the dataclass
    for field in dataclasses.fields(record_type):
        if field.name in df.columns:
            if get_basic_type(field.type) in (int, float):
                df[field.name] = pd.to_numeric(df[field.name], errors="coerce").astype(float)
                df[field.name] = df[field.name].map(lambda x: None if pd.isna(x) else x)

    return list([record for _, record in iterrows_typed(df, record_type)])


def find_one(objects: Iterable[T], predicate: Callable[[T], Any], *, get_first=False) -> Optional[T]:
    matches = [(i, obj) for i, obj in enumerate(objects) if predicate(obj)]

    try:
        if get_first:
            return next(iter(matches), None)
        else:
            if len(matches) > 1:
                raise ValueError("Multiple records found")

            return matches[0]

    except (StopIteration, IndexError):
        return None
