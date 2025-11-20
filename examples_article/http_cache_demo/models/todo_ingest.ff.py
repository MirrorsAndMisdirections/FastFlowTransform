import pandas as pd
from fastflowtransform import model
from fastflowtransform.api.http import get_df


# 1. Define the Paginator
# This function runs after every request to determine what to do next.
def offset_paginator(url, params, response_json):
    # If the API returns an empty list, we are done.
    if not response_json:
        return None

    # Otherwise, increment the page number
    current_page = params.get("_page", 1)
    if current_page >= 2:
        return None
    next_params = dict(params or {})
    next_params["_page"] = current_page + 1
    return {"next_request": {"params": next_params}}


@model(name="todos_ingest")
def fetch_todos() -> pd.DataFrame:
    # 2. get_df handles the HTTP calls, caching, and conversion
    df = get_df(
        url="https://jsonplaceholder.typicode.com/todos",
        params={"_page": 1, "_limit": 10},  # Start at page 1
        paginator=offset_paginator,
        # record_path is None because the root of the JSON is the list itself
        record_path=None,
    )

    # 3. Apply transformation logic
    # If we change THIS logic later, FFT won't re-fetch the API!

    # Example: Mark high-priority items locally
    df["priority"] = df["title"].apply(lambda x: "HIGH" if "delectus" in x else "NORMAL")

    # New Logic: Filter rows
    df = df[df["completed"] == False]

    # New Logic: Uppercase titles
    df["title"] = df["title"].str.upper()

    return df
