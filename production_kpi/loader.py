import pandas as pd


def load_data(filepath: str) -> pd.DataFrame:
    """
    Loads production line data from a CSV file and returns a clean DataFrame.

    Parameters
    ----------
    filepath : str
        Path to the CSV file.

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame with columns:
        - production_line_id (str)
        - status (str)
        - timestamp (datetime)
        Sorted by production_line_id and timestamp ascending.
    """
    df = pd.read_csv(filepath)

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = df.sort_values(["production_line_id", "timestamp"]).reset_index(drop=True)

    return df