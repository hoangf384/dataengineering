import logging
from typing import Set

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
logger = logging.getLogger(__name__)

def clean_and_filter_custom_track(
        df: DataFrame,
        allowed_values: Set[str],
        stage: str = "custom_track_filter",
) -> DataFrame:
    """
    Clean and filter custom_track column. Drop rows with invalid custom_track values.

    Parameter
    ----------
    df : DataFrame
        Input DataFrame containing the 'custom_track' column.
    allowed_values : Set[str]
        Set of allowed values for the 'custom_track' column.
    stage : str
        Name of the satge where filtering is perormed.

    Returns
    -------
    DataFrame
        DataFrame with only valid custom_track values.
    """

    is_valid = col("custom_track").isin(list(allowed_values))

    total_count = df.count()
    valid_df = df.filter(is_valid)
    valid_count = valid_df.count()
    invalid_count = total_count - valid_count

    if invalid_count > 0:
        logger.warning(
            f"[{stage}] Dropped {invalid_count} rows with invalid custom_track. "
            f"Retaining {valid_count}/{total_count} rows."
        )

    df = df.filter(is_valid)

    return df


def validate_event_timestamp(
    df: DataFrame, ratio: float = 0.0, stage: str = "normalize_event_time"
) -> None:
    """
    Validate ts column after timeuuid normalization.
    Fail fast if invalid timestamp rate exceeds threshold.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing the 'ts' column.
    ratio : float
        Maximum allowed ratio of invalid timestamps.
    stage : str
        Name of the stage where validation is performed.

    Raises
    ------
    ValueError
        If the invalid timestamp rate exceeds the threshold.
    """

    total = df.count()
    invalid = df.filter(col("ts").isNull()).count()

    if total == 0:
        raise ValueError(f"[{stage}] Input DataFrame is empty")

    invalid_ratio = invalid / total

    if invalid_ratio > ratio:
        raise ValueError(
            f"[{stage}] Invalid ts detected: "
            f"{invalid}/{total} rows ({invalid_ratio:.2%})"
        )