# python module
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple

# pyspark module
from pyspark.sql import udf, DataFrame
from pyspark.sql import functions as F

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# local module
from config.settings import NUM_100NS_INTERVALS_SINCE_UUID_EPOCH
from process.validations import validate_event_timestamp, clean_and_filter_custom_track


logger = logging.getLogger(__name__)


@udf(returnType=StringType())
def timeuuid_to_ts(uuid_str: Optional[str]) -> Optional[str]:
    """
    User Defined Function (UDF) to convert Cassandra UUID to timestamp string.

    Uses python built-in module (uuid) to extract the timestamp.

    Parameters
    ----------
    uuid_str : str or None
        The UUID string from Cassandra (TimeUUID version 1).

    Returns
    -------
    str or None
        The formatted timestamp string '%Y-%m-%d %H:%M:%S' if valid,
        otherwise None.
    """
    if not uuid_str:
        return None

    try:
        u = uuid.UUID(uuid_str)
        if u.version != 1:
            return None

        ts = (u.time - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 1e7
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    except ValueError as v:
        logger.error("Failed to parse timeuuid %s: %s", uuid_str, v)
        return None
    except Exception as e:
        logger.warning("Failed to parse timeuuid %s: %s", uuid_str, e)
        return None


def normalize_event_time(df: DataFrame) -> DataFrame:
    """
    Adds a `ts` column to the DataFrame by converting `create_time`.

    This function applies the `timeuuid_to_ts (@UDF)` to the `create_time` column.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing the `create_time` column.

    Returns
    -------
    DataFrame
        DataFrame with the new `ts` column added.
    """
    df = df.withColumn("ts", timeuuid_to_ts(col("create_time")))
    return df


def build_base_event(df: DataFrame) -> DataFrame:
    """
    Get useful events data

    Parameters
    ----------
    df: DataFrame

    Returns
    -------
    DataFrame
    """

    df = df.select(
        "create_time",
        "job_id",
        "ts",  # -> tự thêm chỉ số này
        "custom_track",
        "bid",
        "campaign_id",
        "group_id",
        "publisher_id",
    )

    return df


def aggregate_metrics_single_pass(df: DataFrame) -> DataFrame:
    """
    aggregate metric single pass

    Parameters
    ----------
        df: DataFrame

    Returns
    -------
        DataFrame
    """

    # 1. define conditions
    is_click = col("custom_track") == "click"
    is_conversion = col("custom_track") == "conversion"
    is_qualified = col("custom_track") == "qualified"
    is_unqualified = col("custom_track") == "unqualified"
    # is_alive = col("custom_track") == "alive"
    # is_interview = col("custom_track") == "interview_scheduled"

    # 2. implement grouby and aggregate function together
    df = df.groupBy(
        F.to_date("ts").alias("dates"),
        F.hour("ts").alias("hours"),
        "job_id",
        "publisher_id",
        "campaign_id",
        "group_id",
    ).agg(
        # --- Metrics cho Click (Có tính tiền) ---
        # COUNT(IF(track='click', 1, 0))
        F.sum(F.when(is_click, 1).otherwise(0)).alias("clicks"),
        # AVG(IF(track='click', bid, NULL)) -> NULL để không ảnh hưởng trung bình
        F.round(F.avg(F.when(is_click, col("bid"))), 2).alias("bid_set"),
        # SUM(IF(track='click', bid, 0))
        F.sum(F.when(is_click, col("bid")).otherwise(0)).alias("spend_hour"),
        # --- Metrics cho các loại khác (Chỉ đếm) ---
        F.sum(F.when(is_conversion, 1).otherwise(0)).alias("conversion"),
        F.sum(F.when(is_qualified, 1).otherwise(0)).alias("qualified_application"),
        F.sum(F.when(is_unqualified, 1).otherwise(0)).alias("disqualified_application"),
    )

    return df


def enrich_job_dimension(
    df: DataFrame, jobs_df: DataFrame
) -> DataFrame:
    """
    Enrich job dimension

    Parameters
    ----------
    df : DataFrame
        Input DataFrame
    jobs_df : DataFrame
        Job dimension DataFrame

    Returns
    -------
    DataFrame
        Enriched DataFrame
    """
    df = (
        df.join(jobs_df, on="job_id", how="left")
        .drop(jobs_df.campaign_id)
        .drop(jobs_df.group_id)
    )

    return df


def add_metadata(df: DataFrame) -> DataFrame:
    """
    add column procesed_at by using `current_timestamp()`

    Parameters
    ----------
    df: DataFrame
        Input DataFrame

    Returns
    -------
    DataFrame

    """
    df = df.withColumn("processed_at", F.current_timestamp()).withColumn(
        "sources", F.lit("Cassandra")
    )
    return df


def transform_data(df: DataFrame, jobs_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Transform data function. This function is used to manage the transformation process of the input DataFrame.

    Parameters
    ----------
    df: DataFrame
        Input DataFrame
    jobs_df: DataFrame
        Input job dimension DataFrame

    Returns
    -------
    DataFrame
        Transformed DataFrame
    """

    logger.info("Normalizing event time")
    df = normalize_event_time(df)

    logger.info("Validating timestamp")
    validate_event_timestamp(df, 0.1, "timestamp_validation")
    df = df.filter(col("ts").isNotNull())

    logger.info("Building base event")
    base_df = build_base_event(df).cache()

    logger.info("Validating custom_track enum")
    clean_df = clean_and_filter_custom_track(
        base_df,
        allowed_values={
            "click",
            "conversion",
            "qualified",
            "unqualified",
        },
        stage="custom_track_filter",
    )

    logger.info("Aggregating metrics")
    fact_df = aggregate_metrics_single_pass(clean_df)

    logger.info("Enriching Job Dimension")
    fact_df = enrich_job_dimension(fact_df, jobs_df)

    logger.info("Adding metadata")
    final = add_metadata(fact_df)

    return final, clean_df