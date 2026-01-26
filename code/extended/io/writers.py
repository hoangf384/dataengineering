from logging import getLogger
from pyspark.sql import DataFrame
from config.settings import MYSQL_PASSWORD, MYSQL_URL, MYSQL_USER

logger = getLogger(__name__)


def write_data(final: DataFrame, table_name: str = "events"):
    """
    write data to MySQL

    parameter
    ----------
        final : DataFrame
    """
    try:
        if final.rdd.isEmpty():
            logger.info("DataFrame is empty. Skipping write.")
            return

        logger.info(f"Writing {final.count()} rows to MySQL table '{table_name}'...")

        (
            final.write.format("jdbc")
            .option("url", MYSQL_URL)
            .option("dbtable", table_name)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .mode("append")
            .option("batchsize", "10000")
            .option("isolationLevel", "READ_COMMITTED")
            .save()
        )
        logger.info(f"Successfully written data to {table_name}.")

    except Exception as e:
        logger.error(f"Failed to write data to {table_name}: {str(e)}")
        raise e