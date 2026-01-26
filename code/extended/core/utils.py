import uuid
from datetime import datetime, timezone

from config.settings import NUM_100NS_INTERVALS_SINCE_UUID_EPOCH


# ---- function to generate min TimeUUID string ----

def get_min_timeuuid_str(dt: datetime) -> str:
    """
    Generate the smallest TimeUUID string for a given datetime.
    Used for incremental filter pushdown in Cassandra.

    Parameters
    ----------
    dt : datetime
        The datetime for which to generate the minimum TimeUUID.

    Returns
    -------
    str
        The minimum TimeUUID string corresponding to the given datetime.
    """

    # 1. Handle Timezone (Ensure UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    # 2. Convert Unix timestamp → 100ns intervals since UUID Epoch (1582)
    ts_100ns = int(dt.timestamp() * 1e7) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH

    # 3. Bitwise Manipulation (UUID v1 structure)
    time_low = ts_100ns & 0xffffffff
    time_mid = (ts_100ns >> 32) & 0xffff
    time_hi_version = ((ts_100ns >> 48) & 0x0fff) | 0x1000  # Set version 1

    # 4. Create UUID with clock_seq=0 and node=0 (To be the MINIMUM)
    u = uuid.UUID(fields=(time_low, time_mid, time_hi_version, 0, 0, 0))

    return str(u)