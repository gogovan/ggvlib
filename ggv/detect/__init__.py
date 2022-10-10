from datetime import datetime
from this import d
from google.cloud import bigquery
from ggv.log import logger
import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None

REPEAT_PICKING_TIMES = 3
REPEAT_PICKING_FREQUENCY = 2
REPEAT_GPS_TIMES = 10
SPEED_LIMIT = 100
DISTANCE_LIMIT = 1000
TRAVEL_SPEED_LIMIT = 100
TRAVEL_DISTANCE_LIMIT = 1000
TRAVEL_FREQUENCY = 2
ACCEPT_DISTANCE_LIMIT = 5000
ACCEPT_DISTANCE_FREQUENCY = 2
SPEEDY_FREQUENCY = 2
REPEAT_GPS_FREQUENCY = 2
PICK_ACCEPT_THRESHOLD = 0.5


def run_query(query: str) -> pd.DataFrame:
    logger.debug(query)
    result = bigquery.Client().query(query).result().to_dataframe()
    logger.debug(result)
    return result


def get_driver_gps_bq(date: datetime.date, country: str) -> pd.DataFrame:
    query = f"""
    SELECT
      driver_id,
      location_updated_at AS datetime,
      ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
            "$.wkb"))) AS location,
      ST_X(ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
            "$.wkb")))) AS lat,
      ST_Y(ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
            "$.wkb")))) AS lon,
      ST_X(LAG(ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
              "$.wkb")))) OVER (PARTITION BY driver_id ORDER BY location_updated_at ASC)) AS prev_lat,
      ST_Y(LAG(ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
              "$.wkb")))) OVER (PARTITION BY driver_id ORDER BY location_updated_at ASC)) AS prev_lon,
      CAST(DATE(location_updated_at) AS STRING) as dt,
      DATE(location_updated_at) as date,
      SUBSTR(CAST(DATE(location_updated_at) AS STRING), 1,6) as month,
      upper('{country}') as country,
      ST_DISTANCE(ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
              "$.wkb"))),
        LAG(ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
                "$.wkb")))) OVER (PARTITION BY driver_id ORDER BY location_updated_at ASC)) AS distance,
      LAG(location_updated_at) OVER (PARTITION BY driver_id ORDER BY location_updated_at ASC) AS pre_updated_at,
      TIMESTAMP_DIFF((location_updated_at), (LAG(location_updated_at) OVER (PARTITION BY driver_id ORDER BY location_updated_at ASC)), MICROSECOND) AS time_diff_in_mic_s,
      LAG(ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
              "$.wkb")))) OVER (PARTITION BY driver_id ORDER BY location_updated_at ASC) AS prev_location,
      (CASE
          WHEN TIMESTAMP_DIFF((location_updated_at), (LAG(location_updated_at) OVER (PARTITION BY driver_id ORDER BY location_updated_at ASC)), MICROSECOND) =0 THEN 0
        ELSE
        ST_DISTANCE(ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
                "$.wkb"))),
          LAG(ST_GEOGFROMGEOJSON(raw.parse_wkx(JSON_VALUE(location,
                  "$.wkb")))) OVER (PARTITION BY driver_id ORDER BY location_updated_at ASC))/TIMESTAMP_DIFF((location_updated_at), (LAG(location_updated_at) OVER (PARTITION BY driver_id ORDER BY location_updated_at ASC)), MICROSECOND)*1000000
      END
        ) AS speed_in_m_per_s
    FROM
      `gogox-data-science-non-prod.raw.streaming_data_{country}_driver_details`
    WHERE
      DATE(location_updated_at) = DATE('{date}')
      """
    return run_query(query)


def get_order_events_bq(date: datetime.date, country: str) -> pd.DataFrame:
    query = f"""
    WITH
      temp1 AS (
      SELECT
        country,
        cast(order_request_id as String) as order_request_id,
        actor_id,
        actor_type,
        driver_id,
        event_type_cd,
        meta_waypoint_index,
        meta_arrived_name,
        meta_arrived_lat AS lat,
        meta_arrived_lon AS lon,
        created_at,
        LAG(created_at) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC) AS prev_created_at,
        TIMESTAMP_DIFF(created_at, LAG(created_at) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC), microsecond) AS time_diff_in_mic_s,
        DATE(created_at) AS dt
      FROM
        `gogox-data-science-non-prod.analytics_prod_raw.raw_order_request_events`
      WHERE
        DATE(created_at) = DATE('{date}')
        AND country = '{country}'),
      temp2 AS (
      SELECT
        cast(order_request_id as String) as order_request_id,
        actor_id,
        actor_type,
        driver_id,
        event_type_cd,
        meta_waypoint_index,
        meta_arrived_name,
        meta_arrived_lat AS lat,
        meta_arrived_lon AS lon,
        LAG(meta_arrived_lat) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC) AS prev_lat,
        LAG(meta_arrived_lon) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC) AS prev_lon,
        created_at,
        LAG(created_at) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC) AS prev_created_at,
        ST_GEOGPOINT(meta_arrived_lon,
          meta_arrived_lat) AS location,
        ST_GEOGPOINT(LAG(meta_arrived_lon) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC),
          LAG(meta_arrived_lat) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC)) AS prev_location,
        st_distance(ST_GEOGPOINT(meta_arrived_lon,
            meta_arrived_lat),
          ST_GEOGPOINT(LAG(meta_arrived_lon) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC),
            LAG(meta_arrived_lat) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC))) AS distance,
        TIMESTAMP_DIFF(created_at, LAG(created_at) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC), MICROSECOND) AS time_diff_in_mic_s,
        st_distance(ST_GEOGPOINT(meta_arrived_lon,
            meta_arrived_lat),
          ST_GEOGPOINT(LAG(meta_arrived_lon) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC),
            LAG(meta_arrived_lat) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC)))/TIMESTAMP_DIFF(created_at, LAG(created_at) OVER (PARTITION BY order_request_id, actor_id ORDER BY created_at ASC), MICROSECOND)*1000000 AS speed_in_m_per_s
      FROM
        `gogox-data-science-non-prod.analytics_prod_raw.raw_order_request_events`
      WHERE
        DATE(created_at) = DATE('{date}')
        AND country = '{country}'
        AND event_type_cd IN (2,
          5) ),
      temp3 AS (
      SELECT
        cast(system_order_request_id as String) as source_order_request_id,
        standard_order_request_id,
        status,
        product_name,
        delivery_type,
        picked_up_at,
        drop_off_at,
        created_at,
        completed_at,
        cancelled_at,
        driver_id,
        waypoint_count,
        pickup_location_region,
        pickup_location_address,
        pickup_location_lat,
        pickup_location_lon,
        destination_location_region,
        destination_location_address,
        destination_location_lat,
        destination_location_lon,
        booking_type
      FROM
        `gogox-data-science-non-prod.analytics_prod_master.order_master_table_{country}`
      WHERE
        DATE(created_at) = DATE('{date}'))
    SELECT
      temp1.order_request_id ,
      temp1.actor_id,
      temp1.actor_type,
      temp1.event_type_cd,
      concat('waypoint-',cast(temp1.meta_waypoint_index as string), ' : ' , temp1.meta_arrived_name) as meta,
      temp1.lat,
      temp1.lon,
      temp2.prev_lat as lat_prev,
      temp2.prev_lon as lon_prev,
      temp1.created_at,
      temp1.prev_created_at as time_prev,
      temp1.time_diff_in_mic_s/1000 as time_diff_in_ms,
      temp2.distance AS distance_trip,
      temp2.prev_created_at AS time_prev_trip,
      temp2.time_diff_in_mic_s/1000 AS time_diff_in_ms_trip,
      temp2.speed_in_m_per_s AS speed_in_m_per_s_trip,
      temp2.driver_id AS driver,
      temp1.dt AS date,
      upper('{country}') as country,
      temp3.standard_order_request_id as order_request_id_master_table,
      CAST(temp3.source_order_request_id as String) as system_order_request_id,
      temp3.status,
      temp3.created_at as order_created_at,
      temp3.completed_at,
      temp3.cancelled_at,      
      temp3.product_name,
      temp3.picked_up_at AS order_pick_up_at,
      temp3.drop_off_at AS order_drop_off_at,
      temp3.pickup_location_lat as pickup_location_lat,
      temp3.pickup_location_lon as pickup_location_lon,
      temp3.pickup_location_address as pickup_location_address,
      temp3.pickup_location_region as pickup_location_region,
      temp3.destination_location_lat as destination_location_lat,
      temp3.destination_location_lon as destination_location_lon,
      temp3.destination_location_address as destination_location_address,
      temp3.destination_location_region as destination_location_region,
      '' as requirements_need_carry,
      '' as requirements_need_carry_no_lift,
      temp3.waypoint_count,
      temp3.booking_type
    FROM
      temp1
    LEFT JOIN
      temp2
    ON
      temp1.order_request_id = temp2.order_request_id
      AND temp1.created_at = temp2.created_at
      AND temp1.actor_id = temp2.actor_id
    LEFT JOIN
      temp3
    ON
      temp1.order_request_id = temp3.source_order_request_id
    """
    return run_query(query)


def get_repeat_gps(
    df: pd.DataFrame, repeat_gps_threshold: int
) -> pd.DataFrame:
    df = (
        df.groupby(["driver_id", "lat", "lon", "date"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_GPS"})
    )
    df = df[df["ct_GPS"] > repeat_gps_threshold]
    df = (
        df.groupby(["driver_id", "date"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_>=gps_repeat_threshold"})
    )
    return df


def get_speedy_drivers(
    df: pd.DataFrame, speed_threshold: int, distance_threshold: int
) -> pd.DataFrame:
    df["driver_id"] = df["driver_id"].astype(int).astype(str)
    df = df[
        (df["distance"] >= distance_threshold)
        & (df["speed_in_m_per_s"] >= speed_threshold)
    ]
    return df


def get_speedy_summary(
    speedy_drivers: pd.DataFrame, repeat_gps: pd.DataFrame, gps: pd.DataFrame
) -> pd.DataFrame:
    speedy_drivers["driver_id"] = speedy_drivers["driver_id"].astype(int)
    count_of_speedy = (
        speedy_drivers.groupby(["driver_id", "date"])
        .size()
        .reset_index()
        .rename(columns={0: "ct>=speed_threshold_gps"})
    )
    count_of_gps = (
        gps.groupby(["driver_id", "date"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_gps"})
    )
    count_of_gps["driver_id"] = count_of_gps["driver_id"].astype(int)
    gps_speedy = pd.merge(
        count_of_gps,
        count_of_speedy,
        how="left",
        left_on=["driver_id", "date"],
        right_on=["driver_id", "date"],
    )
    gps_speedy["speedy%"] = [
        x / y
        for x, y in zip(
            gps_speedy["ct>=speed_threshold_gps"], gps_speedy["ct_gps"]
        )
    ]
    temp = gps_speedy.copy()
    temp["ct_pct"] = temp[temp["speedy%"].notna()][
        "ct>=speed_threshold_gps"
    ].rank(pct=True)
    temp["speedy%_pct"] = temp[temp["speedy%"].notna()]["speedy%"].rank(
        pct=True
    )
    gps_speedy = pd.merge(
        gps_speedy,
        temp[["driver_id", "date", "ct_pct"]],
        how="left",
        left_on=["driver_id", "date"],
        right_on=["driver_id", "date"],
        suffixes=("", "_x"),
    )
    gps_speedy = pd.merge(
        gps_speedy,
        temp[["driver_id", "date", "speedy%_pct"]],
        how="left",
        left_on=["driver_id", "date"],
        right_on=["driver_id", "date"],
        suffixes=("", "_y"),
    )
    gps_speedy["driver_id"] = gps_speedy["driver_id"].astype(int)
    repeat_gps["driver_id"] = repeat_gps["driver_id"].astype(int)
    gps_speedy = pd.merge(
        gps_speedy,
        repeat_gps,
        how="left",
        left_on=["driver_id", "date"],
        right_on=["driver_id", "date"],
        suffixes=("", "_z"),
    )
    gps_speedy = gps_speedy[
        [
            "driver_id",
            "date",
            "ct_gps",
            "ct>=speed_threshold_gps",
            "speedy%",
            "ct_pct",
            "speedy%_pct",
            "ct_>=gps_repeat_threshold",
        ]
    ]
    gps_speedy["ct>=speed_threshold_gps"] = gps_speedy[
        "ct>=speed_threshold_gps"
    ].fillna(0)
    gps_speedy["ct>=speed_threshold_gps"] = gps_speedy[
        "ct>=speed_threshold_gps"
    ].astype(int)
    gps_speedy["ct_>=gps_repeat_threshold"] = gps_speedy[
        "ct_>=gps_repeat_threshold"
    ].fillna(0)
    gps_speedy["ct_>=gps_repeat_threshold"] = gps_speedy[
        "ct_>=gps_repeat_threshold"
    ].astype(int)
    gps_speedy["speedy%"] = gps_speedy["speedy%"].fillna(0)
    return gps_speedy


def get_pick_accept_cal(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["actor_type"] == "Driver"]

    df1 = df[["order_request_id", "driver"]].drop_duplicates()
    temp = (
        df[(df["event_type_cd"] == 22) | (df["event_type_cd"] == 20)]
        .groupby(["order_request_id", "driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_pick"})
    )
    temp1 = (
        df[df["event_type_cd"] == 2]
        .groupby(["order_request_id", "driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_accept"})
    )

    output = pd.merge(
        df1,
        temp,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )
    output = pd.merge(
        output,
        temp1,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )

    temp2 = (
        df[(df["event_type_cd"] == 22)]
        .groupby(["order_request_id", "driver"])
        .mean()
        .reset_index()[["order_request_id", "driver", "time_diff_in_ms"]]
        .rename(columns={"time_diff_in_ms": "avg_time_pick"})
    )
    temp3 = (
        df[(df["event_type_cd"] == 20)]
        .groupby(["order_request_id", "driver"])
        .mean()
        .reset_index()[["order_request_id", "driver", "time_diff_in_ms"]]
        .rename(columns={"time_diff_in_ms": "avg_time_pick_driving"})
    )
    temp4 = (
        df[(df["event_type_cd"] == 2)]
        .groupby(["order_request_id", "driver"])
        .mean()
        .reset_index()[["order_request_id", "driver", "time_diff_in_ms"]]
        .rename(columns={"time_diff_in_ms": "avg_time_accept"})
    )

    output = pd.merge(
        output,
        temp2,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )
    output = pd.merge(
        output,
        temp3,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )
    output = pd.merge(
        output,
        temp4,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )

    temp5 = (
        df[(df["event_type_cd"] == 22) & (df["time_diff_in_ms"] <= 2000)]
        .groupby(["order_request_id", "driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_<=2s_pick"})[
            ["order_request_id", "driver", "ct_<=2s_pick"]
        ]
    )
    temp6 = (
        df[(df["event_type_cd"] == 20) & (df["time_diff_in_ms"] <= 2000)]
        .groupby(["order_request_id", "driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_<=2s_pick_driving"})[
            ["order_request_id", "driver", "ct_<=2s_pick_driving"]
        ]
    )
    temp7 = (
        df[(df["event_type_cd"] == 2) & (df["time_diff_in_ms"] <= 2000)]
        .groupby(["order_request_id", "driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_<=2s_accept"})[
            ["order_request_id", "driver", "ct_<=2s_accept"]
        ]
    )

    output = pd.merge(
        output,
        temp5,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )
    output = pd.merge(
        output,
        temp6,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )
    output = pd.merge(
        output,
        temp7,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )

    temp8 = (
        df[(df["event_type_cd"] == 22) & (df["time_diff_in_ms"] <= 1000)]
        .groupby(["order_request_id", "driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_<=1s_pick"})[
            ["order_request_id", "driver", "ct_<=1s_pick"]
        ]
    )
    temp9 = (
        df[(df["event_type_cd"] == 20) & (df["time_diff_in_ms"] <= 1000)]
        .groupby(["order_request_id", "driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_<=1s_pick_driving"})[
            ["order_request_id", "driver", "ct_<=1s_pick_driving"]
        ]
    )
    temp10 = (
        df[(df["event_type_cd"] == 2) & (df["time_diff_in_ms"] <= 1000)]
        .groupby(["order_request_id", "driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_<=1s_accept"})[
            ["order_request_id", "driver", "ct_<=1s_accept"]
        ]
    )

    output = pd.merge(
        output,
        temp8,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )
    output = pd.merge(
        output,
        temp9,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )
    output = pd.merge(
        output,
        temp10,
        how="left",
        left_on=["driver", "order_request_id"],
        right_on=["driver", "order_request_id"],
    )

    return output

def get_avg_time_pick_accept(df: pd.DataFrame) -> pd.DataFrame:

    pick = df[df['event_type_cd']==20].group_by(['driver'])
    pick_driving = df[df['event_type_cd']==22]
    accept = df[df['event_type_cd']==2]

    return_df = 0

    return return_df

def get_driving_speed(df: pd.DataFrame, tsl: int, tdl: int) -> pd.DataFrame:
    df2 = (
        df[
            ((df["event_type_cd"] == 5) | (df["event_type_cd"] == 2))
            & (df["speed_in_m_per_s_trip"] >= tsl)
            & (df["distance_trip"] >= tdl)
        ]
        .groupby(["order_request_id", "driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_speedy_driving"})
    )
    return df2


def get_accept_travel_id(df: pd.DataFrame) -> pd.DataFrame:
    temp1 = df[["order_request_id", "driver", "event_type_cd"]]
    temp2 = temp1[temp1["event_type_cd"] == 2].drop_duplicates()
    temp3 = temp1[temp1["event_type_cd"] == 5].drop_duplicates()
    temp4 = pd.merge(
        temp2,
        temp3,
        how="inner",
        left_on=["order_request_id", "driver"],
        right_on=["order_request_id", "driver"],
    )
    return temp4[["order_request_id", "driver"]]


def get_suspicious_accept(
    df: pd.DataFrame, order_driver_list: pd.DataFrame
) -> pd.DataFrame:
    order_driver_list = order_driver_list.values.tolist()
    df["temp"] = df["order_request_id"] + "+" + df["driver"]
    return_df = df[df["temp"].isin(order_driver_list)]
    return_df = return_df.drop(columns=["temp"], axis=1)
    return return_df


def get_accept_distance(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        (df["event_type_cd"] == 2) | (df["event_type_cd"] == 5)
    ].sort_values(by=["order_request_id", "driver", "created_at"])
    df["accept_from_far"] = [
        n if x == 2 else 0
        for n, x in zip(df["distance_trip"], df["event_type_cd"].shift(1))
    ]
    df = df[
        [
            "order_request_id",
            "driver",
            "event_type_cd",
            "created_at",
            "accept_from_far",
        ]
    ]
    return df


def get_repeat_pick(
    df: pd.DataFrame, repeat_picking_threshold: int
) -> pd.DataFrame:
    num_of_orders = (
        df[df["ct_pick"].notnull()]
        .groupby(["driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_orders_pick"})
    )
    repeat_pick = (
        df[df["ct_pick"] >= repeat_picking_threshold]
        .groupby(["driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_repeat_pick"})
    )
    repeat = pd.merge(
        num_of_orders,
        repeat_pick,
        how="left",
        left_on="driver",
        right_on="driver",
    )
    repeat["p_repeat_pick"] = [
        x / y if y != 0 else 0
        for x, y in zip(repeat["ct_repeat_pick"], repeat["ct_orders_pick"])
    ]
    temp = repeat[repeat["ct_repeat_pick"] > 0][["driver", "p_repeat_pick"]]
    temp_pct = (
        temp[["p_repeat_pick"]]
        .rank(pct=True)
        .rename(columns={"p_repeat_pick": "pct_p_repeat_pick"})
    )
    pct_df = pd.concat([temp, temp_pct], axis=1)
    repeat_df = pd.merge(
        repeat,
        pct_df[["driver", "pct_p_repeat_pick"]],
        how="left",
        left_on="driver",
        right_on="driver",
    )
    return repeat_df


def get_completed_cancelled_released(
    df: pd.DataFrame
)-> pd.DataFrame:
    df = df[df["actor_type"] == "Driver"]
    
    release = df[(df["event_type_cd"] == 21)|(df["event_type_cd"] ==4)][["actor_id","order_request_id"]].groupby(["actor_id"]).nunique().reset_index().rename(columns={"actor_id":"driver_id","order_request_id":"ct_orders_release"})
    cancel = df[(df["event_type_cd"] == 3)][["actor_id","order_request_id"]].groupby(["actor_id"]).nunique().reset_index().rename(columns={"actor_id":"driver_id","order_request_id":"ct_orders_cancel"})
    complete = df[(df["event_type_cd"] == 6)][["actor_id","order_request_id"]].groupby(["actor_id"]).nunique().reset_index().rename(columns={"actor_id":"driver_id","order_request_id":"ct_orders_complete"})
    
    return_df = pd.merge(complete, release, how = "outer", left_on ="driver_id", right_on = "driver_id")
    return_df = pd.merge(return_df, cancel, how = "outer", left_on ="driver_id", right_on = "driver_id")
    
    return return_df

def get_pc_pct(df: pd.DataFrame) -> pd.DataFrame:
    df["driver"] = df["driver"].astype(str)

    df["pick%_<=2s"] = [
        x / y if y != 0 else 0
        for x, y in zip(df["ct_<=2s_pick"], df["ct_pick"])
    ]
    df["pick%_<=1s"] = [
        x / y if y != 0 else 0
        for x, y in zip(df["ct_<=1s_pick"], df["ct_pick"])
    ]

    temp = df[df["ct_<=2s_pick"] > 0][["driver", "pick%_<=2s"]]
    temp_pick_pct = (
        temp[["pick%_<=2s"]]
        .rank(pct=True)
        .rename(columns={"pick%_<=2s": "pct_p_<=2s_pick"})
    )
    pct_pct2s = pd.concat([temp, temp_pick_pct], axis=1)
    temp = df[df["ct_<=1s_pick"] > 0][["driver", "pick%_<=1s"]]
    temp_pick_pct = (
        temp[["pick%_<=1s"]]
        .rank(pct=True)
        .rename(columns={"pick%_<=1s": "pct_p_<=1s_pick"})
    )
    pct_pct1s = pd.concat([temp, temp_pick_pct], axis=1)

    df["accept%_<=2s"] = [
        x / y if y != 0 else 0
        for x, y in zip(df["ct_<=2s_accept"], df["ct_accept"])
    ]
    df["accept%_<=1s"] = [
        x / y if y != 0 else 0
        for x, y in zip(df["ct_<=1s_accept"], df["ct_accept"])
    ]

    temp = df[df["ct_<=2s_accept"] > 0][["driver", "accept%_<=2s"]]
    temp_acpt_pct = (
        temp[["accept%_<=2s"]]
        .rank(pct=True)
        .rename(columns={"accept%_<=2s": "pct_p_<=2s_accept"})
    )
    pct_acpt2s = pd.concat([temp, temp_acpt_pct], axis=1)
    temp = df[df["ct_<=1s_accept"] > 0][["driver", "accept%_<=1s"]]
    temp_acpt_pct = (
        temp[["accept%_<=1s"]]
        .rank(pct=True)
        .rename(columns={"accept%_<=1s": "pct_p_<=1s_accept"})
    )
    pct_acpt1s = pd.concat([temp, temp_acpt_pct], axis=1)

    df["far_accept%"] = [
        x / y if y != 0 else 0
        for x, y in zip(df["ct_accept_far"], df["ct_accept"])
    ]

    far = df[df["ct_accept_far"] > 0][["driver", "far_accept%"]]
    far_acpt_pct = (
        far[["far_accept%"]]
        .rank(pct=True)
        .rename(columns={"far_accept%": "pct_p_far_accept"})
    )
    pct_far = pd.concat([far, far_acpt_pct], axis=1)

    pct_pct2s["driver"] = pct_pct2s["driver"].astype(str)
    pct_pct1s["driver"] = pct_pct1s["driver"].astype(str)
    pct_acpt2s["driver"] = pct_acpt2s["driver"].astype(str)
    pct_acpt1s["driver"] = pct_acpt1s["driver"].astype(str)
    pct_far["driver"] = pct_far["driver"].astype(str)

    df = pd.merge(
        df,
        pct_pct2s[["driver", "pct_p_<=2s_pick"]],
        how="left",
        left_on="driver",
        right_on="driver",
    )
    df = pd.merge(
        df,
        pct_pct1s[["driver", "pct_p_<=1s_pick"]],
        how="left",
        left_on="driver",
        right_on="driver",
    )
    df = pd.merge(
        df,
        pct_acpt2s[["driver", "pct_p_<=2s_accept"]],
        how="left",
        left_on="driver",
        right_on="driver",
    )
    df = pd.merge(
        df,
        pct_acpt1s[["driver", "pct_p_<=1s_accept"]],
        how="left",
        left_on="driver",
        right_on="driver",
    )
    df = pd.merge(
        df,
        pct_far[["driver", "pct_p_far_accept"]],
        how="left",
        left_on="driver",
        right_on="driver",
    )
    return df


def fill_format(df, col_list, fill_content, format_type):
    for col in col_list:
        df[col].fillna(fill_content, inplace=True)
        df[col] = df[col].astype(format_type)
    return df


def process_cheating_upload(date: datetime.date, country: str):
    gps = get_driver_gps_bq(date, country)
    order_events_check = get_order_events_bq(date, country)
    repeat_gps = get_repeat_gps(df=gps, repeat_gps_threshold=REPEAT_GPS_TIMES)
    speedy_drivers = get_speedy_drivers(
        df=gps, speed_threshold=SPEED_LIMIT, distance_threshold=DISTANCE_LIMIT
    )
    gps_speedy_final = get_speedy_summary(
        speedy_drivers=speedy_drivers, repeat_gps=repeat_gps, gps=gps
    )

    # Might be able to remove these
    gps_speedy_final["driver_id"] = gps_speedy_final["driver_id"].astype(str)

    order_events_check["order_request_id"] = order_events_check[
        "order_request_id"
    ].astype(str)
    # ^
    order_events_check["driver"] = (
        order_events_check["driver"].astype(str).str.replace("nan", "")
    )
    order_events_check["actor_id"] = (
        order_events_check["actor_id"].astype(str).str.replace("nan", "")
    )
    order_events = get_pick_accept_cal(order_events_check)

    print(order_events)
    # avg_time = order_events['']

    driving_speed = get_driving_speed(
        order_events_check, tsl=TRAVEL_SPEED_LIMIT, tdl=TRAVEL_DISTANCE_LIMIT
    )
    order_pick_driving = pd.merge(
        order_events,
        driving_speed,
        how="left",
        left_on=["order_request_id", "driver"],
        right_on=["order_request_id", "driver"],
    )
    order_driver_list = get_accept_travel_id(order_events_check)
    order_driver = get_suspicious_accept(order_events_check, order_driver_list)
    accept_distance = get_accept_distance(order_driver)
    accept_distance = accept_distance[
        (accept_distance["accept_from_far"] > 0)
        & (accept_distance["event_type_cd"] == 5)
    ][["order_request_id", "driver", "accept_from_far"]]

    order_pick_driving_far = pd.merge(
        order_pick_driving,
        accept_distance,
        how="left",
        left_on=["order_request_id", "driver"],
        right_on=["order_request_id", "driver"],
    )
    far_accept = (
        accept_distance[
            accept_distance["accept_from_far"] >= ACCEPT_DISTANCE_LIMIT
        ][["driver", "order_request_id"]]
        .groupby(["driver"])
        .size()
        .reset_index()
        .rename(columns={0: "ct_accept_far"})
    )
    driver_pick_speed = (
        order_pick_driving_far.groupby("driver")
        .sum()
        .reset_index()[
            [
                "driver",
                "ct_pick",
                "ct_accept",
                "ct_<=2s_pick",
                "ct_<=2s_pick_driving",
                "ct_<=2s_accept",
                "ct_<=1s_pick",
                "ct_<=1s_pick_driving",
                "ct_<=1s_accept",
                "ct_speedy_driving",
            ]
        ]
    )
    driver_pick_speed_far = pd.merge(
        driver_pick_speed,
        far_accept,
        how="left",
        left_on=["driver"],
        right_on=["driver"],
    )
    repeat_pick = get_repeat_pick(
        order_pick_driving_far, repeat_picking_threshold=REPEAT_PICKING_TIMES
    )
  
    driver_order_status = get_completed_cancelled_released(
        order_events_check
    )


    driver_pick_speed_far_p = get_pc_pct(driver_pick_speed_far)
    driver_table = pd.merge(
        gps_speedy_final,
        driver_pick_speed_far_p,
        how="outer",
        left_on="driver_id",
        right_on="driver",
    )
    driver_table["date"] = date
    driver_summary = pd.merge(
        driver_table,
        repeat_pick,
        how="left",
        left_on="driver_id",
        right_on="driver",
    )
    driver_summary = pd.merge(
        driver_summary,
        driver_order_status,
        how="left",
        left_on="driver_id",
        right_on="driver_id",
    )

    rule_gps = driver_summary["ct>=speed_threshold_gps"] >= SPEEDY_FREQUENCY
    rule_repeat_gps = driver_summary["ct_>=gps_repeat_threshold"] >= REPEAT_GPS_FREQUENCY
    rule_pick_2s = driver_summary["pick%_<=2s"] >= PICK_ACCEPT_THRESHOLD
    rule_accept_2s = driver_summary["accept%_<=2s"] >= PICK_ACCEPT_THRESHOLD
    rule_pick_1s = driver_summary["pick%_<=1s"] >= PICK_ACCEPT_THRESHOLD
    rule_accept_1s = driver_summary["accept%_<=1s"] >= PICK_ACCEPT_THRESHOLD
    rule_driving_speed = driver_summary["ct_speedy_driving"] >= TRAVEL_FREQUENCY
    rule_repeat_pick = driver_summary["ct_repeat_pick"] >= REPEAT_PICKING_FREQUENCY
    rule_accept_far = driver_summary["ct_accept_far"] >= ACCEPT_DISTANCE_FREQUENCY

    driver_summary["fake_gps"] = rule_gps
    driver_summary["repeat_gps"] = rule_repeat_gps
    driver_summary["pick_accept_bot"] = (rule_pick_2s)|(rule_accept_2s)|(rule_pick_1s)|(rule_accept_1s)
    driver_summary["speedy_driving"] = rule_driving_speed
    driver_summary["repeat_pick"] = rule_repeat_pick
    driver_summary["far_accept"] = rule_accept_far
    print(driver_summary.columns)
    driver_summary = driver_summary[
        [
            "driver_id",
            "date",
            "fake_gps",
            "ct_gps",
            "ct>=speed_threshold_gps",
            "speedy%",
            "speedy%_pct",
            "repeat_gps",
            "ct_>=gps_repeat_threshold",
            "pick_accept_bot",
            "ct_<=2s_pick",
            "ct_<=2s_pick_driving",
            "ct_<=2s_accept",
            "ct_<=1s_pick",
            "ct_<=1s_pick_driving",
            "ct_<=1s_accept",
            "pick%_<=2s",
            "pick%_<=1s",
            "accept%_<=2s",
            "accept%_<=1s",
            "pct_p_<=2s_pick",
            "pct_p_<=1s_pick",
            "pct_p_<=2s_accept",
            "pct_p_<=1s_accept",
            "speedy_driving",
            "ct_speedy_driving",
            "far_accept",
            "ct_accept_far",
            "far_accept%",
            "pct_p_far_accept",
            "repeat_pick",
            "ct_orders_pick",
            "ct_repeat_pick",
            "p_repeat_pick",
            "pct_p_repeat_pick",
            "ct_orders_complete",
            "ct_orders_release",
            "ct_orders_cancel",
        ]
    ]
    airport_carry = order_events_check[
        (order_events_check["pickup_location_region"] == "機場")
        | (order_events_check["destination_location_region"] == "機場")
        | (order_events_check["requirements_need_carry"] == True)
        | (order_events_check["requirements_need_carry_no_lift"] == True)
    ]
    order_list_suspicious = order_pick_driving_far[
        (order_pick_driving_far["ct_pick"] >= REPEAT_PICKING_TIMES)
        | (order_pick_driving_far["ct_<=2s_pick"] >= 1)
        | (order_pick_driving_far["ct_<=2s_pick_driving"] >= 1)
        | (order_pick_driving_far["ct_<=2s_accept"] >= 1)
        | (order_pick_driving_far["ct_<=2s_pick"] >= 1)
        | (order_pick_driving_far["ct_<=2s_pick_driving"] >= 1)
        | (order_pick_driving_far["ct_<=2s_accept"] >= 1)
        | (order_pick_driving_far["ct_speedy_driving"] >= 1)
        | (order_pick_driving_far["accept_from_far"] >= ACCEPT_DISTANCE_LIMIT)
    ]
    gps_list_suspicious = gps_speedy_final[
        (gps_speedy_final["ct>=speed_threshold_gps"] >= SPEEDY_FREQUENCY)
        | (
            gps_speedy_final["ct_>=gps_repeat_threshold"]
            >= REPEAT_GPS_FREQUENCY
        )
    ]
    order_list = tuple(
        np.concatenate(
            (
                order_list_suspicious["order_request_id"].unique(),
                airport_carry["order_request_id"].unique(),
            )
        )
    )
    driver_list = tuple(gps_list_suspicious["driver_id"].unique().astype(str))

    order_event_suspicious = order_events_check[
        (order_events_check["order_request_id"].isin(order_list))
        | (order_events_check["driver"].isin(driver_list))
    ]
    order_event_pick_driving = pd.merge(
        order_event_suspicious,
        driving_speed,
        how="left",
        left_on=["order_request_id", "driver"],
        right_on=["order_request_id", "driver"],
    )
    order_event_pick_driving_far = pd.merge(
        order_event_pick_driving,
        accept_distance,
        how="left",
        left_on=["order_request_id", "driver"],
        right_on=["order_request_id", "driver"],
    )
    order_output = order_event_pick_driving_far[
        [
            "order_request_id",
            "actor_id",
            "actor_type",
            "event_type_cd",
            "meta",
            "lat",
            "lon",
            "lat_prev",
            "lon_prev",
            "created_at",
            "driver",
            "date",
            "distance_trip",
            "time_diff_in_ms_trip",
            "speed_in_m_per_s_trip",
            "ct_speedy_driving",
            "accept_from_far",
            "country",
            "order_request_id_master_table",
            "system_order_request_id",
            "status",
            "order_created_at",
            "completed_at",
            "cancelled_at",
            "product_name",
            "pickup_location_lat",
            "pickup_location_lon",
            "pickup_location_address",
            "pickup_location_region",
            "destination_location_lat",
            "destination_location_lon",
            "destination_location_address",
            "destination_location_region",
            "requirements_need_carry",
            "requirements_need_carry_no_lift",
            "waypoint_count",
            "booking_type",
        ]
    ]
    order_output = order_output.rename(
        columns={
            "order_request_id_master_table": "order_request_id_with_product",
            "waypoint_count": "waypoints_count",
            "booking_type": "order_type",
            "cancelled_at": "canceled_at",
        }
    )
    order_output["created_at"] = pd.to_datetime(order_output["created_at"])
    order_output["order_created_at"] = pd.to_datetime(
        order_output["order_created_at"]
    )

    float_col_list = [
        "lat",
        "lon",
        "distance_trip",
        "time_diff_in_ms_trip",
        "speed_in_m_per_s_trip",
        "accept_from_far",
    ]
    int_col_list = ["ct_speedy_driving", "waypoints_count"]
    string_col_list = [
        "meta",
        "actor_id",
        "actor_type",
        "driver",
        "country",
        "order_request_id_with_product",
        "system_order_request_id",
        "status",
        "product_name",
        "pickup_location_lat",
        "pickup_location_lon",
        "pickup_location_address",
        "pickup_location_region",
        "destination_location_lat",
        "destination_location_lon",
        "destination_location_address",
        "destination_location_region",
        "requirements_need_carry",
        "requirements_need_carry_no_lift",
        "order_type",
    ]

    order_output = fill_format(order_output, float_col_list, float(0), float)
    order_output = fill_format(order_output, int_col_list, 0, int)
    order_output = fill_format(order_output, string_col_list, " ", str)

    driver_summary = driver_summary.rename(
        columns={
            "driver": "driver_id",
            "date": "dt",
            "fake_gps": "fake_gps",
            "ct_gps": "gps_ct",
            "ct>=speed_threshold_gps": "fake_gps_ct",
            "speedy%": "fake_gps_p",
            "speedy%_pct": "fake_gps_p_pct",
            "repeat_gps": "repeat_gps",
            "ct_>=gps_repeat_threshold": "repeat_gps_ct",
            "pick_accept_bot": "pick_accept_bot",
            "ct_<=2s_pick": "pick_2s_ct",
            "ct_<=2s_pick_driving": "pick_driving_2s_ct",
            "ct_<=2s_accept": "accept_2s_ct",
            "ct_<=1s_pick": "pick_1s_ct",
            "ct_<=1s_pick_driving": "pick_driving_1s_ct",
            "ct_<=1s_accept": "accept_1s_ct",
            "pick%_<=2s": "pick_2s_p",
            "pick%_<=1s": "pick_1s_p",
            "accept%_<=2s": "accept_2s_p",
            "accept%_<=1s": "accept_1s_p",
            "pct_p_<=2s_pick": "pick_2s_p_pct",
            "pct_p_<=1s_pick": "pick_1s_p_pct",
            "pct_p_<=2s_accept": "accept_2s_p_pct",
            "pct_p_<=1s_accept": "accept_1s_p_pct",
            "speedy_driving": "speedy_driving",
            "ct_speedy_driving": "speedy_driving_ct",
            "far_accept": "far_accept",
            "ct_accept_far": "far_accept_ct",
            "far_accept%": "far_accept_p",
            "pct_p_far_accept": "far_accept_p_pct",
            "repeat_pick": "repeat_pick",
            "ct_orders_pick": "orders_pick_ct",
            "ct_repeat_pick": "repeat_pick_ct",
            "p_repeat_pick": "repeat_pick_p",
            "pct_p_repeat_pick": "repeat_pick_p_pct",
            "ct_orders_complete": "orders_complete_ct",
            "ct_orders_release": "orders_release_ct",
            "ct_orders_cancel": "orders_cancel_ct",
        }
    )

    driver_summary = driver_summary.dropna(subset = ['driver_id'])

    driver_summary['country'] = country.upper()
    driver_summary["cheat_score"] = [int(a)+int(b)+int(c)+int(d)+int(e)+int(f) 
                                    for a,b,c,d,e,f in 
                                    zip(driver_summary["fake_gps"],
                                        driver_summary["repeat_gps"],
                                        driver_summary["pick_accept_bot"],
                                        driver_summary["speedy_driving"],
                                        driver_summary["far_accept"],
                                        driver_summary["repeat_pick"]
                                        )
                                    ]

    int_col_list = [        
        "gps_ct",
        "fake_gps_ct",
        "repeat_gps_ct",
        "pick_2s_ct",
        "pick_driving_2s_ct",
        "accept_2s_ct",
        "pick_1s_ct",
        "pick_driving_1s_ct",
        "accept_1s_ct",
        "speedy_driving_ct",
        "far_accept_ct",
        "orders_pick_ct",
        "repeat_pick_ct",
        "orders_complete_ct",
        "orders_release_ct",
        "orders_cancel_ct",
        
    ]
    float_col_list = [
        "fake_gps_p",
        "fake_gps_p_pct",
        "pick_2s_p",
        "pick_1s_p",
        "accept_2s_p",
        "accept_1s_p",
        "pick_2s_p_pct",
        "pick_1s_p_pct",
        "accept_2s_p_pct",
        "accept_1s_p_pct",
        "far_accept_p",
        "far_accept_p_pct",
        "repeat_pick_p",
        "repeat_pick_p_pct",
    ]

    driver_summary = fill_format(driver_summary,int_col_list,0,int)
    driver_summary = fill_format(driver_summary,float_col_list,float(0),float)

    return driver_summary, order_output