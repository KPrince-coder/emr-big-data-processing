# Dataset Reference

This document contains information about the datasets used in the Big Data Processing with EMR project, including schema details and sample data.

## Locations Dataset

**Number of rows:** 300

### Schema

```markdown
root
 |-- location_id: integer (nullable = true)
 |-- location_name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- zip_code: integer (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
```

### Sample Data

```markdown
+-----------+--------------------+--------------------+---------+-----+--------+-----------+----------+
|location_id|       location_name|             address|     city|state|zip_code|   latitude| longitude|
+-----------+--------------------+--------------------+---------+-----+--------+-----------+----------+
|       2702|Jackson, Velazque...|3140 Heath Radial...|  Modesto|   CA|   94540|   86.25802| -169.2448|
|       4380|            Bean LLC|51144 Patrick Isl...|  Fontana|   CA|   92188|-74.4558925|-42.279882|
|       7709|     Gilbert-Simmons|    4738 Lewis Locks|Roseville|   CA|   91032|-65.4309305|-64.763489|
|       8607|    Coleman-Robinson|  324 Robin Causeway|  Modesto|   CA|   93714| -64.281076|-77.669631|
|       5499|        Deleon Group|    51725 Evans View|Roseville|   CA|   91849| 18.4951575|-154.76578|
+-----------+--------------------+--------------------+---------+-----+--------+-----------+----------+
```

## Rental Transactions Dataset

**Number of rows:** 20080

### Schema

```markdown
root
 |-- rental_id: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- vehicle_id: string (nullable = true)
 |-- rental_start_time: timestamp (nullable = true)
 |-- rental_end_time: timestamp (nullable = true)
 |-- pickup_location: integer (nullable = true)
 |-- dropoff_location: integer (nullable = true)
 |-- total_amount: double (nullable = true)
```

### Sample Data

```markdown
+----------+----------+----------+-------------------+-------------------+---------------+----------------+------------+
| rental_id|   user_id|vehicle_id|  rental_start_time|    rental_end_time|pickup_location|dropoff_location|total_amount|
+----------+----------+----------+-------------------+-------------------+---------------+----------------+------------+
|b139d8e1b2|320be8068b|0d52304987|2024-02-28 08:05:00|2024-03-01 05:05:00|           1497|            6785|       450.0|
|7afd60f6d3|320be8068b|975d72985c|2024-01-07 20:16:00|2024-01-09 21:16:00|           5345|            2608|      2450.0|
|733a9361bc|8f31b734a6|0d9f0f0fb9|2024-01-07 09:36:00|2024-01-07 17:36:00|           2546|            5442|        80.0|
|6e546b69dd|8f31b734a6|967fdab45e|2024-01-05 11:30:00|2024-01-07 04:30:00|           8147|            4380|      2050.0|
|acc192b64a|8f31b734a6|32d58ea4b7|2024-03-06 18:19:00|2024-03-09 14:19:00|           6290|            8932|      1360.0|
+----------+----------+----------+-------------------+-------------------+---------------+----------------+------------+
```

## Users Dataset

**Number of rows:** 30000

### Schema

```markdown
root
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- driver_license_number: string (nullable = true)
 |-- driver_license_expiry: date (nullable = true)
 |-- creation_date: date (nullable = true)
 |-- is_active: integer (nullable = true)
```

### Sample Data

```markdown
+----------+----------+---------+--------------------+------------------+---------------------+---------------------+-------------+---------+
|   user_id|first_name|last_name|               email|      phone_number|driver_license_number|driver_license_expiry|creation_date|is_active|
+----------+----------+---------+--------------------+------------------+---------------------+---------------------+-------------+---------+
|26d08ab733|      Lisa|   Parker|lisa.parker@gmail...|334.271.2972x60554|             MO028963|           2033-06-21|   2024-05-26|        1|
|0a0430e6f9|  Courtney|   Martin|courtney.martin@y...|  826-262-0518x252|             VW966518|           2028-09-28|   2024-05-22|        0|
|eb5d10cccd|    Andrew|  Mcclain|andrew.mcclain@ho...|   +1-467-858-1702|             WL839491|           2028-09-01|   2024-01-29|        1|
|2a59127ee0|   Michael|   Hoover|michael.hoover@ya...|  001-220-342-6250|             UI603163|           2028-11-29|   2024-03-22|        1|
|e3a46a2a11|     Molly|   Brooks|molly.brooks@yaho...|   +1-595-498-7645|             BJ158232|           2025-01-24|   2024-03-02|        1|
+----------+----------+---------+--------------------+------------------+---------------------+---------------------+-------------+---------+
```

## Vehicles Dataset

**Number of rows:** 109584

### Schema

```markdown
root
 |-- active: integer (nullable = true)
 |-- vehicle_license_number: string (nullable = true)
 |-- registration_name: string (nullable = true)
 |-- license_type: string (nullable = true)
 |-- expiration_date: string (nullable = true)
 |-- permit_license_number: string (nullable = true)
 |-- certification_date: date (nullable = true)
 |-- vehicle_year: integer (nullable = true)
 |-- base_telephone_number: string (nullable = true)
 |-- base_address: string (nullable = true)
 |-- vehicle_id: string (nullable = true)
 |-- last_update_timestamp: string (nullable = true)
 |-- brand: string (nullable = true)
 |-- vehicle_type: string (nullable = true)
```

### Sample Data

```markdown
+------+----------------------+--------------------+----------------+---------------+---------------------+------------------+------------+---------------------+--------------------+----------+---------------------+---------+------------+
|active|vehicle_license_number|   registration_name|    license_type|expiration_date|permit_license_number|certification_date|vehicle_year|base_telephone_number|        base_address|vehicle_id|last_update_timestamp|    brand|vehicle_type|
+------+----------------------+--------------------+----------------+---------------+---------------------+------------------+------------+---------------------+--------------------+----------+---------------------+---------+------------+
|     1|               5818886|CITY,LIVERY,LEASI...|FOR HIRE VEHICLE|     27-09-2025|             6EPABCVK|        2018-01-09|        2018|        (646)780-0129|1515 THIRD STREET...|67789f742d|  04-06-2024 13:25:00|  Ferrari|    high_end|
|     1|               5520432|    FERNANDEZ,JOSE,A|FOR HIRE VEHICLE|     08-01-2026|             IC0VQ8EC|        2015-01-21|        2015|        (646)780-0129|1515 THIRD STREET...|70e8c42e4f|  04-06-2024 13:25:00|      BMW|     premium|
|     1|               5790608| RIGO,LIMO-AUTO,CORP|FOR HIRE VEHICLE|     19-06-2025|             AGTGT62I|        2020-03-31|        2020|        (646)780-0129|1515 THIRD STREET...|aa2522d199|  04-06-2024 13:25:00|   Toyota|       basic|
|     1|               6045671|    NARZIEV,LAZIZJON|FOR HIRE VEHICLE|     22-11-2025|             OO9QLG6E|        2022-11-09|        2022|        (646)780-0129|1515 THIRD STREET...|0984531ace|  04-06-2024 13:25:00|Chevrolet|       basic|
|     1|               6022074|         YAQOOB,SAAD|FOR HIRE VEHICLE|     05-04-2025|             3U109JZC|        2018-11-29|        2018|        (646)780-0129|1515 THIRD STREET...|1ee2538be7|  04-06-2024 13:25:00|    Tesla|    high_end|
+------+----------------------+--------------------+----------------+---------------+---------------------+------------------+------------+---------------------+--------------------+----------+---------------------+---------+------------+
```
