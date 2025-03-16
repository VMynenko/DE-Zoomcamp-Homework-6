# Module 6: Stream Processing  

## Question 1: Redpanda version
What's the version, based on the output of the command you executed? (copy the entire version)  
```bash
$ rpk version
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9
```
Answer  
- `v24.2.18`

## Question 2. Creating a topic
What's the output of the command for creating a topic? Include the entire output in your answer.    
```bash
$ rpk topic create green-trips
TOPIC        STATUS
green-trips  OK
```
Answer  
- `TOPIC STATUS`  
- `green-trips  OK`

## Question 3. Connecting to the Kafka server
Provided that you can connect to the server, what's the output of the last command?    
```bash
python kafka_producer.py
True
```
Answer  
- `True`

## Question 4: Sending the Trip Data
How much time did it take to send the entire dataset and flush?   
```bash
python send_data.py
Sent 476386 messages in 68.43 seconds.
```
Answer  
- `Sent 476386 messages in 68.43 seconds.`

## Question 5: Build a Sessionization Window
Which pickup and drop off locations have the longest unbroken streak of taxi trips?      
```sql
WITH ordered_trips AS (
  SELECT
    lpep_pickup_datetime,
    PULocationID,
    DOLocationID,
    LAG(PULocationID) OVER (ORDER BY lpep_pickup_datetime) AS prev_pu,
    LAG(DOLocationID) OVER (ORDER BY lpep_pickup_datetime) AS prev_do
  FROM processed_events_aggregated
),
streak_start AS (
  SELECT
    *,
    CASE 
      WHEN PULocationID = prev_pu AND DOLocationID = prev_do THEN 0
      ELSE 1
    END AS is_streak_start
  FROM ordered_trips
),
streak_groups AS (
  SELECT
    *,
    SUM(is_streak_start) OVER (ORDER BY lpep_pickup_datetime) AS streak_id
  FROM streak_start
),
streak_lengths AS (
  SELECT
    PULocationID,
    DOLocationID,
    streak_id,
    COUNT(*) AS streak_length
  FROM streak_groups
  GROUP BY PULocationID, DOLocationID, streak_id
)
SELECT
  PULocationID,
  DOLocationID,
  MAX(streak_length) AS longest_streak
FROM streak_lengths
GROUP BY PULocationID, DOLocationID
ORDER BY longest_streak DESC
LIMIT 1;
```
Answer  
- `pickup location ID = 7 and drop off location ID = 264 has 9 unbroken streak`
