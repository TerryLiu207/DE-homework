# DE-homework-week1
    #Question3
    Select count(1) as n 
    from taxi2019
    where Date(lpep_pickup_datetime)= '2019-09-18'
    and Date(lpep_dropoff_datetime)= '2019-09-18';
    
    #Question4
    SELECT DATE_TRUNC('day', lpep_pickup_datetime) AS pickup_day, SUM(trip_distance) AS total_trip_distance
    FROM taxi2019
    GROUP BY pickup_day
    ORDER BY total_trip_distance DESC
    LIMIT 1;
    
    #Question5
    SELECT zone2019."Borough", SUM(taxi2019.total_amount) AS TotalAmount
    FROM taxi2019 
    LEFT JOIN zone2019 ON taxi2019."PULocationID" = zone2019."LocationID"
    WHERE DATE(taxi2019.lpep_pickup_datetime) = '2019-09-18' AND zone2019."Borough" != 'Unknown'
    GROUP BY zone2019."Borough"
    ORDER BY TotalAmount DESC;
    
    #Question6
    SELECT SUM(A.tip_amount) as totaltips, zone2019."Zone"
    FROM (
        SELECT *
        FROM taxi2019
        LEFT JOIN zone2019 ON taxi2019."PULocationID" = zone2019."LocationID"
        WHERE EXTRACT(MONTH FROM taxi2019.lpep_pickup_datetime) = 9  
        AND zone2019."Zone" = 'Astoria'
    ) A
    LEFT JOIN zone2019 ON A."DOLocationID" = zone2019."LocationID"
    GROUP BY zone2019."Zone"
    ORDER BY totaltips DESC;
