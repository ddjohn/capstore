-- Load data
flights  = LOAD '$FILE' USING PigStorage(',') AS ( 
	DAYOFWEEK: int, 
	FLIGHTDATE: chararray, 
	UNIQUECARRIER: chararray, 
	FLIGHTNUM: chararray,
	DEST: chararray, 
	ORIGIN: chararray, 
	DEPTIME: chararray, 
	DEPDELAY: float, 
	ARRDELAY: float
);

-- Only load file once
flights_w_date = FOREACH flights GENERATE DAYOFWEEK, FLIGHTDATE, UNIQUECARRIER, 
	FLIGHTNUM, ORIGIN, DEST, DEPTIME, ARRDELAY;

-- Early flights
early_flights = FILTER flights_w_date BY (ORIGIN == '$ORIGIN') AND (DEST == '$MIDDLE') AND (DEPTIME < '1200') AND (FLIGHTDATE == '$ORIGINDATE');

-- Late flights
late_flights = FILTER flights_w_date BY (ORIGIN == '$MIDDLE') AND (DEST == '$DEST') AND (DEPTIME > '1200') AND (FLIGHTDATE == '$MIDDLEDATE');

-- Lets join
join_flights  = JOIN 
	early_flights BY (DEST), 
	late_flights BY (ORIGIN);


report = FOREACH join_flights GENERATE 	CONCAT(early_flights::ORIGIN, '=>', early_flights::DEST), 
					CONCAT(early_flights::UNIQUECARRIER, ' ', early_flights::FLIGHTNUM),
                                       	CONCAT(early_flights::FLIGHTDATE, ' ', early_flights::DEPTIME), 
	   		               	CONCAT(late_flights::ORIGIN, '=>', late_flights::DEST),
					CONCAT(late_flights::UNIQUECARRIER, ' ', late_flights::FLIGHTNUM),
					CONCAT(late_flights::FLIGHTDATE, ' ', late_flights::DEPTIME),
					(early_flights::ARRDELAY + late_flights::ARRDELAY) AS totaldelay;

best_flights = ORDER report BY totaldelay ASC;

best_flight = LIMIT best_flights 1;

DUMP best_flight;
