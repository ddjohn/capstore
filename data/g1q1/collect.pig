DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

a = LOAD '$FILE' USING CSVLoader(',') AS (
	PASSENGERS : int, 
	FREIGHT : int, 
	MAIL : int, 
	DISTANCES : int, 
	UNIQUE_CARRIER : chararray,
	AIRLINE_ID : int, 
	UNIQUE_CARRIER_NAME : chararray, 
	UNIQUE_CARRIER_ENTITY : chararray, 
	REGION : chararray, 
	CARRIER : chararray,
	CARRIER_NAME : chararray, 
	CARRIER_GROUP : int, 
	CARRIER_GROUP_NEW : int, 
	ORIGIN : chararray, 
	ORIGIN_CITY_NAME : chararray,
	ORIGIN_CITY_NUM : int, 
	ORIGIN_STATE_ABR : chararray, 
	ORIGIN_STATE_FIPS : chararray, 
	ORIGIN_STATE_NM : chararray, 
	ORIGIN_WAC : int,
	DEST : chararray, 
	DEST_CITY_NAME : chararray, 
	DEST_CITY_NUM : int, 
	DEST_STATE_ABR : chararray, 
	DEST_STATE_FIPS : chararray,
	DEST_STATE_NM : chararray,
	DEST_WAC : chararray,
	YEAR : int,
	QUARTER : int,
	MONTH : int,
	DISTANCE_GROUP : int,
	CLASS : chararray
);

b = foreach a generate ORIGIN, DEST;

DUMP b;
