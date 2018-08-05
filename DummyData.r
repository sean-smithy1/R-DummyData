
library(RMariaDB)
library(lubridate)

#Connect to database - .my.cnf has the group 6sigma for connection details
con <- dbConnect(RMariaDB::MariaDB(), group='6sigma')

#Constants
data.years = 1
end.date <- ymd_hms("2018/08/3 15:00:00", tz = "GMT") # Metrics Gathered 3pm each day
#Function to calculate number of periods in single year
year_periods = function(period) { 
	switch(period,
		'Daily' = 365,
		'Weekly' = 52,
		'Fortnightly' = 26,
		'Monthly' = 12,
		'Quarterly' = 4,
		'Yearly' = 1
		)
	}

# Function to write dummy data to DB
write_dummy_data = function(metric.id, freq, target) {
	total.periods = freq * data.years

	# Initialise DataFrame
	metric.measures <- data.frame(
		metric_id = integer(),
    created_at = as.POSIXct(character())
    )

	#Add measure and created_at to dataframe
	for (row in 1:total.periods) {
		created.at <- end.date-row*365/freq*86400 # days decimal-min
		metric.measures[row, "metric_id"] <- metric.id
		metric.measures[row, "created_at"] <- created.at
	}

	#Create normal data vector
	measure <- rnorm(total.periods, target, 1)
		
	# Add measure vector to dataframe
	metric.measures['measure'] <- measure
	
	# DEBUG turn off writing to DB 
	# print(metric.measures)

	dbWriteTable(con, value = metric.measures, name = "metric_measures", append = TRUE ) 

	# Remove dataframe object
	rm(metric.measures)
}

#Get metrics to build based on flags[is_measured=TRUE AND is_dashboard_item=TRUE]
metrics <- dbGetQuery(con, 
	'SELECT id, measure_freq, target, ntol, ptol, yearly_quantity
	FROM metrics
	WHERE is_measured IS TRUE;')

	print(paste("Number of Records:", nrow(metrics)))
	# DEBUG
	# metrics


# Itterate through metrics dataframe 
for (row in 1:nrow(metrics)) {
	
	if(metrics[row, 'measure_freq'] == 'Each') {
		# Each is just a count
		total.periods = metrics[row, 'yearly_quantity'] * data.years
		freq = metrics[row, 'yearly_quantity']
	} else {
		# Calculate Time periods using year_periods
		total.periods = year_periods(metrics[row, 'measure_freq']) * data.years
		freq = year_periods(metrics[row, 'measure_freq'])
	}

	metric.id <- as.integer(metrics[row, 'id'])
	target <- metrics[row, 'target']
	# To Add
	# trend
	# volitility (SD)
	
	# Clear out chnaged metrics based on metric_id
	# This will allow us to use flags to recreate only the ones we need to 
	recs <- dbExecute(con, 
		paste("DELETE 
		FROM metric_measures
		WHERE metric_id = ", metric.id, ";", sep= "")
		)

	print(paste("Records Deleted: ", recs))

	write_dummy_data(metric.id, freq, target)
}


dbDisconnect(con)
