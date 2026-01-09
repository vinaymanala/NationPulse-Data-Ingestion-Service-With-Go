package service

var (
	PopulationTableColNames = []string{
		"REF_AREA",
		"Reference area",
		"MEASURE",
		"Measure",
		"SEX",
		"Sex",
		"Age",
		"TIME_PERIOD",
		"OBS_VALUE",
	}

	HealthTableColNames = []string{
		"REF_AREA",
		"Reference area",
		"MEASURE",
		"Measure",
		"SEX",
		"Sex",
		"Cause of death",
		"Unit of measure",
		"TIME_PERIOD",
		"OBS_VALUE",
	}

	EconomyGDPTableColNames = []string{
		"REF_AREA",
		"Reference area",
		"TRANSACTION",
		"Transaction",
		"TIME_PERIOD",
		"OBS_VALUE",
	}

	EcnomyGovTableColNames = []string{
		"REF_AREA",
		"Reference area",
		"MEASURE",
		"Measure",
		"TIME_PERIOD",
		"OBS_VALUE",
	}

	GrowthGDPTableColNames = []string{
		"REF_AREA",
		"Reference area",
		"MEASURE",
		"Measure",
		"TIME_PERIOD",
		"OBS_VALUE",
	}

	GrowthPopulationTableColNames = []string{
		"REF_AREA",
		"Reference area",
		"MEASURE",
		"Measure",
		"TIME_PERIOD",
		"OBS_VALUE",
	}

	PopulationTableName       = "poptable"
	HealthTableName           = "htable"
	EconomyGDPTableName       = "egdptable"
	EconomyGovTableName       = "egovtable"
	GrowthGDPTableName        = "ggdptable"
	GrowthPopulationTableName = "gpopulationtable"

	PopulationTableCreateSqlStatement  = `CALL create_population_table($1);`
	HealthTableCreateSqlStatement      = `CALL create_health_table($1);`
	EconomyGDPTableCreateSqlStatement  = `CALL create_economy_gdp_table($1);`
	EconomyGovTableCreateSqlStatement  = `CALL create_economy_gov_table($1);`
	GrowthGdpTableCreateSqlStatement   = `CALL create_growth_gdp_table($1);`
	GrowthPopulationCreateSqlStatement = `CALL create_growth_population_table($1);`

	PopulationTableInsertSqlStatement = `CALL insert_population_data(			
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$10		
		);`
	HealthTableInsertSqlStatment = `CALL insert_health_data(
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7,
		$8,
		$9,
		$10,
		$11
	);`
	EconomyGDPTableInsertSqlStatment = `CALL insert_economy_gdp_data(
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7
	);`
	EconomyGovTableInsertSqlStatement = `CALL insert_economy_gov_data(
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7
	);`
	GrowthGDPTableInsertSqlStatement = `CALL insert_growth_gdp_data(
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7
	);`
	GrowthPopulationTableInsertSqlStatement = `CALL insert_growth_population_data(
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7
	);`
)
