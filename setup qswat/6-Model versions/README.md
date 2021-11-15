______________________________________________________________________________________________________________________________________________________________________________________

Conques Internes SWAT+ model
______________________________________________________________________________________________________________________________________________________________________________________

08/11/21, v01 (ci_01)

	QSWAT+ v2.0.6
	SWAT+ Editor v2.0.4
	SWAT+ rev60.5.4

Projecció de les capes: EPSG:25831 - ETRS89 / UTM zone 31N - Proyectado

-------------------------

Step 1: Delineate Watershed

Upload DEM

Use existing watershed: subbasins.shp, wshed.shp, channel.shp, reservoirs.shp (inlets/outlets shp)

Recalculate and overwrite existing -> Run -> OK


-------------------------

Step 2: Create HRUs

Landuse map: Corine
Soil map: European database

Slope bands [0, 2, 8, 9999] (recommended by Srini in SWAT+ Workshop)
Elevation bands NO (but Srini recommended using them if -parts of?- the watershed is above 1000 metres and there's more than a 300 m difference between subbasins)

Read choice: Read from maps
Short channel merged 0 ha

	Subbasins count: 34
	Channels count: 902
	Full HRUs count: 24778

Exempt landuses: CRIR, RICE & ORCD

Single/Multiple HRUs: 
select: Filter by landuse/soil/slope
	Landuse: 6%
	Soil: 11%
	Slope: 13%

Create HRUs -> 10061


-------------------------

Step 3: Edit Inputs and Run SWAT
 
   EDIT SWAT+ INPUTS

	CLIMATE-Weather Generator

Impor weather generator from 'swatplus_wgn_aemet_spain02.sqlite'
Mark box 'using observed weather data'

	CLIMATE-Weather Stations

Import data from stations, use SWAT2012 format.
Weather Stations -> Import Data -> browse folder "...\Q-SWAT+TRACA - conques_internes\Data to build SWAT+ model\7-Weather" and Start import

   RUN SWAT+ 

	Set your simulation period (based on observed files, default)

Starting date of simulation: 1 de enero de 2000
Ending date of simulation: 14 de marzo de 2021
Time steps in a day for rainfall, runoff and routing: Daily

	Choose output to print (as needed)

Warm-up period: 1

	SIMULATION-Time
Number of years to not print output: 1
Beginning Julian Day of simulation to start printing output files for daily printing only: 0
Beginning year of simulation to start printing output files: 0
Ending Julian Day of simulation to start printing output files for daily printing only: 0
Ending year of simulation to start printing output files: 0
Daily print within the period (e.g., interval=2 will print every other day): 1



______________________________________________________________________________________________________________________________________________________________________________________




15/11/21, v02 (ci_02)

	QSWAT+ v2.0.6
	SWAT+ Editor v2.0.4
	SWAT+ rev60.5.4

Projecció de les capes: EPSG:25831 - ETRS89 / UTM zone 31N - Proyectado

-------------------------

Step 1: Delineate Watershed

Upload DEM

Use existing watershed: subbasins.shp, wshed.shp, channel.shp, reservoirs.shp (inlets/outlets shp)

Recalculate and overwrite existing -> Run -> OK


-------------------------

Step 2: Create HRUs

Landuse map: Corine. Modified version, CRIR landuse split in CRIR, BARL, ALFA and CORN
Soil map: European database

Slope bands [0, 2, 8, 9999] (recommended by Srini in SWAT+ Workshop)
Elevation bands NO (but Srini recommended using them if -parts of?- the watershed is above 1000 metres and there's more than a 300 m difference between subbasins)

Read choice: Read from maps
Short channel merged 0 ha

	Subbasins count: 34
	Channels count: 902
	Full HRUs count: 26150 

Exempt landuses: CRIR, BARL, ALFA, CORN, RICE & ORCD

Single/Multiple HRUs: 
select: Filter by landuse/soil/slope
	Landuse: 6%
	Soil: 11%
	Slope: 13%

Create HRUs -> 


-------------------------

Step 3: Edit Inputs and Run SWAT
 
   EDIT SWAT+ INPUTS

	CLIMATE-Weather Generator

Impor weather generator from 'swatplus_wgn_aemet_spain02.sqlite'
Mark box 'using observed weather data'

	CLIMATE-Weather Stations

Import data from stations, use SWAT2012 format.
Weather Stations -> Import Data -> browse folder "...\Q-SWAT+TRACA - conques_internes\Data to build SWAT+ model\7-Weather" and Start import

   RUN SWAT+ 

	Set your simulation period (based on observed files, default)

Starting date of simulation: 1 de enero de 2000
Ending date of simulation: 14 de marzo de 2021
Time steps in a day for rainfall, runoff and routing: Daily

	Choose output to print (as needed)

Warm-up period: 1

	SIMULATION-Time
Number of years to not print output: 1
Beginning Julian Day of simulation to start printing output files for daily printing only: 0
Beginning year of simulation to start printing output files: 0
Ending Julian Day of simulation to start printing output files for daily printing only: 0
Ending year of simulation to start printing output files: 0
Daily print within the period (e.g., interval=2 will print every other day): 1


	


