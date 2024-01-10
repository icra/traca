import pandas as pd
from pathlib import Path
from lib.calibrationMainConcentration import read_industries, read_edars, exportDataForNils, wwtp_info
from lib.db.ConnectPostgree import ConnectDb as pg
import sys

conca = sys.argv[1]
removal_rate = sys.argv[2]
contaminant = sys.argv[3]

pg_url = "217.61.208.188"
pg_user = "traca_user"
pg_pass = "EdificiH2O!"
pg_db = "traca_1"
connection = pg(pg_url, pg_db, pg_user, pg_pass)

#Fitxers per executar en projecte
edar_data_xlsx = 'inputs/edar_data.xlsx'
industrial_data = 'inputs/industrial.xlsx'
recall_points = "inputs/recall_points.xlsx"
edar_ptr = "inputs/prtr_edars.xlsx"
analitiques_sistemes = "inputs/edars_analitiques_sistemes_2.xlsx"
review = "inputs/review.xlsx"
resum_eliminacio = "inputs/resum_eliminacio.xlsx"
table_name = 'cens_v4_1_prova'    #Taula del cens industrial amb estimacions
contaminants_i_nutrients = [contaminant]


industries_to_edar, industries_to_river = connection.get_industries_to_edar_and_industry_separated(table_name)
id_discharge_to_volumes = read_industries(industries_to_river, industrial_data, recall_points, contaminants_i_nutrients, connection, removal_rate, conca)      #Dades de contaminants abocats directament a riu o a sortida depuradora
edars_calibrated = read_edars(contaminants_i_nutrients, industries_to_edar, edar_data_xlsx, removal_rate, recall_points, conca)    #Dades de contaminants despres de ser filtrats per edar

#contaminacio = exportDataForNils(industries_to_edar, contaminants_i_nutrients, edar_data_xlsx, analitiques_sistemes, edar_ptr, connection, file_name = None)
#coeficients = wwtp_info(review, contaminants_i_nutrients, resum_eliminacio, file_name= None)

print(edars_calibrated)
#return contaminacio, coeficients

#sys.stdout.write('Bugs: 5|Other: 10\n')
#sys.stdout.flush()




