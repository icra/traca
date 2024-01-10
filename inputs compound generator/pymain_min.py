import pandas as pd
from pathlib import Path
from lib.calibrationMainConcentration import read_industries, read_edars
from lib.db.ConnectPostgree import ConnectDb as pg
import sys
from pySWATPlus.TxtinoutReader import TxtinoutReader


conca = sys.argv[1]
txtinput_path = sys.argv[2]
removal_rate = sys.argv[3]
contaminant = sys.argv[4]


pg_url = "217.61.208.188"
pg_user = "traca_user"
pg_pass = "EdificiH2O!"
pg_db = "traca_1"
connection = pg(pg_url, pg_db, pg_user, pg_pass)


#Fitxers per executar en projecte
edar_data_xlsx = 'inputs/edar_data.xlsx'
industrial_data = 'inputs/industrial.xlsx'
recall_points = "inputs/recall_points.xlsx"

table_name = 'cens_v4_1_prova'    #Taula del cens industrial amb estimacions

#Posar info a fitxer .sqlite

contaminants_i_nutrients = [contaminant]

industries_to_edar, industries_to_river = connection.get_industries_to_edar_and_industry_separated(table_name)
id_discharge_to_volumes = read_industries(industries_to_river, industrial_data, recall_points, contaminants_i_nutrients, connection, removal_rate, conca)      #Dades de contaminants abocats directament a riu o a sortida depuradora
edars_calibrated = read_edars(contaminants_i_nutrients, industries_to_edar, edar_data_xlsx, removal_rate, recall_points, conca)    #Dades de contaminants despres de ser filtrats per edar


reader = TxtinoutReader(txtinput_path)
pollutants_om = reader.register_file('pollutants_om.exc', has_units = False, filter_by={'pollutants_pth': contaminants_i_nutrients})


exco_om = reader.register_file('exco_om.exc', has_units = False)
pollutants_om_df = pollutants_om.df
exco_om_df = exco_om.df

pollutants_om_df['load'] = 0
exco_om_df['cbod'] = 0
exco_om_df['sedp'] = 0
exco_om_df['orgn'] = 0
exco_om_df['no3'] = 0
exco_om_df['nh3'] = 0
exco_om_df['flo'] = 0
exco_om_df['solp'] = 0


def add_data_to_txtinout(pt, compounds):
        
    if 'DBO 5 dies' in compounds:
        dbo = compounds["DBO 5 dies"]
        exco_om_df.loc[exco_om_df['name'] == pt, 'cbod'] += dbo

    if 'Fòsfor orgànic' in compounds:
        fosfor = compounds["Fòsfor orgànic"]
        exco_om_df.loc[exco_om_df['name'] == pt, 'sedp'] += fosfor

    if 'Nitrogen orgànic' in compounds:
        ptl_n = compounds["Nitrogen orgànic"]  #organic nitrogen
        exco_om_df.loc[exco_om_df['name'] == pt, 'orgn'] += ptl_n

    if 'Amoniac' in compounds:
        nh3_n = compounds["Amoniac"]  #ammonia
        exco_om_df.loc[exco_om_df['name'] == pt, 'nh3'] += nh3_n

    if 'Nitrats' in compounds:
        no3_n = compounds["Nitrats"]  #nitrate
        exco_om_df.loc[exco_om_df['name'] == pt, 'no3'] += no3_n

    if 'q' in compounds:
        cabal = compounds["q"]            
        exco_om_df.loc[exco_om_df['name'] == pt, 'flo'] += cabal

    if 'Fosfats' in compounds:
        fosfats = compounds["Fosfats"]
        exco_om_df.loc[exco_om_df['name'] == pt, 'solp'] += fosfats

        
    #Per cadascun dels contaminants que no va a recall_dat, posar-lo a recall_pollutants_dat
    for contaminant in contaminants_i_nutrients:            
        if contaminant not in ["DBO 5 dies", "Fòsfor orgànic", "Nitrogen orgànic", "Amoniac", "Nitrats", "Fosfats"] and contaminant in compounds:
            load = compounds[contaminant]
            #print(load, contaminant, pt)
            pollutants_om_df.loc[(pollutants_om_df['recall_rec'] == pt) & (pollutants_om_df['pollutants_pth'] == contaminant), 'load'] += load


#llegir abocaments depuradores i afegir a pollutants_om_df
for es_code, wwtp in edars_calibrated.items():
    if "id_swat" in wwtp and "compounds_effluent" in wwtp:
        pt = wwtp['id_swat']
        compounds = wwtp['compounds_effluent']
        add_data_to_txtinout(pt, compounds)
        

#llegir abocaments industries i afegir a pollutants_om_df
"""
for pt, abocament in id_discharge_to_volumes.items():
    add_data_to_txtinout(pt, abocament)
"""


pollutants_om.df = pollutants_om_df


pollutants_om.overwrite_file()







