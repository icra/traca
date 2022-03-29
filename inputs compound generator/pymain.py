import PySimpleGUI
from GUI.views.mainGUI import mainGUI as mainGUI
from lib.calibrationMainConcentration import read_edars
from lib.db.renameSQLite import renameSQLite as rS
from GUI.views.settingsGUI import settingsGUI as settingsGUI
from lib.db.ConnectPostgree import ConnectDb as pg
import csv


def readListOfIndustriesFromCSV(industrial_to_river_csv):
    # Reads the first column of the csv file with the industrial to river mapping
    # Returns a list of strings
    industries = {}
    with open(industrial_to_river_csv, encoding='windows-1252', newline='') as f:
        reader = csv.reader(f, delimiter=',')
        isFirst = True
        for row in reader:
            if isFirst:
                isFirst = False
            else:
                if row[0]+' '+row[1] not in industries:
                    industries[row[0]+' '+row[1]] = {
                        "name": row[0]+' '+row[1],
                        "activitat": row[0],
                        "abocament": row[1],
                        "point": row[2],
                        "count": 0,
                        "list": []
                    }
                if(row[2] not in industries[row[0]+' '+row[1]]["list"]):
                    industries[row[0]+' '+row[1]]["count"] += 1
                    industries[row[0]+' '+row[1]]["list"].append(row[1])
    return industries

def groupIndustriesByActivityName(industries, industries_point):
    # Groups the industries by activity name
    # Returns a list of dictionaries
    grouped_industries = {}
    for industry in industries:
        if industry[1]+' '+industry[2] not in grouped_industries:
            grouped_industries[industry[1]+' '+industry[2]] = []
        grouped_industries[industry[1]+' '+industry[2]].append({
            "tid": industry[0],
            "activitat/ubicacio": industry[1],
            "nom_abocament": industry[2],
            "cod_ccae": industry[3],
            "ccae": industry[4],
            "Tipus (LLM)": industry[5],
            "Subtipus (LLM)": industry[6],
            "nom_variable": industry[7],
            "valor_maxim": industry[8],
            "unitats": industry[9],
            "point": industries_point[industry[1]+' '+industry[2]]["point"],
        })
    return grouped_industries

def volumes_to_point(industries_grouped, swat_to_edar_code_csv):
    # Calculates the volumes of each industry and assing them to the point of the river
    # Returns a list of dictionaries
    points = {}
    aux_point = {}
    with open(swat_to_edar_code_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        point_2_id = {}
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                if row[9] != '':
                    if row[9] not in point_2_id:
                        point_2_id[row[9]] = row[0]


    for industry in industries_grouped:
        if industries_grouped[industry][0]["point"] != '':
            if industries_grouped[industry][0]["point"] not in point_2_id:
                print("Error: point not found in the csv file", industries_grouped[industry][0])
            else:
                aux_point = {
                    "activitat": industries_grouped[industry][0]["activitat/ubicacio"],
                    "abocament": industries_grouped[industry][0]["nom_abocament"],
                    "point": industries_grouped[industry][0]["point"],
                    "id": point_2_id[industries_grouped[industry][0]["point"]],
                    "q": 0,
                    "dbo": 0,
                    "phosphor": 0,
                    "nitrogen": 0,
                    "amoni": 0
                }
                for compound in industries_grouped[industry]:
                    if compound["nom_variable"] == "Cabal diari":
                        aux_point["q"] = float(compound["valor_maxim"])
                    if compound["nom_variable"] == "Cabal anual":
                        if aux_point["q"] != 0:
                            aux_point["q"] = float(compound["valor_maxim"])/365
                    if compound["nom_variable"] == "DBO 5 dies":
                        aux_point["dbo"] = float(compound["valor_maxim"])
                    if compound["nom_variable"] == "Fosfor total":
                        aux_point["phosphor"] = float(compound["valor_maxim"])
                    if compound["nom_variable"] == "Amoni":
                        aux_point["amoni"] = float(compound["valor_maxim"])
                    if compound["nom_variable"] == "Amoniac":
                        aux_point["amoni"] = float(compound["valor_maxim"])
                    if compound["nom_variable"] == "Nitrogen":
                        aux_point["nitrogen"] = float(compound["valor_maxim"])
                aux_point["dbo"] = aux_point["dbo"] * aux_point["q"]
                aux_point["phosphor"] = aux_point["phosphor"] * aux_point["q"]
                aux_point["amoni"] = aux_point["amoni"] * aux_point["q"]
                aux_point["nitrogen"] = aux_point["nitrogen"] * aux_point["q"]

                if aux_point["id"] not in points:
                    points[aux_point["id"]] = aux_point
                else:
                    points[aux_point["id"]]["q"] += aux_point["q"]
                    points[aux_point["id"]]["dbo"] += aux_point["dbo"]
                    points[aux_point["id"]]["phosphor"] += aux_point["phosphor"]
                    points[aux_point["id"]]["amoni"] += aux_point["amoni"]
                    points[aux_point["id"]]["nitrogen"] += aux_point["nitrogen"]
    return points

edar_compounds_csv = "inputs/EDAR_DBO_TN_PT_media2.csv"
edar_population_csv = "inputs/dp_population_country.csv"
edar_analitiques_xlsx = "inputs/edars_analitiques_sistemes_1.xlsx"
edar_ptr_xlsx = "inputs/prtr_edars.xlsx"
swat_to_edar_code_csv = "inputs/recall_ci_05.csv"
edar_cabals_xlsx = "inputs/20017_07_008_EDAR i Cabals (rev Medi).xlsx"
removal_rate_csv = "inputs/removal_rate.csv"
industrial_to_river_csv = "inputs/industrial.csv"


edars_calibrated = read_edars(swat_to_edar_code_csv, edar_compounds_csv, edar_population_csv, edar_analitiques_xlsx, edar_ptr_xlsx, edar_cabals_xlsx, removal_rate_csv)

pg_url = "icra.loading.net"
pg_user = "traca_user"
pg_pass = "EdificiH2O!"
pg_db = "traca_1"

connection = pg(pg_url, pg_db, pg_user, pg_pass)

industries_list = readListOfIndustriesFromCSV(industrial_to_river_csv)
# print(industries_list)
industries = connection.getIndustriesToRiver(industries_list)
print(industries[0])
industries_grouped = groupIndustriesByActivityName(industries, industries_list)
# for industry_name in industries_grouped:
#     for industry in industries_grouped[industry_name]:
#         if(industry["nom_variable"] == "Cabal"):
#             print(industry)
#         if (industry["nom_variable"] == "Cabal diari"):
#             print(industry)
#         if (industry["nom_variable"] == "Cabal anual"):
#             print(industry)

#Show the volumes of each industry
volumes = volumes_to_point(industries_grouped, swat_to_edar_code_csv)
for volume in volumes:
    print(volumes[volume])

"""
print(edars_calibrated["ES9430920002010E"])
print(edars_calibrated["ES9430920002011E"])
print(edars_calibrated["ES9171630006010E"])
print(edars_calibrated["ES9431450003010E"])
"""
# exit(1)

mGUI = mainGUI()
sGUI = settingsGUI()
mGUI.update_table(edars_calibrated)
mGUI.update_table_in(volumes)

while True:
    win, event, values = PySimpleGUI.read_all_windows()
    # Tanca el programa si es tanca l'app
    # print(win, event, values)
    print(event)
    if event != '__TIMEOUT__':
        print(values)
    if event == 'File properties':
        print("Open Settings Window")
        if sGUI.configWindow is None:
            sGUI.createWindow(mGUI.window)
    if event == "add_dp_data":
        try:
            if len(values["swat_db_sqlite"]) == 0: #No ha penjat db
                PySimpleGUI.popup('Upload .SQL project first!')

            elif PySimpleGUI.popup_yes_no('This action will overwrite the uploaded file.\nDo you want to continue?') == "Yes":
                renameHelper = rS(values["swat_db_sqlite"])
                renameHelper.add_data_to_swat(edars_calibrated, volumes)
                PySimpleGUI.popup("WWTP's renamed successfully!")

        except Exception as e:
            print(str(e))
            PySimpleGUI.popup("Error: " + str(e))
    if event == PySimpleGUI.WIN_CLOSED:
        win.close()
        if win == mGUI.window:
            break

mGUI.window.close()