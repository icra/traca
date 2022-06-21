import PySimpleGUI
from GUI.views.mainGUI import mainGUI as mainGUI
from lib.calibrationMainConcentration import read_industries, read_edars, exportDataForNils, wwtp_info
from lib.db.renameSQLite import renameSQLite as rS
from GUI.views.settingsGUI import settingsGUI as settingsGUI
from lib.db.ConnectPostgree import ConnectDb as pg
import csv
import sys



pg_url = "icra.loading.net"
pg_user = "traca_user"
pg_pass = "EdificiH2O!"
pg_db = "traca_1"
connection = pg(pg_url, pg_db, pg_user, pg_pass)

edar_data_xlsx = 'inputs/edar_data.xlsx'
removal_rate = "inputs/removal_rate.xlsx"
industrial_data = 'inputs/industrial.xlsx'
recall_points = "inputs/recall_points.xlsx"
edar_ptr = "inputs/prtr_edars.xlsx"
analitiques_sistemes = "inputs/edars_analitiques_sistemes_2.xlsx"
review = "inputs/review.xlsx"
resum_eliminacio = "inputs/resum_eliminacio.xlsx"


#Generar concentracions i penjar-les a DB
"""
contaminants_i_nutrients = connection.get_contaminants_i_nutrients_tipics()
estimations = connection.generate_industrial_data()
industries = connection.read_all_data('cens_v4')
connection.upload_data(industries, contaminants_i_nutrients, estimations)
"""

#Posar info a fitxer .sqlite
contaminants_i_nutrients = connection.get_contaminants_i_nutrients_tipics()
industries_to_edar, industries_to_river = connection.get_industries_to_edar_and_industry_separated()
id_discharge_to_volumes = read_industries(industries_to_river, industrial_data, recall_points, contaminants_i_nutrients)
edars_calibrated = read_edars(contaminants_i_nutrients, industries_to_edar, edar_data_xlsx, removal_rate, recall_points)


#Calibrar
"""
exportDataForNils(industries_to_edar, contaminants_i_nutrients, edar_data_xlsx, analitiques_sistemes, edar_ptr)
wwtp_info(review, contaminants_i_nutrients, resum_eliminacio)
"""

#cli
if len(sys.argv) > 2:
    db_url = sys.argv[1]
    renameHelper = rS(db_url)
    renameHelper.add_data_to_swat(edars_calibrated, id_discharge_to_volumes)

#gui
else:
    mGUI = mainGUI()
    sGUI = settingsGUI()
    mGUI.update_table(edars_calibrated)
    mGUI.update_table_in(id_discharge_to_volumes)

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
                    renameHelper.add_data_to_swat(edars_calibrated, id_discharge_to_volumes, contaminants_i_nutrients)
                    PySimpleGUI.popup("Data added successfully!")

            except Exception as e:
                print(str(e))
                PySimpleGUI.popup("Error: " + str(e))
        if event == PySimpleGUI.WIN_CLOSED:
            win.close()
            if win == mGUI.window:
                break

    mGUI.window.close()
