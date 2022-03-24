import PySimpleGUI
from GUI.views.mainGUI import mainGUI as mainGUI
from lib.calibrationMainConcentration import read_edars
from lib.db.renameSQLite import renameSQLite as rS
from GUI.views.settingsGUI import settingsGUI as settingsGUI


mGUI = mainGUI()
sGUI = settingsGUI()

edar_compounds_csv = "inputs/EDAR_DBO_TN_PT_media2.csv"
edar_population_csv = "inputs/dp_population_country.csv"
edar_analitiques_xlsx = "inputs/edars_analitiques_sistemes_1.xlsx"
edar_ptr_xlsx = "inputs/prtr_edars.xlsx"
swat_to_edar_code_csv = "inputs/recall_ci_05.csv"
edar_cabals_xlsx = "inputs/20017_07_008_EDAR i Cabals (rev Medi).xlsx"
removal_rate_csv = "inputs/removal_rate.csv"


edars_calibrated = read_edars(swat_to_edar_code_csv, edar_compounds_csv, edar_population_csv, edar_analitiques_xlsx, edar_ptr_xlsx, edar_cabals_xlsx, removal_rate_csv)

"""
print(edars_calibrated["ES9430920002010E"])
print(edars_calibrated["ES9430920002011E"])
print(edars_calibrated["ES9171630006010E"])
print(edars_calibrated["ES9431450003010E"])
"""

mGUI.update_table(edars_calibrated)


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
                renameHelper.add_data_to_swat(edars_calibrated)
                PySimpleGUI.popup("WWTP's renamed successfully!")

        except Exception as e:
            print(str(e))




    if event == PySimpleGUI.WIN_CLOSED:
        win.close()
        if win == mGUI.window:
            break

mGUI.window.close()
