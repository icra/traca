import PySimpleGUI
import pandas as pd

from GUI.views.mainGUI import mainGUI as mainGUI
from lib.calibrationMainConcentration import read_industries, read_edars, exportDataForNils, wwtp_info
from lib.db.renameSQLite import renameSQLite as rS
from GUI.views.settingsGUI import settingsGUI as settingsGUI
from lib.db.ConnectPostgree import ConnectDb as pg
import sys
import lib.scenarios as scenarios
import threading
import sys, os
import pandas
import json


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

def run_scenarios_parallel(window, main_window, n_iteracions):

    scenarios.run_scenarios(connection, industrial_data, recall_points, contaminants_puntuals,
                            edar_data_xlsx, removal_rate, industries_to_edar, industries_to_river,
                            edars_calibrated, file_name, n_iteracions, window, graph_location, river_attenuation, excel_scenario, coord_to_pixel, coord_to_codi, llindars, resultat_escenaris, abocaments_ci, id_pixel, conca)


    main_window.write_event_value('scenario_generator_ended', '')
    window.close()

pg_url = "icra.loading.net"
pg_user = "traca_user"
pg_pass = "EdificiH2O!"
pg_db = "traca_1"
connection = pg(pg_url, pg_db, pg_user, pg_pass)

pujar_concentracions_db = False
fitxers_calibracio_nils = False
generate_exe = False
export_graph_to_csv = False
export_edars_calibrated_json = False
conca = 'besos'

if generate_exe:
#Codi per generar executable
    """
    pyinstaller --onefile --add-data="inputs/industrial.xlsx;inputs" --add-data="inputs/recall_points.xlsx;inputs" --add-data="inputs/atenuacions_generacions.xlsx;inputs" --add-data="inputs/edar_data.xlsx;inputs" --add-data="inputs/catalonia_graph_2022.pkl;inputs" --add-data="inputs/percentatges_eliminacio_tots_calibrats.csv;inputs" --add-data="inputs/excel_scenario.xlsx;inputs" --add-data="inputs/coords_to_pixel_llob.csv;inputs" --add-data="inputs/coord_codi_llob.csv;inputs" --add-data="inputs/llindars_massa_aigua.xlsx;inputs" --add-data="inputs/abocaments_ci.csv;inputs" --add-data="inputs/AGG_WWTP_df_no_treatment.csv;inputs" pymain.py
    """
    edar_data_xlsx = 'Inputs/edar_data.xlsx'
    removal_rate = "Inputs/atenuacions_generacions.xlsx"
    industrial_data = resource_path('inputs/industrial.xlsx')
    recall_points = resource_path("inputs/recall_points.xlsx")
    edar_ptr = "inputs/prtr_edars.xlsx"
    analitiques_sistemes = "inputs/edars_analitiques_sistemes_2.xlsx"
    review = "inputs/review.xlsx"
    resum_eliminacio = "inputs/resum_eliminacio.xlsx"
    graph_location = resource_path("inputs/catalonia_graph_2022.pkl")
    river_attenuation = resource_path("inputs/percentatges_eliminacio_tots_calibrats.csv")
    excel_scenario = resource_path("inputs/excel_scenario.xlsx")
    coord_to_pixel = resource_path("inputs/coords_to_pixel_llob.csv")
    coord_to_codi = resource_path("inputs/coord_codi_llob.csv")
    llindars = "Inputs/llindars_massa_aigua.xlsx"
    resultat_escenaris = "Resultats/resultat.json"
    abocaments_ci = resource_path("inputs/abocaments_ci.csv")
    id_pixel = resource_path("inputs/AGG_WWTP_df_no_treatment.csv")
else:
    #Fitxers per executar en projecte
    edar_data_xlsx = 'inputs/edar_data.xlsx'
    removal_rate = "inputs/atenuacions_generacions.xlsx"
    industrial_data = 'inputs/industrial.xlsx'
    recall_points = "inputs/recall_points.xlsx"
    edar_ptr = "inputs/prtr_edars.xlsx"
    analitiques_sistemes = "inputs/edars_analitiques_sistemes_2.xlsx"
    review = "inputs/review.xlsx"
    resum_eliminacio = "inputs/resum_eliminacio.xlsx"
    graph_location = "inputs/catalonia_graph_2022.pkl"
    graph_location = "inputs/catalonia_graph_2022.pkl"
    river_attenuation = "inputs/percentatges_eliminacio_tots_calibrats.csv"
    excel_scenario = "inputs/excel_scenario.xlsx"
    coord_to_pixel = "inputs/coords_to_pixel_llob.csv"
    coord_to_codi = "inputs/coord_codi_llob.csv"
    llindars = "inputs/llindars_massa_aigua.xlsx"
    resultat_escenaris = "resultat.json"
    abocaments_ci = "inputs/abocaments_ci.csv"
    id_pixel = "inputs/AGG_WWTP_df_no_treatment.csv"
    compound_features_path = "inputs/compound_features.xlsx"

table_name = 'cens_v4_1_prova'    #Taula del cens industrial amb estimacions
if pujar_concentracions_db:
    #Generar concentracions i estimacions i penjar-les a DB (esborra table_name, en cas que existeixi, i en crea una de nova)
    contaminants_i_nutrients = connection.get_contaminants_i_nutrients_tipics()
    estimations = connection.generate_industrial_data()
    industries = connection.read_all_data('cens_v4')
    connection.upload_data(industries, contaminants_i_nutrients, estimations, table_name = table_name)

    #Estadístiques de dades que es pengen a DB
    """
    pollutants_per_ccae = connection.matrix_size()
    industries = connection.read_all_data('cens_v4')
    n_cells = 0
    for industry in industries:
        new_ccae = connection.ccae_remove_category(industries[industry]["cod_ccae"], 1)
        if new_ccae in pollutants_per_ccae:
            for pollutant in pollutants_per_ccae[new_ccae]:
                if pollutant in industries[industry] and industries[industry][pollutant] > 0:
                    n_cells += 1
    print(n_cells)
    """

#Mes estadistiques
#connection.estadistiques_final()

#Posar info a fitxer .sqlite
contaminants_i_nutrients = connection.get_contaminants_i_nutrients_tipics() #Tots els contaminants
contaminants_calibrats_depuradora = connection.get_contaminants_i_nutrients_calibrats_wwtp()    #Contaminants que hem pogut calibrar a EDAR

industries_to_edar, industries_to_river = connection.get_industries_to_edar_and_industry_separated(table_name)
id_discharge_to_volumes = read_industries(industries_to_river, industrial_data, recall_points, contaminants_i_nutrients, connection, removal_rate, conca)      #Dades de contaminants abocats directament a riu o a sortida depuradora
edars_calibrated = read_edars(contaminants_i_nutrients, industries_to_edar, edar_data_xlsx, removal_rate, recall_points, conca)    #Dades de contaminants despres de ser filtrats per edar



contaminants_puntuals = connection.get_contaminants_i_nutrients_puntuals()  #Contaminants nomes d'origen puntual (per generacio escenaris)
#contaminants_puntuals = ["Diuron", "Coure", "Naftalè", "Indeno(1.2.3-C.D)pirè", "Fluorantè", "Benzo(b)fluorantè", "Antracè"]



rows = []

edars_llob = ["ES9081130006010E",
             "ES9080480001010E",
             "ES9083020001010E",
             "ES9083000004010E",
             "ES9082910001010E",
             "ES9081620002010E",
             "ES9081610008010E",
             "ES9082400005010E",
             "ES9081140002010E",
             "ES9082110001010E",
             "ES9082220003010E",
             "ES9081190002010E",
             "ES9081840001010E",
             "ES9082050005010E",
             "ES9082870007010E",
             "ES9080440001010E",
             "ES9080010001010E",
             "ES9081270001010E",
             "ES9080530002010E",
             "ES9080980004010E",
             "ES9080910001010E",
             "ES9082790004050E"]


"""
for edar in edars_llob:

    nt = edars_calibrated[edar]["compounds_effluent"]["Nitrogen Total"]
    ft = edars_calibrated[edar]["compounds_effluent"]["Fòsfor total"]
    cabal = edars_calibrated[edar]["compounds_effluent"]["q"]
    poblacio = edars_calibrated[edar]['population_real']

    print(edars_calibrated[edar])


    rows.append({'Codi': edar, 'Cabal': cabal, "Nitrogen total": nt, "Fòsfor total": ft, "Poblacio": poblacio})

df = pd.DataFrame(rows)
df.to_csv("nitrogen_fosfor_edars_llobregat.csv")
"""

if fitxers_calibracio_nils:
    #Fitxers per calibrar contaminacio a depuradora (en nils te l'script)
    exportDataForNils(industries_to_edar, contaminants_i_nutrients, edar_data_xlsx, analitiques_sistemes, edar_ptr, connection, file_name ="scripts/inputs/calibracio_contaminants.json")
    wwtp_info(review, contaminants_i_nutrients, resum_eliminacio, file_name='edars_pollutant_attenuation.json')

if export_graph_to_csv:
    file_name = 'graph.csv'
    renameHelper = rS(None)
    renameHelper.export_graph_csv(edars_calibrated, id_discharge_to_volumes, contaminants_puntuals, file_name = file_name)

if export_edars_calibrated_json:
    with open("scripts/inputs/edars_calibrated.json", 'w', encoding='utf-8') as outfile:
        json.dump(edars_calibrated, outfile, ensure_ascii=False)
#cli
if len(sys.argv) > 2:
    db_url = sys.argv[1]
    renameHelper = rS(db_url)
    renameHelper.add_data_to_swat(edars_calibrated, id_discharge_to_volumes, compound_features_path, conca)

#gui
else:
    mGUI = mainGUI(contaminants_calibrats_depuradora)
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

        """
        if event == 'File properties':
            print("Open Settings Window")
            if sGUI.configWindow is None:
                sGUI.createWindow(mGUI.window)
        """
        if event == "add_dp_data":
            try:
                if len(values["swat_db_sqlite"]) == 0: #No ha penjat db
                    PySimpleGUI.popup('Upload .SQL project first!')

                elif PySimpleGUI.popup_yes_no('This action will overwrite the uploaded file.\nDo you want to continue?') == "Yes":
                    renameHelper = rS(values["swat_db_sqlite"])
                    renameHelper.add_data_to_swat(edars_calibrated, id_discharge_to_volumes, contaminants_i_nutrients, compound_features_path, conca)
                    PySimpleGUI.popup("Data added successfully!")

            except Exception as e:
                print(str(e))
                PySimpleGUI.popup("Error: " + str(e))
        if event == "pollutants_generator":
            try:
                file_name = PySimpleGUI.popup_get_text('Escriu nom del fitxer per guardar els resultats (exemple: generacio_contaminacio.xlsx)')
                if file_name is not None:

                    if not file_name.endswith(".xlsx"):
                        file_name += ".xlsx"

                    file_name = "Resultats/"+file_name

                    renameHelper = rS(None)
                    renameHelper.add_data_to_excel(edars_calibrated, id_discharge_to_volumes, contaminants_i_nutrients, file_name)
                    PySimpleGUI.popup("Operació realitzada correctament! Resultat guardat a la carpeta Resultats")

            except Exception as e:
                print(str(e))
                PySimpleGUI.popup("Error: " + str(e))
        if event == "scenarios_generator":
            try:
                layout = [
                    #Input text with default value
                    [PySimpleGUI.Text('Nom del fitxer per guardar els resultats', size=(30, 1)), PySimpleGUI.InputText(key='file_name', default_text='generacio_escenaris.xlsx')],
                    #Input number field with default value
                    [PySimpleGUI.Text('Nombre de simulacions', size=(30, 1)), PySimpleGUI.InputText(key='n_scenarios', default_text='100')],
                    [PySimpleGUI.Text('')],
                    #Checkboxes
                    [PySimpleGUI.Text('Filtres:', font='Any 12', size=(30, 1))],
                    [PySimpleGUI.Checkbox('Terciaris a depuradores petites', key='puntual', default=True)],
                    [PySimpleGUI.Checkbox('Permetre trens tractament amb O3', key='difusa', default=True)],
                    [PySimpleGUI.Text('')],
                    [PySimpleGUI.Button('Ok'), PySimpleGUI.Button('Cancel')]
                ]

                pop_up_escenaris = PySimpleGUI.Window("Generació d'escenaris", layout, size=(600, 300),
                                   )
                event, values = pop_up_escenaris.read()
                pop_up_escenaris.close()

                if event == "Ok":
                    if not values['file_name'].endswith(".xlsx"):
                        file_name = values['file_name'] + ".xlsx"
                    else:
                        file_name = values['file_name']

                    file_name = "Resultats/" + file_name

                    if not values["n_scenarios"].isdigit():
                        n_iteracions = 100
                    else:
                        n_iteracions = int(values["n_scenarios"])

                    if n_iteracions > 3512320:
                        n_iteracions = 3512320
                    elif n_iteracions <= 0:
                        n_iteracions = 1

                    layout = [[PySimpleGUI.ProgressBar(max_value=n_iteracions, orientation='h', size=(20, 20),
                                                       key='progress_1')]]
                    popup_window = PySimpleGUI.Window('Test', layout, finalize=True, modal=True)

                    threading.Thread(target=run_scenarios_parallel,
                                     args=(popup_window, win, n_iteracions),
                                     daemon=True).start()



            except Exception as e:
                print(str(e))
                PySimpleGUI.popup("Error: " + str(e))


        if event == 'scenario_generator_ended':
            PySimpleGUI.popup("Operació realitzada correctament! Resultat guardat a la carpeta Resultats")

        if event == PySimpleGUI.WIN_CLOSED:
            win.close()
            if win == mGUI.window:
                break

    mGUI.window.close()

#%%
