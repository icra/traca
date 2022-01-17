import PySimpleGUI
from lib.db.Custom_SQLite import Custom_SQLite as cS
from lib.db.ConnectPostgree import ConnectDb as pg
from lib.db.renameSQLite import renameSQLite as rS
import lib.utils as utils

from GUI.views.mainGUI import mainGUI as mainGUI
from GUI.views.settingsGUI import settingsGUI as settingsGUI
from GUI.views.renameGUI import renameGUI as renameGUI
import csv
import xlsxwriter
import sqlite3
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np

# Preprocesado y modelado
# ==============================================================================
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy.optimize import curve_fit, least_squares

config_db_url = r"db\config.sqlite"
mGUI = mainGUI()
sGUI = settingsGUI(config_db_url)
rGUI = renameGUI(config_db_url)

def init():
    config_db = cS(config_db_url)
    config_db.create_connection()

init()
#window = PySimpleGUI.Window("IG+", mGUI.init_GUI(), finalize=True, location=(0, -1000))
window = PySimpleGUI.Window("IG+", mGUI.init_GUI(), finalize=True, location=(0,0))

#configWindow = PySimpleGUI.Window("Settings", config_db_GUI(), modal=True, finalize=True)

sGUI.refreshList(0)
sGUI.load_data(0)

if sGUI.selected_config is not None:

    connection = pg(sGUI.selected_config.postgre_url, sGUI.selected_config.postgre_dbname,
                    sGUI.selected_config.postgre_user, sGUI.selected_config.postgre_pass)

    wwtps = connection.getAllWWTP()
    industries = connection.getIndustriesToEdar()
    acceptedWWTP = []
    for wwtp in wwtps:
        acceptedWWTP.append(wwtp[0])

    window['dp_total'].update("Number of discharge points: " + str(len(wwtps)))

    window['industries_total'].update("Number of industries: " + str(len(industries)))

    # Fields of edar compounts
    # cabal_diari m3/dia
    # unidades mg/l

    listOfEDARCompounds = {}
    with open(sGUI.selected_config.wwtp_con_db, encoding='utf8', newline='') as csvEDARCompounts:
        reader = csv.reader(csvEDARCompounts, delimiter=';')
        isFirst = True
        for row in reader:
            if isFirst:
                isFirst = False
            else:
                listOfEDARCompounds[row[0]] = {
                    "nom": row[2],
                    "cabal_diari": row[3],
                    "dbo5": row[4],
                    "ton": row[5],
                    "top": row[6]
                }
    listOfCompounds = []
    compoundByCountry = {}
    with open(sGUI.selected_config.comp_con_db, encoding='utf8', newline='') as csvCountryCompounts:
        reader = csv.reader(csvCountryCompounts, delimiter=';')
        isFirst = True
        for row in reader:
            if isFirst:
                for cell in range(2, len(row)):
                    listOfCompounds.append(row[cell])
                isFirst = False
            else:
                compound = {}
                for cell in range(2, len(row)):
                    compound[listOfCompounds[cell - 2]] = row[cell]
                compoundByCountry[row[0]] = compound

    listOfUww = {}
    with open(sGUI.selected_config.dp_pop_db, encoding='utf8', newline='') as csvfile:
        isFirst = True
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            if isFirst:
                isFirst = False
            else:
                uww = {
                    "uwwCode": row[0],
                    "uwwDPName": row[3],
                    "uwwDP": row[2],
                    "uwwCoordinate": {"lat": row[4], "lon": row[5]},
                    "uwwCountry": row[7],
                    "uwwPop": row[6],
                    # "compounds": {},
                    "pe": 0,
                    "peC1": 0,
                    "peC2": 0,
                    "peC3": 0,
                    "peC4": 0,
                    "peC5": 0,
                    "C1": 0,
                    "C2": 0,
                    "C3": 0,
                    "C4": 0,
                    "C5": 0
                }
                # if uww['uwwCountry'] in compoundByCountry:
                #     uww['compounds'] = compoundByCountry[uww['uwwCountry']]
                if row[0] in acceptedWWTP:
                    listOfUww[row[0]] = uww
                # print(listOfUww[row[0]])
        query = '''
            SELECT
                   aucAggCode,
                   aggGenerated,
                   aucUwwCode,
                   aggC1 AS c1,
                   aggC2 AS c2,
                   aggPercWithoutTreatment AS c3,
                   aucPercEnteringUWWTP AS c4,
                   aucPercC2T AS c5,
                   aggC1-aucPercEnteringUWWTP AS c6
            FROM T_UWWTPAgglos,
                 T_Agglomerations
            WHERE
                T_Agglomerations.aggCode = T_UWWTPAgglos.aucAggCode
            ORDER BY aucAggCode'''

        con = sqlite3.connect(sGUI.selected_config.eu_db)
        cur = con.cursor()

        # C1 Total aigua canalitzada (Utilitzada per calcular C4 i C6)
        # C2 Fosa septica
        # C3 Directe a medi !!!!! No és diferenciable de C6 !!!!
        # C4 Entrada canalitzada depuradora
        # C5 Entrada transportada depuradora
        # C6 Canalitzada i a medi

        for row in cur.execute(query):
            if row[2] in listOfUww:
                pe = 0
                c1 = 0
                if row[3] is not None:
                    c1 = row[3]
                c2 = 0
                if row[4] is not None:
                    c2 = row[4]
                c3 = 0
                if row[5] is not None:
                    c3 = row[5]
                c4 = 0
                if row[6] is not None:
                    c4 = row[6]
                c5 = 0
                if row[7] is not None:
                    c5 = row[7]
                totalToUww = row[6] + c5

                # Com que no podem diferenciar si és canalizada a medi o directe a medi C1 = C4
                peC1 = (c4 / 100) * row[1]
                listOfUww[row[2]]['peC1'] += peC1

                # Tant de C2 com C3 fem la distribució a depuradora en funció de la quantitat de aigua canalitzada + transportada
                peC2 = (((totalToUww / 100) * c2) / 100) * row[1]
                peC3 = (((totalToUww / 100) * c3) / 100) * row[1]
                listOfUww[row[2]]['peC2'] += peC2
                listOfUww[row[2]]['peC3'] += peC3

                peC4 = (c4 / 100) * row[1]
                peC5 = (c5 / 100) * row[1]
                listOfUww[row[2]]['peC4'] += peC4
                listOfUww[row[2]]['peC5'] += peC5
                # No calculem C6

                # Per tornar a calcular els percentatges hem de sumar tot el que hem evocat al final
                listOfUww[row[2]]['pe'] += peC2 + peC3 + peC4 + peC5

        arrayOfUww = [
            ["NOM", "CODI", "C1", "C2", "C3", "C4", "C5", "PE", "WATER GENERATED", "ACA DATA", "CABAL INDUSTRIAL"]]

        poblacio = []
        cabal = []
        dbo = []
        ton = []
        top = []
        dataToTable = []
        dataCabal = []
        dataDBO = []
        dataTON = []
        dataTOP = []
        dataRename = []

        for uww in listOfUww:
            if listOfUww[uww]['pe'] > 0:
                listOfUww[uww]['C1'] = (listOfUww[uww]['peC1'] / listOfUww[uww]['pe']) * float(listOfUww[uww]['uwwPop'])
                listOfUww[uww]['C2'] = (listOfUww[uww]['peC2'] / listOfUww[uww]['pe']) * float(listOfUww[uww]['uwwPop'])
                listOfUww[uww]['C3'] = (listOfUww[uww]['peC3'] / listOfUww[uww]['pe']) * float(listOfUww[uww]['uwwPop'])
                listOfUww[uww]['C4'] = (listOfUww[uww]['peC4'] / listOfUww[uww]['pe']) * float(listOfUww[uww]['uwwPop'])
                listOfUww[uww]['C5'] = (listOfUww[uww]['peC5'] / listOfUww[uww]['pe']) * float(listOfUww[uww]['uwwPop'])

                cabal_industria = 0
                for industria in industries:
                    if industria[1] == uww:
                        # print(uww, industria, industria[9])
                        if industria[9] is not None:
                            cabal_industria += industria[9]

                poblacio.append(listOfUww[uww]['C4'] + listOfUww[uww]['C5'])
                cabal.append(float(listOfEDARCompounds[uww]['cabal_diari'].replace(',', '.')) - cabal_industria)
                dbo.append(float(listOfEDARCompounds[uww]['dbo5'].replace(',', '.')) * float(
                    listOfEDARCompounds[uww]['cabal_diari'].replace(',', '.')) - cabal_industria)
                top.append(float(listOfEDARCompounds[uww]['top'].replace(',', '.')) * float(
                    listOfEDARCompounds[uww]['cabal_diari'].replace(',', '.')) - cabal_industria)
                ton.append(float(listOfEDARCompounds[uww]['ton'].replace(',', '.')) * float(
                    listOfEDARCompounds[uww]['cabal_diari'].replace(',', '.')) - cabal_industria)

                # listOfUww[uww]['Water2WWTP'] = ((listOfUww[uww]['C4'] + listOfUww[uww]['C5']) * 0.150) + cabal_industria
                listOfUww[uww]['AcaWater2WWTP'] = listOfEDARCompounds[uww]['cabal_diari']
                listOfUww[uww]['AcaDBO'] = listOfEDARCompounds[uww]['dbo5']
                listOfUww[uww]['AcaTOP'] = listOfEDARCompounds[uww]['top']
                listOfUww[uww]['AcaTON'] = listOfEDARCompounds[uww]['ton']
                listOfUww[uww]['nom'] = listOfEDARCompounds[uww]['nom']
            dataCabal.append([
                listOfUww[uww]["uwwDPName"].partition("LA EDAR DE")[2],
                int(round(float(listOfUww[uww]["uwwPop"]), 0)),
                listOfUww[uww]['AcaWater2WWTP'],
                "-"
            ])
            dataDBO.append([
                listOfUww[uww]["uwwDPName"].partition("LA EDAR DE")[2],
                int(round(float(listOfUww[uww]["uwwPop"]), 0)),
                listOfUww[uww]['AcaDBO'],
                "-"
            ])
            dataTOP.append([
                listOfUww[uww]["uwwDPName"].partition("LA EDAR DE")[2],
                int(round(float(listOfUww[uww]["uwwPop"]), 0)),
                listOfUww[uww]['AcaTOP'],
                "-"
            ])
            dataTON.append([
                listOfUww[uww]["uwwDPName"].partition("LA EDAR DE")[2],
                int(round(float(listOfUww[uww]["uwwPop"]), 0)),
                listOfUww[uww]['AcaTON'],
                "-"
            ])

            listOfUww[uww].pop('peC1')
            listOfUww[uww].pop('peC2')
            listOfUww[uww].pop('peC3')
            listOfUww[uww].pop('peC4')
            listOfUww[uww].pop('peC5')

            dataToTable.append([
                uww,
                listOfUww[uww]['uwwDP'],
                listOfUww[uww]["uwwDPName"].partition("LA EDAR DE")[2],
                float(listOfUww[uww]["uwwCoordinate"]["lat"]),
                float(listOfUww[uww]["uwwCoordinate"]["lon"]),
                int(round(float(listOfUww[uww]["uwwPop"]), 0)),
                int(round(float(listOfUww[uww]["C2"]), 0)),
                int(round(float(listOfUww[uww]["C3"]), 0)),
                int(round(float(listOfUww[uww]["C4"]), 0)),
                int(round(float(listOfUww[uww]["C5"]), 0)),
            ])

        data_compute_cabal = pd.DataFrame({'poblacio': poblacio, 'cabal': cabal})
        data_compute_dbo = pd.DataFrame({'poblacio': poblacio, 'dbo': dbo})
        data_compute_top = pd.DataFrame({'poblacio': poblacio, 'top': top})
        data_compute_ton = pd.DataFrame({'poblacio': poblacio, 'ton': ton})

        X_poblacio = data_compute_cabal['poblacio']
        global y_cabal
        y_cabal = data_compute_cabal['cabal']
        global y_dbo
        y_dbo = data_compute_dbo['dbo']
        y_top = data_compute_top['top']
        y_ton = data_compute_ton['ton']

        # X_train, X_test, y_train, y_test = train_test_split(
        #     X_poblacio.values.reshape(-1, 1),
        #     y_cabal.values.reshape(-1, 1),
        #     train_size=1,
        #     random_state=1234,
        #     shuffle=True
        # )
        # print(X_train)

        X_poblacio = sm.add_constant(X_poblacio, prepend=True)
        modelo = sm.OLS(endog=y_cabal, exog=X_poblacio)
        modelo = modelo.fit()
        window["cabal_response"].update(modelo.summary())

        X_poblacio = sm.add_constant(X_poblacio, prepend=True)
        modelo = sm.OLS(endog=y_dbo, exog=X_poblacio)
        modelo = modelo.fit()
        window["dbo_response"].update(modelo.summary())

        X_poblacio = sm.add_constant(X_poblacio, prepend=True)
        modelo = sm.OLS(endog=y_top, exog=X_poblacio)
        modelo = modelo.fit()
        window["top_response"].update(modelo.summary())

        X_poblacio = sm.add_constant(X_poblacio, prepend=True)
        modelo = sm.OLS(endog=y_ton, exog=X_poblacio)
        modelo = modelo.fit()
        window["ton_response"].update(modelo.summary())

        window["dp_table"].update(dataToTable)

        window["cabal_table"].update(dataCabal)
        window["dbo_table"].update(dataDBO)
        window["top_table"].update(dataTOP)
        window["ton_table"].update(dataTON)
        window['dp_total'].update("Number of discharge points: " + str(len(listOfUww)))

        for uww_id in acceptedWWTP:
            if uww_id not in listOfUww:
                print("Somthing not ok with " + uww_id)
            # else:
            # print("Found: "+listOfUww[uww_id])

        print(len(acceptedWWTP))
        print(len(listOfUww))

    # Llegim paràmetres calibrats
    calibrated_parameters = {}
    with open(sGUI.selected_config.comp_removal_rate, encoding='utf8', newline='') as csvfile:
        isFirst = True
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            if isFirst:
                isFirst = False
            else:
                compound_id, name, generation_per_capita, primary, sp, sn, sc, uf, cl, uv, other, sf = row
                calibrated_parameters[compound_id] = {
                    "name": name,
                    "generation_per_capita": float(generation_per_capita.replace(",", ".")),
                    "P": float(primary.replace(",", ".")),
                    "SP": float(sp.replace(",", ".")),
                    "SN": float(sn.replace(",", ".")),
                    "SC": float(sc.replace(",", ".")),
                    "UF": float(uf.replace(",", ".")),
                    "CL": float(cl.replace(",", ".")),
                    "UV": float(uv.replace(",", ".")),
                    "OTHER": float(other.replace(",", ".")),
                    "SF": float(sf.replace(",", ".")),
                }

    # Per cada DP, diu concentracio de cada contaminant a l'efluent
    estimated_concentration_wwtp_effluent = utils.estimate_effluent(calibrated_parameters, listOfUww)

while True:
    win, event, values = PySimpleGUI.read_all_windows()
    # Tanca el programa si es tanca l'app
    # print(win, event, values)
    if event != '__TIMEOUT__':
        print(values)
    if event == 'Settings':
        print("Open Settings Window")
        if sGUI.configWindow is None:
            sGUI.createWindow(window)
    if event == 'Export DP':
        res = PySimpleGUI.popup_get_file("Select where to save data:", save_as=True,
                                         file_types=(('Excel file', '.xlsx'),))
        print(res)
        workbook = xlsxwriter.Workbook(res)
        worksheet = workbook.add_worksheet()
        row = 1
        col = 0
        for item in [
            "EDAR EU_ID",
            "DP ID",
            "Name",
            "Latitude",
            "Longitude",
            "Population",
            "Septic tank (C2)",
            "Direct (C3/C6)",
            "Canalized to WWTP (C4)",
            "Transported to WWTP (C5)"]:
            worksheet.write(0, col, item)
            col += 1

        for excelRow in dataToTable:
            col = 0

            for item in excelRow:
                if str(item).isnumeric():
                    worksheet.write(row, col, float(item))
                else:
                    worksheet.write(row, col, item)
                col += 1
            row += 1

        workbook.close()
    if event == 'cabal_plot':
        plt.figure()
        plt.plot(x_dades, y_results, 'o', label='dades reals ACA')
        plt.plot(x_model, model(x_model, *popt), 'r-', label='model ajustat')
        plt.legend(loc='best')
        plt.xlabel('x')
        plt.ylabel('y')
        plt.yscale('log')
        plt.xscale('log')
        plt.tight_layout()
        plt.show()
    if event == 'config_list_selection':
        sGUI.load_data(sGUI.configWindow['config_list_selection'].get_indexes()[0])
        print("Data loaded")
    if event == 'add_data':
        if values['config_name'] != '':
            print(values)
            config_db = cS(config_db_url)
            config_db.create_config(values['config_name'])
            sGUI.refreshList()
    if event == 'rename_data':
        if values['config_name'] != '' and values['config_list_selection']:
            print(values)
            config_id = sGUI.list_configs[sGUI.configWindow['config_list_selection'].get_indexes()[0]].config_id
            config_db = cS(config_db_url)
            config_db.rename_config(values['config_name'], config_id)
            sGUI.refreshList()
    if event == 'save_data':
        config_db = cS(config_db_url)
        config_id = sGUI.list_configs[sGUI.configWindow['config_list_selection'].get_indexes()[0]].config_id
        config_db.update_config_data(
            config_id,
            values['url'],
            values['db_name'],
            values['user'],
            values['password'],
            values['url_eu_db'],
            values['dp_pop_db'],
            values['wwtp_con_db'],
            values['comp_con_db']
        )
        sGUI.refreshList()
        print("Saving data!")
    if event == 'test_db':
        pg_test = pg()
        if sGUI.configWindow is not None:
            sGUI.configWindow['test_db_response'].update('Testing...')
            sGUI.configWindow.refresh()
            print(values)
            valid, reason = pg_test.testConnection(
                host=values['url'],
                database=values['db_name'],
                user=values['user'],
                password=values['password']
            )
            if valid is True:
                sGUI.configWindow['test_db_response'].update('Successfull: DB Version ' + str(reason))
            else:
                sGUI.configWindow['test_db_response'].update(reason)
    if event == 'Change Recall Name SWAT+ Editor':  #Obrir finestra per canviar noms de .sqlite
        if rGUI.renameWindow is None:
            rGUI.createWindow(window, calibrated_parameters)

        #rGUI.renameWindow["rename_table"].update(dataRename)

    if event == 'wwt_file_name':  #Omplir taula amb WWTP de base de dades i fitxer .sqlite
        renameHelper = rS(values["wwt_file_name"])
        try:
            float(values["input_rename_threshold"])
            dataRename = renameHelper.populate_table("recall_con", dataToTable, float(values["input_rename_threshold"]))
            rGUI.renameWindow["rename_table"].update(dataRename)

        except Exception as e:
            print(e)
    if event == 'run_rename':  #Canviar dades a fitxer .sqlite
        try:
            if len(dataRename) == 0: #No ha penjat cap arxiu encara
                raise Exception('Upload any .SQLite file first') from e

            if PySimpleGUI.popup_yes_no('This action will overwrite the uploaded file.\nDo you want to continue?') == "Yes":
                renameHelper = rS(values["wwt_file_name"])
                new_data_table = renameHelper.rename_db(dataRename)
                rGUI.renameWindow["rename_table"].update(new_data_table)
                rGUI.renameWindow["calibration_tab"].update(disabled=False)
                PySimpleGUI.popup("WWTP's renamed successfully!")

        except Exception as e:
            print(str(e))

    if event == 'run_estimate_effluent':
        try:
            renameHelper.add_data_to_swat(estimated_concentration_wwtp_effluent)
            PySimpleGUI.popup("Effluent data estimated successfully!!")
        except:
            pass


    if event == PySimpleGUI.WIN_CLOSED:
        win.close()
        if win == window:
            break
        elif win == sGUI.configWindow:
            sGUI.configWindow = None
        elif win == rGUI.renameWindow:
            rGUI.renameWindow = None
window.close()
