import csv
import sqlite3
import json
import tkinter as tk
from tkinter import ttk
import xlsxwriter
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

from classes.ConnectDb import ConnectDb
from classes.GUI import GUI



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    connection = ConnectDb()

    wwtps = connection.getAllWWTP()
    # unitats mg/l
    # cabal m3/dia
    industries = connection.getIndustriesToEdar()
    acceptedWWTP = []
    for wwtp in wwtps:
        acceptedWWTP.append(wwtp[0])
        # //print(wwtp)

    listOfCompounds = []
    compoundByCountry = {}
    # Fields of edar compounts
    # cabal_diari m3/dia
    # unidades mg/l
    listOfEDARCompounds = {}
    listOfUww = {}
    with open('./inputs/EDAR_DBO_TN_PT_media2.csv', encoding='utf8', newline='') as csvEDARCompounts:
        reader = csv.reader(csvEDARCompounts, delimiter=';')
        isFirst = True
        for row in reader:
            if isFirst:
                isFirst = False
            else:
                print()
                listOfEDARCompounds[row[0]] = {
                    "nom": row[2],
                    "cabal_diari": row[3],
                    "dbo5": row[4],
                    "ton": row[5],
                    "top": row[6]
                }
    with open('./inputs/compounts_contry.csv', encoding='utf8', newline='') as csvCountryCompounts:
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
                    compound[listOfCompounds[cell-2]] = row[cell]
                compoundByCountry[row[0]] = compound

    with open('./inputs/dp_population_country.csv', encoding='utf8', newline='') as csvfile:
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

        con = sqlite3.connect('./inputs/Waterbase_UWWTD_v8.accdb.sqlite')
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

        arrayOfUww = [["NOM", "CODI", "C1", "C2", "C3", "C4", "C5", "PE", "WATER GENERATED", "ACA DATA", "CABAL INDUSTRIAL"]]

        poblacio = []
        cabal = []

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
                cabal.append(float(listOfEDARCompounds[uww]['cabal_diari'].replace(',', '.'))-cabal_industria)

                # listOfUww[uww]['Water2WWTP'] = ((listOfUww[uww]['C4'] + listOfUww[uww]['C5']) * 0.150) + cabal_industria
                listOfUww[uww]['AcaWater2WWTP'] = listOfEDARCompounds[uww]['cabal_diari']
                listOfUww[uww]['nom'] = listOfEDARCompounds[uww]['nom']

            listOfUww[uww].pop('peC1')
            listOfUww[uww].pop('peC2')
            listOfUww[uww].pop('peC3')
            listOfUww[uww].pop('peC4')
            listOfUww[uww].pop('peC5')

    dades = pd.DataFrame({'poblacio': poblacio, 'cabal': cabal})
    # print(dades[['cabal']])
    # print(dades[['poblacio']])
    # print(dades.head(3))

    X = dades[['poblacio']]
    y = dades['cabal']

    # X_train, X_test, y_train, y_test = train_test_split(
    #     X.values.reshape(-1, 1),
    #     y.values.reshape(-1, 1),
    #     train_size=1,
    #     random_state=1234,
    #     shuffle=True
    # )
    # print(X_train)

    # X = sm.add_constant(X, prepend=True)
    # modelo = sm.OLS(endog=y, exog=X, )
    # modelo = modelo.fit()
    # print(modelo.summary())
    # print(modelo.params['poblacio'])

    # CABAL CALCULAT Vicens 0.138
    for uww in listOfUww:
        if listOfUww[uww]['pe'] > 0:
            cabal_industria = 0
            kilosIndustria = 0
            for industria in industries:
                if industria[1] == uww:
                    if industria[9] is not None:
                        cabal_industria += industria[9]
                    if industria[3] is not None and industria[9] is not None:
                        kilosIndustria += industria[9] * industria[3]

            listOfUww[uww]['Water2WWTP'] = ((listOfUww[uww]['C4'] + listOfUww[uww]['C5']) * 0.138) + cabal_industria
            arrayOfUww.append([
                listOfUww[uww]['nom'],
                uww,
                listOfUww[uww]['C1'],
                listOfUww[uww]['C2'],
                listOfUww[uww]['C3'],
                listOfUww[uww]['C4'],
                listOfUww[uww]['C5'],
                listOfUww[uww]['pe'],
                listOfUww[uww]['Water2WWTP'],
                listOfUww[uww]['AcaWater2WWTP'],
                cabal_industria,
                kilosIndustria,
                float(listOfEDARCompounds[uww]['dbo5'].replace(",", "."))
            ])


    workbook = xlsxwriter.Workbook('generation100.xlsx')
    worksheet = workbook.add_worksheet()
    row = 0

    for excelRow in arrayOfUww:
        col = 0
        for item in excelRow:
            worksheet.write(row, col, item)
            col += 1
        row += 1

    workbook.close()

    # model
    # persones (x)
    # Q per persona (a)
    # QInd Cabal industria
    # CInd concentració industria
    # CEdar conectració edar (b)
    # CKilos edar (y)
    # Ckilos / (Q*Persones) = Sum(QInd * CInd) + CEdar -> y = Sum(QInd * CInd) * (Q*x) + CEdar * (Q*x)
    def model(x, b):
        return b * x #Model sense tenir en compte industries (Calibració molt complicada)

    x_dades = []
    y_results = []
    countNoIndustry = 0
    edarName = ""
    biggerX = 0
    poblacioTotal = 0
    for uww in listOfUww:
        compoundEdar = float(listOfEDARCompounds[uww]['ton'].replace(",", "."))
        caudalEdar = float(listOfEDARCompounds[uww]['cabal_diari'].replace(",", "."))

        poblacioTotal += listOfUww[uww]['C4']
        kilosIndustria = 0
        for industria in industries:
            if industria[1] == uww:
                if industria[9] is not None and industria[3] is not None:
                    kilosIndustria += (industria[9])*industria[3]
                    # print("--- Industria {:5.3f}", (industria[9])*industria[3])



        # print("Total Industries EDAR = {:5.3f}".format(kilosIndustria))
        # print("EDAR L_DBO: {:5.3f} N_DBOcc: {:5.3f}".format((compoundEdar * caudalEdar), listOfUww[uww]['C4']*0.6))
        # print("EDAR Y:", (compoundEdar * caudalEdar) - listOfUww[uww]['C4']*0.6)
        # print("EDAR x:", kilosIndustria)
        # print("Edar: {:s}  Observat edar (mg): {:5.3f}  Observat industria(mg): {:5.3f}".format(listOfEDARCompounds[uww]['nom'], compoundEdar * caudalEdar, kilosIndustria))
        print("EDAR Y:", (compoundEdar * caudalEdar) - (listOfUww[uww]['C4'] * 55.503))

        if kilosIndustria == 0 :
            # if (compoundEdar * caudalEdar)*1.01 < kilosIndustria + (listOfUww[uww]['C4']*55.503):
            if ((compoundEdar * caudalEdar) - (listOfUww[uww]['C4'] * 55.503)) < 0:
                countNoIndustry += 1
            print("Edar: {:s}  Compound (mg): {:5.3f}  cabal(m3): {:5.3f}".format(listOfEDARCompounds[uww]['nom'],compoundEdar, caudalEdar))
            print("EDAR Y:", (compoundEdar * caudalEdar) - listOfUww[uww]['C4'] * 55.503)
            # print(((compoundEdar * caudalEdar) - (listOfUww[uww]['C4']*55.503)) / kilosIndustria )
            y_results.append((compoundEdar * caudalEdar))
            if biggerX < listOfUww[uww]['C4']:
                biggerX = listOfUww[uww]['C4']
                edarName = listOfEDARCompounds[uww]['nom']
            x_dades.append(listOfUww[uww]['C4'])
    # Parametres inicials
    # Calcul fet per DBOcc 55.503 ± 5.484
    # Q per capita 0.138

    parametres_inicials = [0.6]
    print(poblacioTotal)
    print(edarName, biggerX)
    # Ajustem amb curve_fit
    popt, pcov = curve_fit(model, x_dades, y_results, p0=parametres_inicials)

    # Dades de depuredores, analisi estadistic, coeficient de variació

    x_model = np.linspace(0, 1500000, 10)

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

    print(popt)
    # De la matris de covarinza podemos obtener los valores de desviacion estandar
    # de los parametros hallados
    pstd = np.sqrt(np.diag(pcov))

    nombres_de_param = ['b']
    print('Paràmetres trobats:')
    for i, param in enumerate(popt):
        print('{:s} = {:5.3f} ± {:5.3f}'.format(nombres_de_param[i], param, pstd[i] / 2))

    # connection.calibrateContaminant()
    # a_file = open("export.json", "w")
    # json.dump(listOfUww, a_file)
    # a_file.close()
    print('Done')

    # app = GUI()
    # app.mainloop()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
