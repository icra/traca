import pandas
from lib.calibrationMainConcentration import read_industries, read_edars, exportDataForNils, wwtp_info
from lib.db.renameSQLite import renameSQLite as rS
import csv
import sys
import json
import collections
import itertools
import lib.graph.Simulation as Simulation
import pandas as pd
import time
import random


def highlighting_mean_greater(s):
    """
    highlighting yello value is greater than mean else red
    """
    is_max = s>s.mean()
    return ['background-color:yellow' if i else 'background-color:red'for i in is_max]

def calculate_price(terciaris, cabal):

    cost = 0
    if terciaris is None:
        return cost
    for terciari in terciaris:

        cabal_aux = cabal
        if cabal > 100000:
            cabal_aux = 100000

        if terciari == 'SF':
            cost += 0.218 * pow(cabal_aux, -0.103) * cabal_aux * 365
        elif terciari == 'O3':
            cost += 3.678 * pow(cabal_aux, -0.339) * cabal_aux * 365
        elif terciari == 'GAC':
            cost += 6.98 * pow(cabal_aux, -0.406) * cabal_aux * 365
        elif terciari == 'UV':
            cost += 0.122 * pow(cabal_aux, -0.213) * cabal_aux * 365
        elif terciari == 'UF':
            cost += 8.384 * pow(cabal_aux, -0.367) * cabal_aux * 365
        elif terciari == 'RO':
            cost += 0.368 * pow(cabal_aux, -0.0897) * cabal_aux * 365
        elif terciari == 'AOP':
            cost += 0.61 * pow(cabal_aux, -0.383) * cabal_aux * 365
    return cost

def all_scenarios(edars_escenaris, edars_calibrated_init):

    configuracions_edars = pd.read_excel(edars_escenaris, index_col=0).to_dict(
        orient='index')
    escenaris_total = []


    edars_cabal = {}
    for edar in edars_calibrated_init:
        edars_cabal[edar] = edars_calibrated_init[edar]["compounds_effluent"]["q"]

    preu_inicial = 0

    for edar in configuracions_edars:
        secundari = None
        terciaris = None
        escenaris_wwtp = []

        if 'secundari' in configuracions_edars[edar]:
            secundari = configuracions_edars[edar]["secundari"]
        if 'terciari' in configuracions_edars[edar] and isinstance(configuracions_edars[edar]["terciari"], str):
            terciaris = configuracions_edars[edar]["terciari"].replace(" ", "").split(",")

        cabal = edars_cabal[edar]
        preu_inicial += calculate_price(terciaris, cabal)

        """
        if secundari is not None:
            if secundari == "SP" or secundari == "SN":
                secundari = [secundari]
            else:
                secundari = ["SC", "SP", "SN"]

            for secundari_aux in secundari:
                if terciaris is None:
                    if cabal < 8000:
                        escenaris = [
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["SF","UV"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': terciaris,
                                'wwtp': edar
                            },
                        ]
                    elif cabal < 20000:
                        escenaris = [

                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3","SF"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["SF","UV"],
                                'wwtp': edar
                            },

                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3","SF","UV"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["GAC"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': terciaris,
                                'wwtp': edar
                            },
                        ]
                    else:
                        escenaris = [
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["SF","UV"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3","SF"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3","SF","UV"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["GAC"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3","GAC","UV"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["UF","RO","AOP"],
                                'wwtp': edar
                            },

                            {
                                'secundari': secundari_aux,
                                'terciaris': ["UF","UV"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': terciaris,
                                'wwtp': edar
                            },
                        ]
                else:
                    escenaris = [
                        {
                            'secundari': secundari_aux,
                            'terciaris': terciaris,
                            'wwtp': edar
                        },
                    ]

                escenaris_wwtp.extend(escenaris)
        escenaris_total.append(escenaris_wwtp)
        """

        escenaris = {
            'secundari': secundari,
            'terciaris': terciaris,
            'wwtp': edar
        }
        escenaris_total.append([escenaris])

    return list(itertools.product(*escenaris_total)), preu_inicial

def run_scenarios(connection, industrial_data, recall_points, contaminants_i_nutrients, edar_data_xlsx, removal_rate, industries_to_edar, industries_to_river, edars_escenaris, edars_calibrated_init):

    contaminants_i_nutrients = ["Ciprofloxacina", "Clorobenzè", "Hexabromociclodecà", "Nonilfenols", "Octilfenols", "Tetracloroetilè", "Triclorometà", "Cloroalcans", "Niquel dissolt", "Plom dissolt", "Diuron"]

    scenarios, cost_inicial = all_scenarios(edars_escenaris, edars_calibrated_init)

    random.shuffle(scenarios)

    edars = pd.read_excel(edars_escenaris, index_col=0)
    edars_cic = pd.read_excel(edar_data_xlsx, index_col=0)

    renameHelper = rS(None)
    id_discharge_to_volumes = read_industries(industries_to_river, industrial_data, recall_points,
                                              contaminants_i_nutrients, connection)

    pixel_to_poll = renameHelper.add_data_industry_to_graph(id_discharge_to_volumes, contaminants_i_nutrients)

    g = Simulation.Simulation()

    masses_aigua = pandas.read_csv("inputs/coord_codi_llob.csv")
    masses_aigua["lat_lon"] = masses_aigua["lat"].map(str) + " " + masses_aigua["lon"].map(str)
    masses_aigua = masses_aigua.set_index('lat_lon').to_dict(orient = 'index')


    coords_to_pixel = {}

    for coord in masses_aigua.values():
        pixel = g.give_pixel([coord["lat"], coord["lon"]], "inputs/reference_raster.tif", True, False)
        coords_to_pixel[str(coord["lat"])+" "+str(coord["lon"]) ] = pixel


    #Saber quin node del graf es correspon punts aigues amunt baix llobregat
    """
    print(g.give_pixel([41.56458, 1.64404], "inputs/reference_raster.tif", True, False))
    print(g.give_pixel([41.70632, 1.83961], "inputs/reference_raster.tif", True, False))
    print(g.give_pixel([41.68489, 1.85208], "inputs/reference_raster.tif", True, False))
    """
    #coords_to_pixel = g.closest_pixel(map(lambda x: [x['lat'], x['lon']], masses_aigua.values()), renameHelper)

    llindars_massa_aigua = pandas.read_excel("inputs/llindars_massa_aigua.xlsx", index_col=0)


    i = 0

    with open("resultat.json", 'w', encoding="utf-8") as json_file:
        json.dump([], json_file, ensure_ascii=False,
                  indent=4,
                  separators=(',', ': '))

    edars_cabal = {}
    for edar in edars_calibrated_init:
        edars_cabal[edar] = edars_calibrated_init[edar]["compounds_effluent"]["q"]


    for scenario in scenarios:

        cost_final = 0
        edars_aux = edars.copy()
        for edar in scenario:
            edars_aux.at[edar['wwtp'], 'secundari'] = edar['secundari']
            cost_final += calculate_price(edar['terciaris'], edars_cabal[edar['wwtp']])

            if 'terciaris' in edar and edar['terciaris'] is not None:
                edars_aux.at[edar['wwtp'], 'terciari'] = ','.join(edar['terciaris'])

        edars_aux = edars_aux.combine_first(edars_cic)
        edars_aux.to_excel("inputs/excel_scenario.xlsx")
        edars_calibrated = read_edars(contaminants_i_nutrients, industries_to_edar, "inputs/excel_scenario.xlsx", removal_rate, recall_points)

        df_pixels = renameHelper.add_data_edar_to_graph(edars_calibrated, contaminants_i_nutrients, pixel_to_poll.copy())
        graph = g.run_graph(df_pixels)


        massa_aigua_mitjanes = {}
        masses_aigua_valors = {}
        masses_aigua_incompliments = {}
        for contaminant in contaminants_i_nutrients:
            #Càrrega i concentracio nodes aigues amunt baix llobrehat
            """
            print(contaminant,  round(24 * graph.nodes[117463114][contaminant] / 1000000000, 5) , round(graph.nodes[117463114][contaminant] / (1000*graph.nodes[117463114]['flow_HR']), 5))
            print(contaminant, round(24 * graph.nodes[116965401][contaminant] / 1000000000, 5) , round(graph.nodes[116965401][contaminant] / (1000*graph.nodes[116965401]['flow_HR']), 5))
            print(contaminant, round(24 * graph.nodes[117038604][contaminant] / 1000000000, 5) , round(graph.nodes[117038604][contaminant] / (1000*graph.nodes[117038604]['flow_HR']), 5))
            print('------------------')
            """
            for coord in masses_aigua:
                pixel = coords_to_pixel[coord]
                massa_aigua = masses_aigua[coord]['codi_ma']
                if massa_aigua not in masses_aigua_valors:
                    masses_aigua_valors[massa_aigua] = {}
                if contaminant not in masses_aigua_valors[massa_aigua]:
                    masses_aigua_valors[massa_aigua][contaminant] = []

                #print(graph.nodes[pixel])
                conc = graph.nodes[pixel][contaminant] / graph.nodes[pixel]['flow_HR']

                masses_aigua_valors[massa_aigua][contaminant].append(conc)

            for massa_aigua in masses_aigua_valors:

                if massa_aigua not in masses_aigua_incompliments:
                    masses_aigua_incompliments[massa_aigua] = []
                if massa_aigua not in massa_aigua_mitjanes:
                    massa_aigua_mitjanes[massa_aigua] = {}

                mitjana_contaminant = sum(masses_aigua_valors[massa_aigua][contaminant]) / len(masses_aigua_valors[massa_aigua][contaminant])

                if contaminant == "Ciprofloxacina":
                    mitjana_contaminant = mitjana_contaminant / 1.76
                elif contaminant == "Hexabromociclodecà":
                    mitjana_contaminant = mitjana_contaminant / 11.18
                elif contaminant == "Nonilfenols":
                    mitjana_contaminant = mitjana_contaminant / 0.016

                massa_aigua_mitjanes[massa_aigua][contaminant] = mitjana_contaminant

                if mitjana_contaminant > llindars_massa_aigua.at[contaminant, massa_aigua]:
                    masses_aigua_incompliments[massa_aigua].append(contaminant)

        print(scenario)
        print(masses_aigua_incompliments)
        pandas.DataFrame(massa_aigua_mitjanes).to_excel("/home/zephol/Desktop/traca_csv/s0.xlsx")

        def highlight(x):
            red = f"background-color:red"
            green = f"background-color:green"
            df1 = pd.DataFrame('', index=x.index, columns=x.columns)
            for row in df1.index.values:
                for col in df1.columns:
                    if col in masses_aigua_incompliments and row in masses_aigua_incompliments[col]:
                        df1.loc[row, col] = red
                    else:
                        df1.loc[row, col] = green
            return df1

        pandas.DataFrame(massa_aigua_mitjanes).style.apply(highlight, axis=None).to_excel("/home/zephol/Desktop/traca_csv/sc0.xlsx", engine='openpyxl')


        # Read JSON file
        with open("resultat.json", "rb") as fp:
            listObj = json.load(fp)

        listObj.append({
            "scenario": scenario,
            "masses_aigua_valors": masses_aigua_incompliments,
            "cost": cost_final - cost_inicial
        })
        
        with open("resultat.json", 'w', encoding="utf-8") as json_file:
            json.dump(listObj, json_file, ensure_ascii=False,
                      indent=4,
                      separators=(',', ': '))

        i += 1
        print(i)



    listObj = []
    # Read JSON file
    with open("resultat.json", "rb") as fp:
        listObj = json.load(fp)

    for obj in listObj:
        scenario = obj['scenario']
        masses_aigua = obj['masses_aigua_valors']
        cost = obj['cost']

        incompliment_per_contaminant = {}

        for massa_aigua in masses_aigua:
            for contaminant in masses_aigua[massa_aigua]:
                if contaminant not in incompliment_per_contaminant:
                    incompliment_per_contaminant[contaminant] = 0
                incompliment_per_contaminant[contaminant] += 1

        print(incompliment_per_contaminant)

