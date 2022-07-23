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

                """
                if terciaris == ['UF'] or (collections.Counter(terciaris) == collections.Counter(["UF", "CL"])):
                    escenaris = [
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["UF,RO,AOP"],
                            'wwtp': edar
                        },

                        {
                            'secundari': secundari_aux,
                            'terciaris': ["UF,UV"],
                            'wwtp': edar

                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': terciaris,
                            'wwtp': edar
                        },


                    ]
                elif terciaris == ['CL'] or terciaris is None:
                    escenaris = [

                        {
                            'secundari': secundari_aux,
                            'terciaris': ["SF"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["O3,SF"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["SF,UV"],
                            'wwtp': edar
                        },

                        {
                            'secundari': secundari_aux,
                            'terciaris': ["O3,SF,UV"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["GAC"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["O3,GAC,UV"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["UF,RO,AOP"],
                            'wwtp': edar
                        },

                        {
                            'secundari': secundari_aux,
                            'terciaris': ["UF,UV"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': terciaris,
                            'wwtp': edar
                        },
                    ]
                elif collections.Counter(terciaris) == collections.Counter(["UV", "CL"]):
                    escenaris = [
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["SF,UV"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["O3,SF,UV"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["O3,GAC,UV"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["UF,UV"],
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
                        }
                    ]
                """
                escenaris_wwtp.extend(escenaris)


        """
        escenaris = {
            'secundari': secundari,
            'terciaris': terciaris,
            'wwtp': edar
        }
        escenaris_total.append([escenaris])
        """

        escenaris_total.append(escenaris_wwtp)

    return list(itertools.product(*escenaris_total)), preu_inicial

def run_scenarios(connection, industrial_data, recall_points, contaminants_i_nutrients, edar_data_xlsx, removal_rate, industries_to_edar, industries_to_river, edars_escenaris, edars_calibrated_init):


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

    #coords_to_pixel = g.closest_pixel(map(lambda x: [x['lat'], x['lon']], masses_aigua.values()), renameHelper)

    llindars_massa_aigua = pandas.read_excel("inputs/llindars_massa_aigua.xlsx", index_col=0)

    escenaris_resultat = []

    i = 0
    for scenario in scenarios:

        masses_aigua_valors = {}
        edars_aux = edars.copy()
        for edar in scenario:
            edars_aux.at[edar['wwtp'], 'secundari'] = edar['secundari']
            if 'terciaris' in edar and edar['terciaris'] is not None:
                edars_aux.at[edar['wwtp'], 'terciari'] = ','.join(edar['terciaris'])

        edars_aux = edars_aux.combine_first(edars_cic)

        edars_aux.to_excel("inputs/excel_scenario.xlsx")

        edars_calibrated = read_edars(contaminants_i_nutrients, industries_to_edar, "inputs/excel_scenario.xlsx", removal_rate, recall_points)

        df_pixels = renameHelper.add_data_edar_to_graph(edars_calibrated, contaminants_i_nutrients, pixel_to_poll.copy())
        #df_pixels = pandas.read_csv("trimetoprim_original.csv", index_col=0)
        graph = g.run_graph(df_pixels)

        for coord in masses_aigua:
            pixel = coords_to_pixel[coord]
            massa_aigua = masses_aigua[coord]['codi_ma']
            if massa_aigua not in masses_aigua_valors:
                masses_aigua_valors[massa_aigua] = {}
            for contaminant in contaminants_i_nutrients:
                if contaminant not in masses_aigua_valors[massa_aigua]:
                    masses_aigua_valors[massa_aigua][contaminant] = []
                #print(graph.nodes[pixel])
                conc = graph.nodes[pixel][contaminant] / graph.nodes[pixel]['flow_HR']
                masses_aigua_valors[massa_aigua][contaminant].append(conc)

        escenaris_resultat.append((scenario, masses_aigua_valors))

        if i == 2:
            break
        i += 1

    edars_cabal = {}
    for edar in edars_calibrated_init:
        edars_cabal[edar] = edars_calibrated_init[edar]["compounds_effluent"]["q"]


    for (simulacio, masses_aigua) in escenaris_resultat:

        incompliments = {}
        """       
        for contaminant in contaminants_i_nutrients:
            incompliments[contaminant] = 0
        """
        for massa_aigua in masses_aigua:
            if massa_aigua not in incompliments:
                incompliments[massa_aigua] = 0
            for contaminant in contaminants_i_nutrients:
                avg_massa_aigua = sum(masses_aigua[massa_aigua][contaminant]) / len(masses_aigua[massa_aigua][contaminant])

                if avg_massa_aigua > llindars_massa_aigua.at[contaminant, massa_aigua]:
                    incompliments[massa_aigua] += 1

        cost_final = 0
        for wwtp in simulacio:
            cost_final += calculate_price(wwtp['terciaris'], edars_cabal[wwtp['wwtp']])

        print(incompliments, cost_final - cost_inicial)

    print(escenaris_resultat[0][1][1000870]["Ciprofloxacina"])





