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

def all_scenarios(edars_escenaris):

    configuracions_edars = pd.read_excel(edars_escenaris, index_col=0).to_dict(
        orient='index')
    escenaris_total = []

    for edar in configuracions_edars:
        secundari = None
        terciaris = None
        escenaris_wwtp = []

        if 'secundari' in configuracions_edars[edar]:
            secundari = configuracions_edars[edar]["secundari"]
        if 'terciari' in configuracions_edars[edar] and isinstance(configuracions_edars[edar]["terciari"], str):
            terciaris = configuracions_edars[edar]["terciari"].replace(" ", "").split(",")

        if secundari is not None:
            if secundari == "SP" or secundari == "SN":
                secundari = [secundari]
            else:
                secundari = ["SC", "SP", "SN"]

            for secundari_aux in secundari:

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

                escenaris_wwtp.extend(escenaris)

        escenaris_total.append(escenaris_wwtp)

    return list(itertools.product(*escenaris_total))

def run_scenarios(connection, industrial_data, recall_points, contaminants_i_nutrients, edar_data_xlsx, removal_rate, industries_to_edar, industries_to_river, edars_escenaris):


    scenarios = all_scenarios(edars_escenaris)

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
    coords_to_pixel = g.closest_pixel(map(lambda x: [x['lat'], x['lon']], masses_aigua.values()), renameHelper)
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

        if i == 3:
            break
        i += 1

    for (simulacio, masses_aigua) in escenaris_resultat:

        incompliments = {}
        """       
        for contaminant in contaminants_i_nutrients:
            incompliments[contaminant] = 0
        """
        for massa_aigua in masses_aigua:
            if massa_aigua not in incompliments:
                incompliments[massa_aigua] = []
            for contaminant in contaminants_i_nutrients:
                avg_massa_aigua = sum(masses_aigua[massa_aigua][contaminant]) / len(masses_aigua[massa_aigua][contaminant])
                #print(avg_massa_aigua)
                #print(massa_aigua, '--', contaminant, '--', avg_massa_aigua)
                #masses_aigua[massa_aigua][contaminant] = avg_massa_aigua

                if avg_massa_aigua > llindars_massa_aigua.at[contaminant, massa_aigua]:
                    incompliments[massa_aigua].append([contaminant, avg_massa_aigua])



        print(incompliments)





