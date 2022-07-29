import pandas
from lib.calibrationMainConcentration import read_industries, read_edars, exportDataForNils, wwtp_info
from lib.db.renameSQLite import renameSQLite as rS
import json
import itertools
import lib.graph.Simulation as Simulation
import pandas as pd
import random
import csv
import time
from dask import dataframe as dd

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

    configuracions_edars = edars_escenaris.to_dict(
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
                                'terciaris': ["SF","UV"],
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
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3", "SF"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3", "SF", "UV"],
                                'wwtp': edar
                            }
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
                                'terciaris': ["GAC"],
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
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3", "GAC", "UV"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3", "SF", "UV"],
                                'wwtp': edar
                            },
                            {
                                'secundari': secundari_aux,
                                'terciaris': ["O3", "SF"],
                                'wwtp': edar
                            }
                        ]
                        if edar in ['ES9081130006010E', 'ES9081270001010E', 'ES9080010001010E', 'ES9081140002010E', 'ES9082110001010E']:
                            escenaris.append({
                                'secundari': secundari_aux,
                                'terciaris': ["UF","RO","AOP"],
                                'wwtp': edar
                            })

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
        """
    return list(itertools.product(*escenaris_total)), preu_inicial

def run_scenarios(connection, industrial_data, recall_points, contaminants_i_nutrients, edar_data_xlsx, removal_rate, industries_to_edar, industries_to_river, edars_calibrated_init, file_name, n_iteracions, window, graph_location, river_attenuation, excel_scenario, coord_to_pixel, coord_to_codi, llindars, resultat_escenaris, abocaments_ci, id_pixel):


    contaminants_i_nutrients = ["Ciprofloxacina", "Clorobenzè", "Hexabromociclodecà", "Nonilfenols", "Octilfenols", "Tetracloroetilè", "Triclorometà", "Cloroalcans", "Niquel dissolt", "Plom dissolt", "Diuron"]


    edars_cic = pd.read_excel(edar_data_xlsx, index_col=0)
    edars = edars_cic.loc[
        ["ES9080010001010E",
         "ES9080910001010E",
         "ES9083020001010E",
         "ES9081130006010E",
         "ES9081140002010E",
         "ES9081270001010E",
         "ES9081840001010E",
         "ES9082110001010E",
         "ES9082790004050E"
         ]]

    scenarios, cost_inicial = all_scenarios(edars, edars_calibrated_init)
    random.shuffle(scenarios)

    print(len(scenarios))



    renameHelper = rS(None)
    id_discharge_to_volumes = read_industries(industries_to_river, industrial_data, recall_points,
                                              contaminants_i_nutrients, connection)

    pixel_to_poll = renameHelper.add_data_industry_to_graph(recall_points, id_discharge_to_volumes, contaminants_i_nutrients, abocaments_ci, id_pixel)

    g = Simulation.Simulation(graph_location, river_attenuation)

    masses_aigua = pandas.read_csv(coord_to_codi)
    masses_aigua["lat_lon"] = masses_aigua["lat"].map(str) + " " + masses_aigua["lon"].map(str)
    masses_aigua = masses_aigua.set_index('lat_lon').to_dict(orient = 'index')


    """  
    coords_to_pixel = {}
    for coord in masses_aigua.values():
        pixel = g.give_pixel([coord["lat"], coord["lon"]], "inputs/reference_raster.tif", True, False)
        coords_to_pixel[str(coord["lat"])+" "+str(coord["lon"]) ] = pixel
    """

    coords_to_pixel = pandas.read_csv(coord_to_pixel)
    coords_to_pixel["lat_long"] = coords_to_pixel["lat"].astype(str)+" "+coords_to_pixel["long"].astype(str)
    coords_to_pixel = dict(coords_to_pixel.drop(columns=["lat", "long"]).reindex(columns=['lat_long','pixel']).values)


    llindars_massa_aigua = pandas.read_excel(llindars, index_col=0)


    with open("resultats.csv", 'w', newline='', encoding="utf-8") as write_obj:
        # Create a writer object from csv module
        csv_writer = csv.writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(['iteracio', 'escenari'])

    edars_cabal = {}
    for edar in edars_calibrated_init:
        edars_cabal[edar] = edars_calibrated_init[edar]["compounds_effluent"]["q"]

    current_iteration = 0


    for scenario in scenarios:

        if current_iteration == n_iteracions:
            break

        cost_final = 0
        edars_aux = edars.copy()
        for edar in scenario:
            edars_aux.at[edar['wwtp'], 'secundari'] = edar['secundari']
            cost_final += calculate_price(edar['terciaris'], edars_cabal[edar['wwtp']])

            if 'terciaris' in edar and edar['terciaris'] is not None:
                edars_aux.at[edar['wwtp'], 'terciari'] = ','.join(edar['terciaris'])

        edars_aux = edars_aux.combine_first(edars_cic)
        edars_aux.to_excel(excel_scenario)
        edars_calibrated = read_edars(contaminants_i_nutrients, industries_to_edar, excel_scenario, removal_rate, recall_points)

        df_pixels = renameHelper.add_data_edar_to_graph(recall_points, edars_calibrated, contaminants_i_nutrients, pixel_to_poll.copy(), abocaments_ci)
        graph = g.run_graph(df_pixels)


        masses_aigua_valors = {}
        masses_aigua_incompliments = {}
        for contaminant in contaminants_i_nutrients:

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

                mitjana_contaminant = sum(masses_aigua_valors[massa_aigua][contaminant]) / len(masses_aigua_valors[massa_aigua][contaminant])

                if contaminant == "Ciprofloxacina":
                    mitjana_contaminant = mitjana_contaminant / 1.76
                elif contaminant == "Hexabromociclodecà":
                    mitjana_contaminant = mitjana_contaminant / 11.18
                elif contaminant == "Nonilfenols":
                    mitjana_contaminant = mitjana_contaminant / 0.016
                if mitjana_contaminant > llindars_massa_aigua.at[contaminant, massa_aigua]:
                    masses_aigua_incompliments[massa_aigua].append(contaminant)

        with open("resultats.csv", 'a', newline='', encoding="utf-8") as write_obj:
            # Create a writer object from csv module
            csv_writer = csv.writer(write_obj)
            # Add contents of list as last row in the csv file
            csv_writer.writerow([current_iteration, {
            "scenario": scenario,
            "masses_aigua_valors": masses_aigua_incompliments,
            "cost": cost_final - cost_inicial
        }])


        current_iteration += 1
        print(current_iteration)
        window['progress_1'].update(current_iteration)


    listObj = []
    #df = dd.read_csv('resultats.csv').persist()
    df = pandas.read_csv('resultats.csv')
    for string in list(df['escenari']):
        listObj.append(eval(string))

    def f(x):
        incompliments = 0
        for massa in x["masses_aigua_valors"]:
            incompliments += len(x["masses_aigua_valors"][massa])
        return incompliments, x["cost"]

    data_sorted = sorted(listObj, key=lambda x: f(x))

    configuracions = []
    incompliments_masses = []
    incompliment_contaminant = []

    for obj in data_sorted:
        scenario = obj['scenario']
        masses_aigua = obj['masses_aigua_valors']
        cost = obj['cost']

        obj = {}
        for edar in scenario:
            tractaments = "P,"+edar["secundari"]
            if edar["terciaris"] is not None:
                tractaments = tractaments+','+','.join(edar["terciaris"])
            obj[edar["wwtp"]] = tractaments
        obj["Cost diferencial"] = cost
        incompliments_masses.append(masses_aigua)
        n_incompliments = 0
        incompliment_per_contaminant = {}

        for massa_aigua in masses_aigua:
            #for contaminant in masses_aigua[massa_aigua]:
            for contaminant in contaminants_i_nutrients:
                if contaminant not in incompliment_per_contaminant:
                    incompliment_per_contaminant[contaminant] = 0
                if contaminant in masses_aigua[massa_aigua]:
                    incompliment_per_contaminant[contaminant] += 1
                    n_incompliments += 1

        incompliment_contaminant.append(incompliment_per_contaminant)
        obj["Nombre incompliments"] = n_incompliments
        configuracions.append(obj)


    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        pd.DataFrame(configuracions).to_excel(writer, sheet_name='Configuracions', startrow=1, startcol=0)
        pd.DataFrame(incompliments_masses).to_excel(writer, sheet_name="Incompliments per massa d'aigua", startrow=1, startcol=0)
        pd.DataFrame(incompliment_contaminant).to_excel(writer, sheet_name='Incompliments per contaminant', startrow=1, startcol=0)


