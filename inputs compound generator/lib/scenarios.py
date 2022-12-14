import pandas
from lib.calibrationMainConcentration import read_industries, read_edars, exportDataForNils, wwtp_info
from lib.db.renameSQLite import renameSQLite as rS
import lib.graph.Simulation as Simulation
import pandas as pd
import random
import csv
from UliPlot.XLSX import auto_adjust_xlsx_column_width
from lib.LazyCartesianProduct import LazyCartesianProduct
from lib.Optimitzacio_tec import Optimitzacio_tec

#aaaabvbvbvxcvxcvcx

#Donada una depuradora amb tractaments terciaris i cabal determinats, retorna el cost
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

def random_subset(s, threshold):
    criterion = lambda x: random.random() < threshold
    return list(filter(criterion, s))

#Donat un iterador it de longitud n_scenarios, retorna un iterador de k mostres aleatories
def sample(scenarios_combination, n_scenarios, k):
    cp = LazyCartesianProduct(scenarios_combination)
    for i in random.sample(range(n_scenarios), k):
        yield cp.entryAt(i)



"""
Configuracions de trens existents, si validar_osmosi_inversa es comprova que es pot posar UF+RO+AOP a les depuradores
Si nomes_escenari_base, es retorna la configuracio inicial de totes les depuradores
"""
def trens_tractament_reals(configuracions_edars, edars_cabal, n_iteracions, limitacio_osmosi_inversa = True, nomes_escenari_base = False):

    edars_osmosi_inversa = ['ES9081130006010E', 'ES9081270001010E', 'ES9080010001010E', 'ES9081140002010E', 'ES9082110001010E', 'ES9080440001010E']
    preu_inicial = 0
    escenaris_total = []
    n_escenaris_totals = 1

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

        #Unicament configuracio inicial
        if nomes_escenari_base:
            escenaris = {
                'secundari': secundari,
                'terciaris': terciaris,
                'wwtp': edar
            }
            escenaris_total.append([escenaris])

        else:
            if secundari is not None:
                if secundari == "SP" or secundari == "SN":
                    secundari = [secundari]
                else:
                    secundari = ["SC", "SP", "SN"]
            else:
                secundari = ["SC", "SP", "SN"]
            for secundari_aux in secundari:
                if terciaris is None:
                    escenaris = [
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["SF", "UV"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["GAC"],
                            'wwtp': edar
                        },
                        {
                            'secundari': secundari_aux,
                            'terciaris': ["UF", "UV"],
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
                        },
                    ]
                    if not limitacio_osmosi_inversa or edar in edars_osmosi_inversa:
                        escenaris.append({
                            'secundari': secundari_aux,
                            'terciaris': ["UF", "RO", "AOP"],
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
            n_escenaris_totals *= len(escenaris_wwtp)
    return sample(escenaris_total, n_escenaris_totals, min(n_escenaris_totals, n_iteracions)), preu_inicial

"""
Configuracions de tecnologies completament aleatories
"""
def trens_tractament_random(configuracions_edars, n_iteracions, threshold  = 0.7):

    tecnologies = ["SF", "UV", "GAC", "O3", "UF", "RO", "AOP"]
    escenaris_total = []
    for i in range(n_iteracions):
        escenari = []
        for edar in configuracions_edars:
            if 'secundari' in configuracions_edars[edar]:
                secundari = configuracions_edars[edar]["secundari"]
                if secundari == "SP" or secundari == "SN":
                    secundari = [secundari]
                else:
                    secundari = ["SC", "SP", "SN"]
            else:
                secundari = ["SC", "SP", "SN"]
            escenari.append({
                'wwtp': edar,
                'secundari': random.choice(secundari),
                'terciaris': random_subset(tecnologies, threshold)
            })
        escenaris_total.append(escenari)

    return escenaris_total, 0


def all_scenarios(edars_escenaris, edars_calibrated_init, n_iteracions, escenaris_reals=True):

    configuracions_edars = edars_escenaris.to_dict(orient='index')

    edars_cabal = {}
    for edar in edars_calibrated_init:
        edars_cabal[edar] = edars_calibrated_init[edar]["compounds_effluent"]["q"]

    if escenaris_reals:
        escenaris, preu_inicial = trens_tractament_reals(configuracions_edars, edars_cabal, n_iteracions, limitacio_osmosi_inversa=True,
                               nomes_escenari_base=False)
    else:
        escenaris, preu_inicial = trens_tractament_random(configuracions_edars, n_iteracions)

    return escenaris, preu_inicial

def nomes_escenari_base(edars_escenaris, edars_calibrated_init, n_iteracions):
    configuracions_edars = edars_escenaris.to_dict(orient='index')

    edars_cabal = {}
    for edar in edars_calibrated_init:
        edars_cabal[edar] = edars_calibrated_init[edar]["compounds_effluent"]["q"]

    escenaris, preu_inicial = trens_tractament_reals(configuracions_edars, edars_cabal, n_iteracions,
                                                     limitacio_osmosi_inversa=True,
                                                     nomes_escenari_base=True)

    #print(list(escenaris)[0])
    #pd.DataFrame(list(escenaris)[0]).to_csv("escenari_base_nils_2.csv")

    return escenaris, preu_inicial


def prepare_calibration(contaminant, g, graph):
    path = "C:\\Users\\jsalo\\Desktop\\obseravacions_contaminants\\"
    df = pd.read_csv(path + contaminant + ".csv")
    df[['lat', 'long']] = df['lat_long'].str.replace('"', '').str.split(' ', 1, expand=True)
    df['lat'] = df['lat'].astype(float)
    df['long'] = df['long'].astype(float)
    df = df.drop(columns=["lat_long"])
    observations_list = df.to_numpy().tolist()
    to_calibrate = []
    for observation, lat, long in observations_list:
        pixel = g.give_pixel([lat, long], "inputs/reference_raster.tif", True, False)
        if pixel in graph:
            conc = graph.nodes[pixel][contaminant] / graph.nodes[pixel]['flow_HR']
            to_calibrate.append([lat, long, observation, conc])

    to_calibrate_df = pd.DataFrame(to_calibrate, columns = ['lat', 'long', 'observation', 'prediction'])
    to_calibrate_df.to_csv("C:\\Users\\jsalo\\Desktop\\calibrar_nils\\"+contaminant+".csv")

def run_scenarios(connection, industrial_data, recall_points, contaminants_i_nutrients, edar_data_xlsx, removal_rate, industries_to_edar, industries_to_river, edars_calibrated_init, file_name, n_iteracions, window, graph_location, river_attenuation, excel_scenario, coord_to_pixel, coord_to_codi, llindars, resultat_escenaris, abocaments_ci, id_pixel):

    #contaminants_i_nutrients = ["Ciprofloxacina", "Cloroalcans", "Clorobenzè", "Hexabromociclodecà", "Nonilfenols", "Octilfenols", "Tetracloroetilè", "Triclorometà", "Niquel", "Plom", "Diuron"]
    contaminants_i_nutrients = ["Ciprofloxacina", "Cloroalcans", "Clorobenzè", "Nonilfenols", "Octilfenols", "Hexabromociclodecà", "Tetracloroetilè", "Triclorometà", "Niquel", "Plom", "Diuron"]

    store_calibration_files = False

    edars_cic = pd.read_excel(edar_data_xlsx, index_col=0)

    #Codi de depuradores amb les quals generem escenaris
    edars = edars_cic.loc[
        ["ES9080010001010E",
         "ES9080910001010E",
         "ES9083020001010E",
         "ES9081130006010E",
         "ES9081140002010E",
         "ES9081270001010E",
         "ES9081840001010E",
         "ES9082110001010E",
         "ES9082790004050E",
         "ES9080440001010E",
         "ES9080530002010E"
         ]]


    edars_osmosi_inversa = ['ES9081130006010E', 'ES9081270001010E', 'ES9080010001010E', 'ES9081140002010E', 'ES9082110001010E', 'ES9080440001010E']


    escenari_base, cost_inicial = nomes_escenari_base(edars, edars_calibrated_init, n_iteracions)



    #Canviar per resultat optimitzacio nils

    optimitzacio = Optimitzacio_tec(pd.DataFrame(pd.DataFrame(list(escenari_base)[0])))
    resultats = optimitzacio.optimize()
    #print('aaa')
    print(resultats[0])
    #scenarios, cost_inicial = all_scenarios(edars, edars_calibrated_init, n_iteracions, True)
    #print(list(scenarios))

    #Assignem càrregues d'origen industrial
    renameHelper = rS(None)
    id_discharge_to_volumes = read_industries(industries_to_river, industrial_data, recall_points,
                                              contaminants_i_nutrients, connection, removal_rate)
    pixel_to_poll = renameHelper.add_data_industry_to_graph(recall_points, id_discharge_to_volumes, contaminants_i_nutrients, abocaments_ci, id_pixel)


    g = Simulation.Simulation(graph_location, river_attenuation)

    #Per cada coordenada de cada pixel, diem quina massa d'aigua pertany
    masses_aigua = pandas.read_csv(coord_to_codi)
    masses_aigua["lat_lon"] = masses_aigua["lat"].map(str) + " " + masses_aigua["lon"].map(str)
    masses_aigua = masses_aigua.set_index('lat_lon').to_dict(orient = 'index')

    #Calcular nodes dels grafs a partir del raster
    """
    coords_to_pixel = {}
    for coord in masses_aigua.values():
        pixel = g.give_pixel([coord["lat"], coord["lon"]], "inputs/reference_raster.tif", True, False)
        coords_to_pixel[str(coord["lat"])+" "+str(coord["lon"]) ] = pixel
    """

    #Calcular a partir de fitxer pre-guardat
    coords_to_pixel = pandas.read_csv(coord_to_pixel)
    coords_to_pixel["lat_long"] = coords_to_pixel["lat"].astype(str)+" "+coords_to_pixel["long"].astype(str)
    coords_to_pixel = dict(coords_to_pixel.drop(columns=["lat", "long"]).reindex(columns=['lat_long','pixel']).values)

    #Per cada llindar massa d'aigua, diem quina concentracio màxima per contaminant es permet
    llindars_massa_aigua = pandas.read_excel(llindars, index_col=0)

    #Inicialitzem fitxer intermedi per guardar resultats
    with open("resultats.csv", 'w', newline='', encoding="utf-8") as write_obj:
        # Create a writer object from csv module
        csv_writer = csv.writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(['iteracio', 'escenari'])

    #Cabals de depuradores
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

            #print(edar['wwtp'], '---', calculate_price(edar['terciaris'], edars_cabal[edar['wwtp']]))

            cost_final += calculate_price(edar['terciaris'], edars_cabal[edar['wwtp']])
            if 'terciaris' in edar and edar['terciaris'] is not None:
                edars_aux.at[edar['wwtp'], 'terciari'] = ','.join(edar['terciaris'])
        edars_aux = edars_aux.combine_first(edars_cic)
        edars_aux.to_excel(excel_scenario)
        #Calculem contaminants a sortida de depuradora per configuració actual
        edars_calibrated = read_edars(contaminants_i_nutrients, industries_to_edar, excel_scenario, removal_rate, recall_points)

        #Afegim contaminació de depuradores al graf
        df_pixels = renameHelper.add_data_edar_to_graph(recall_points, edars_calibrated, contaminants_i_nutrients, pixel_to_poll.copy(), abocaments_ci)

        #Executem simulacio
        graph = g.run_graph(df_pixels)


        #Llegim estadistiques
        masses_aigua_valors = {}
        masses_aigua_incompliments = {}
        for contaminant in contaminants_i_nutrients:
            if store_calibration_files:
                prepare_calibration(contaminant, g, graph)

            #Per cada pixel de cada massa d'aigua, en llegim la concentracio
            for coord in masses_aigua:
                pixel = coords_to_pixel[coord]
                massa_aigua = masses_aigua[coord]['codi_ma']
                if massa_aigua not in masses_aigua_valors:
                    masses_aigua_valors[massa_aigua] = {}
                if contaminant not in masses_aigua_valors[massa_aigua]:
                    masses_aigua_valors[massa_aigua][contaminant] = []

                conc = graph.nodes[pixel][contaminant] / graph.nodes[pixel]['flow_HR']

                masses_aigua_valors[massa_aigua][contaminant].append(conc)

            #Per cada massa aigua, mirem si la seva mitjana de valors supera incompliment
            for massa_aigua in masses_aigua_valors:

                if massa_aigua not in masses_aigua_incompliments:
                    masses_aigua_incompliments[massa_aigua] = []

                mitjana_contaminant = sum(masses_aigua_valors[massa_aigua][contaminant]) / len(masses_aigua_valors[massa_aigua][contaminant])

                """
                if massa_aigua == 1000900 or massa_aigua == 1000740:
                    print(contaminant, massa_aigua, mitjana_contaminant)
                """
                #Multiplicadors per adaptar optimització de tot CIC al Baix Llobregat
                """
                if contaminant == "Ciprofloxacina":
                    mitjana_contaminant = mitjana_contaminant / 1.76
                elif contaminant == "Hexabromociclodecà":
                    mitjana_contaminant = mitjana_contaminant / 11.18
                elif contaminant == "Nonilfenols":
                    mitjana_contaminant = mitjana_contaminant / 0.016
                """


                if mitjana_contaminant > llindars_massa_aigua.at[contaminant, massa_aigua]:
                    masses_aigua_incompliments[massa_aigua].append(contaminant)

        #Guardem a fitxer intermig
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

    #Calculem estadistiques
    df = pandas.read_csv('resultats.csv')
    for string in list(df['escenari']):
        listObj.append(eval(string))

    def f(x):
        incompliments = 0
        for massa in x["masses_aigua_valors"]:
            incompliments += len(x["masses_aigua_valors"][massa])
        return incompliments, x["cost"]

    #Ordenem primer per nombre incompliments, despres per cost
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

    def write_to_excel(dictionary, writer, sheet_name, n):
        df = pd.DataFrame(dictionary).head(n)
        df.to_excel(writer, sheet_name=sheet_name, startrow=1, startcol=0)
        df.rename(columns=lambda x: str(x), inplace=True)
        auto_adjust_xlsx_column_width(df, writer, sheet_name=sheet_name, margin=0)

    # Guardem només els primers 1048576 resultats (màxim excel), ja que ja estan ordenats de millor a pitjor
    n = min(current_iteration, 1048576)
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        write_to_excel(configuracions, writer, 'Configuracions', n)
        write_to_excel(incompliments_masses, writer, "Incompliments per massa d'aigua", n)
        write_to_excel(incompliment_contaminant, writer, 'Incompliments per contaminant', n)


