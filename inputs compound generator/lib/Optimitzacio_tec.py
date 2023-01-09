# %%

#import json
import numpy as np
#from pandas import date_range
#import matplotlib.pyplot as plt
#import calibrationMainConcentration
import spotpy
#import scipy.stats as st
import pandas as pd
import joblib
#from sklearn.ensemble import HistGradientBoostingRegressor
import math
import sklearn


# %%
class Optimitzacio_tec:
    def __init__(self, escenari_base):

        self.esc_base = escenari_base
        print(self.esc_base)
        print('------------')


        self.num_dep = len(self.esc_base)
        self.names_edars = []

        for i in range(self.num_dep):
            self.names_edars.append(self.esc_base["wwtp"][i])

        self.tecnologies = ["SC", "SN", "SP", "SF", "UV", "GAC", "O3", "UF", "RO", "AOP"]
        self.histxgboost = joblib.load('lib/XGboost_escenaris')
        self.results = []
    def evaluate(self, par):
        num_tec = 10
        x = np.zeros(num_tec * self.num_dep * 2).reshape(2, num_tec * self.num_dep)
        for i in range(self.num_dep):
            for j in range(3):
                if self.esc_base["secundari"][i] == self.tecnologies[j]:
                    x[0][i * num_tec + j] = 1


            if self.esc_base["terciaris"][i] is not None:
                for j in range(3, 10):
                    if self.tecnologies[j] in self.esc_base["terciaris"][i]:
                        x[0][i * num_tec + j] = 1
            # 0 -> sense modificar
            # 1 -> [SF,UV]
            # 2 -> [GAC]
            # 3 -> [UF, UV]
            # 4 -> [O3, GAC, UV]
            # 5 -> [O3, SF, UV]
            # 6 -> [O3, SF]
            # 7 -> [UF, RO, AOP]
            if int(par[i]) == 1:
                x[0][i * num_tec + 3] = 1
                x[0][i * num_tec + 4] = 1
            if int(par[i]) == 2:
                x[0][i * num_tec + 5] = 1
            if int(par[i]) == 3:
                x[0][i * num_tec + 7] = 1
                x[0][i * num_tec + 4] = 1
            if int(par[i]) == 4:
                x[0][i * num_tec + 6] = 1
                x[0][i * num_tec + 5] = 1
                x[0][i * num_tec + 4] = 1
            if int(par[i]) == 5:
                x[0][i * num_tec + 6] = 1
                x[0][i * num_tec + 3] = 1
                x[0][i * num_tec + 4] = 1
            if int(par[i]) == 6:
                x[0][i * num_tec + 6] = 1
                x[0][i * num_tec + 3] = 1
            if int(par[i]) == 7:
                x[0][i * num_tec + 7] = 1
                x[0][i * num_tec + 8] = 1
                x[0][i * num_tec + 9] = 1

        pred = self.histxgboost.predict(x)

        return round(pred[0])

    class spotpy_setup_1(object):

        # ['ES9081130006010E', 'ES9081270001010E', 'ES9080010001010E', 'ES9081140002010E',
        #          'ES9082110001010E', 'ES9080440001010E'] poden tenir osmosi inversa

        # ES9081130006010E -> edar4, ES9081270001010E -> edar6, ES9080010001010E -> edar1
        # ES9081140002010E -> edar5, ES9082110001010E -> edar8, ES9080440001010E -> edar10

        def __init__(self, names_edars, esc_base, evaluate, filtres=None):

            if filtres is None:
                filtres = set()

            self.evaluate = evaluate
            self.params = []
            ind = 0

            for edar in names_edars:
                # de moment si ja té terciaris no hi ha canvis.
                if esc_base["terciaris"][ind] is not None:  # is not nan
                    self.params.append(spotpy.parameter.Uniform(edar, 0, 0))
                #Si es vol aplicar filtres de cabal i cabal depuradora < 20000, no hi ha canvis
                elif 'cabal' in filtres and esc_base['cabal'][ind] < 20000:
                    self.params.append(spotpy.parameter.Uniform(edar, 0, 0))
                #si filtre o3, no es poden aplicar tractaments amb o3
                elif 'o3' in filtres:
                    #QUÈ S'HA DE POSAR AQUÍ
                    self.params.append(spotpy.parameter.Uniform(edar, 0, 0))
                else:
                    if edar in ['ES9081130006010E', 'ES9081270001010E', 'ES9080010001010E', 'ES9081140002010E',
                                'ES9082110001010E', 'ES9080440001010E']:
                        self.params.append(spotpy.parameter.Uniform(edar, 0, 7.98))
                    else:
                        self.params.append(spotpy.parameter.Uniform(edar, 0, 6.98))

                ind = ind + 1

        def parameters(self):
            return spotpy.parameter.generate(self.params)

        # defining the simulation for the given objective function
        def simulation(self, vector):
            x = np.array(vector)
            simulations = [self.evaluate(x)]
            return simulations

        # Response we want for our function, the result of our observations
        def evaluation(self):
            observations = [0]
            return observations

        # define objective function
        def objectivefunction(self, simulation, evaluation):
            objectivefunction = spotpy.objectivefunctions.mae(evaluation, simulation)
            return objectivefunction

    def optimize(self):
        rep = 15000
        parallel = "seq"
        dbformat = "csv"
        timeout = 10  # Given in Seconds


        spot_setup_1 = self.spotpy_setup_1(self.names_edars, self.esc_base, self.evaluate)

        sampler = spotpy.algorithms.sceua(spot_setup_1, parallel=parallel, dbname='opt_edars', dbformat=dbformat,
                                          sim_timeout=timeout)
        sampler.sample(rep)
        self.results = spotpy.analyser.load_csv_results('opt_edars')
        return self.best_results()

    def best_results(self):
        minimum = math.inf
        for i in range(len(self.results)):
            if minimum > self.results[i][0]:
                minimum = self.results[i][0]

        best_results = []
        for i in range(len(self.results)):
            if (self.results[i][0] == minimum) or (self.results[i][0] == minimum + 1):
                best_results.append(self.results[i])

        tec_trains = ["no_mod", ['SF', 'UV'], ["GAC"], ["UF", "UV"], ["O3","GAC","UV"], ["O3","SF", "UV"], ["O3", "SF"], ["UF","RO","AOP"]]

        millors_configuracions = []

        for i in range(len(best_results)):
            secundari = []
            terciaris = []

            for j in range(1, self.num_dep + 1):
                secundari.append(self.esc_base["secundari"][j - 1])
                if int(best_results[i][j]) >= 1:
                    terciaris.append(tec_trains[int(best_results[i][j])])
                else:
                    terciaris.append(self.esc_base["terciaris"][j - 1])
            d = {"secundari": secundari, "terciaris": terciaris, "wwtp": self.names_edars}
            df = pd.DataFrame(data=d)
            millors_configuracions.append(df)

        return millors_configuracions
   

        




