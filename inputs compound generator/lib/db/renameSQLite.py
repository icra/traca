import sqlite3
from sqlite3 import Error


class renameSQLite:

    def __init__(self, url):
        self.url = url

    def __read_table(self, table_name):
        """ Llegir taula table i retornar-la en forma de diccionari """
        conn = None
        try:
            conn = sqlite3.connect(self.url)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            query = 'SELECT * FROM ' + table_name
            c.execute(query)
            result = [dict(row) for row in c.fetchall()]

            return result
        except Error as e:
            raise(e)
        finally:
            if conn:
                conn.close()

    def add_data_to_swat(self, edars_calibrated):

        try:
            conn = sqlite3.connect(self.url)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            for edar in edars_calibrated.values():
                try:
                    if("nom_swat" in edar and "compounds_effluent" in edar):    #Si no te key "nom_swat" es que aboca a mar

                        name = edar["nom_swat"]
                        dbo = edar["compounds_effluent"]["dbo"]
                        fosfor = edar["compounds_effluent"]["fosfor"]
                        ptl_n = edar["compounds_effluent"]["nitrogen_org"]  #organic nitrogen
                        nh3_n = edar["compounds_effluent"]["amoni"]  #ammonia
                        no3_n = edar["compounds_effluent"]["nitrats"]  #nitrate
                        cabal = edar["compounds_effluent"]["cabal"]


                        #Esborrar entrada existent del DP
                        c.execute("DELETE FROM recall_dat WHERE recall_rec_id = ("
                                  "SELECT rec_id FROM recall_con WHERE name=?)", (name,))

                        #Diari
                        """
                        for i in range(1, 366):
                            c.execute('INSERT INTO recall_dat (recall_rec_id, yr, t_step, flo, sed, ptl_n, ptl_p, no3_n, sol_p, chla, nh3_n, no2_n, cbn_bod, oxy, sand, silt, clay, sm_agg, lg_agg, gravel, tmp) '
                              'VALUES ((SELECT id FROM recall_rec WHERE name = ?), 0, ?, ?, 0, 0, ?, 0, 0, 0, 0, 0, ?, 0, 0, 0, 0, 0, 0, 0, 0)', (dp_id, i, cabal, fosfor, dbo))
                        """
                        # Constant

                        c.execute(
                            'INSERT INTO recall_dat (recall_rec_id, yr, t_step, flo, sed, ptl_n, ptl_p, no3_n, sol_p, chla, nh3_n, no2_n, cbn_bod, oxy, sand, silt, clay, sm_agg, lg_agg, gravel, tmp) '
                            'VALUES ((SELECT rec_id FROM recall_con WHERE name = ?), 0, 0, ?, 0, ?, ?, ?, 0, 0, ?, 0, ?, 0, 0, 0, 0, 0, 0, 0, 0)',(name, cabal, ptl_n, fosfor, no3_n, nh3_n, dbo))



                except Error as error:
                    print("No es pot afegir WWTP ", edar["eu_code"])

            conn.commit()

        except Error as e:
            raise e
        finally:
            if conn:
                conn.close()

