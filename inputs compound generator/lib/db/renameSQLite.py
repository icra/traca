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

    def add_data_to_swat(self, edars_calibrated, volumes, contaminants_i_nutrients):

        try:
            conn = sqlite3.connect(self.url)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            # Insert data of volumes to swat database
            for point in volumes:
                try:
                    dbo = 0
                    fosfor = 0
                    ptl_n = 0
                    nh3_n = 0
                    no3_n = 0
                    cabal = 0
                    fosfats = 0

                    if 'DBO 5 dies' in volumes[point]:
                        dbo = volumes[point]['DBO 5 dies']
                    if 'Fòsfor orgànic' in volumes[point]:
                        fosfor = volumes[point]['Fòsfor orgànic']
                    if 'Nitrogen orgànic' in volumes[point]:
                        ptl_n = volumes[point]["Nitrogen orgànic"]  # organic nitrogen
                    if 'Amoniac' in volumes[point]:
                        nh3_n = volumes[point]["Amoniac"]  # ammonia
                    if 'Nitrats' in volumes[point]:
                        no3_n = volumes[point]["Nitrats"]  # nitrate
                    if 'q' in volumes[point]:
                        cabal = volumes[point]["q"]
                    if 'Fosfats' in volumes[point]:
                        fosfats = volumes[point]["Fosfats"]

                    # Constant
                    c.execute(
                        ''' UPDATE recall_dat
                              SET flo = ? + flo,
                                  orgn = ? + orgn,
                                  sedp = ? + sedp,
                                  no3 = ? + no3,  
                                  nh3 = ? + nh3,   
                                  cbod = ? + cbod,
                                  solp = ? + solp
                              WHERE recall_rec_id = ?''',
                        (cabal, ptl_n, fosfor, no3_n, nh3_n, dbo, fosfats, point,))


                    #Per cadascun dels contaminants que no va a recall_dat, posar-lo a recall_pollutants_dat
                    for contaminant in contaminants_i_nutrients:
                        #if contaminant not in ["DBO 5 dies", "Fòsfor orgànic", "Nitrogen orgànic", "Amoniac", "Nitrats", "Fosfats"] and contaminant in volumes[point]:
                        if contaminant not in ["Fòsfor orgànic", "Nitrogen orgànic", "Amoniac", "Nitrats", "Fosfats"] and contaminant in volumes[point]:

                            #Mirem que el contaminant estigui creat a db
                            c.execute(
                                ''' SELECT id FROM pollutants_pth WHERE name = ?''',
                                (contaminant, ))

                            rows = c.fetchall()
                            if len(rows) > 0:

                                c.execute(
                                    ''' SELECT * FROM recall_pollutants_dat WHERE recall_rec_id = ? and pollutants_pth_id = (SELECT id FROM pollutants_pth WHERE name=?)''',
                                    (point, contaminant,))

                                rows = c.fetchall()

                                #En cas que no existeixi entrada, la creem
                                if len(rows) > 0:
                                    c.execute(
                                        ''' UPDATE recall_dat
                                              SET load = ? + load,
                                              WHERE (recall_rec_id = ? and pollutants_pth_id = (SELECT id FROM pollutants_pth WHERE name=?))''',
                                        (cabal, point, contaminant))
                                else:
                                    c.execute(
                                        ''' INSERT INTO recall_pollutants_dat (recall_rec_id, pollutants_pth_id, jday, mo, day_mo, yr, load) VALUES 
                                                  (?, (SELECT id FROM pollutants_pth WHERE name=?), ?, ?, ?, ?, ?)''',
                                        (point, contaminant, 1, 1, 1, 1, volumes[point][contaminant]))


                except Error as error:
                    print(error)

            # Insertar dades de edars_calibrated
            for edar in edars_calibrated.values():
                try:
                    if "id_swat" in edar and "compounds_effluent" in edar:    #Si no te key "nom_swat" es que aboca a mar

                        dbo = 0
                        fosfor = 0
                        ptl_n = 0
                        nh3_n = 0
                        no3_n = 0
                        cabal = 0
                        fosfats = 0

                        if 'DBO 5 dies' in edar["compounds_effluent"]:
                            dbo = edar["compounds_effluent"]["DBO 5 dies"]
                        if 'Fòsfor orgànic' in edar["compounds_effluent"]:
                            fosfor = edar["compounds_effluent"]["Fòsfor orgànic"]
                        if 'Nitrogen orgànic' in edar["compounds_effluent"]:
                            ptl_n = edar["compounds_effluent"]["Nitrogen orgànic"]  #organic nitrogen
                        if 'Amoniac' in edar["compounds_effluent"]:
                            nh3_n = edar["compounds_effluent"]["Amoniac"]  #ammonia
                        if 'Nitrats' in edar["compounds_effluent"]:
                            no3_n = edar["compounds_effluent"]["Nitrats"]  #nitrate
                        if 'q' in edar["compounds_effluent"]:
                            cabal = edar["compounds_effluent"]["q"]
                        if 'Fosfats' in edar["compounds_effluent"]:
                            fosfats = edar["compounds_effluent"]["Fosfats"]


                        # Constant
                        c.execute(
                            ''' UPDATE recall_dat
                                       SET flo = ? + flo,
                                           orgn = ? + orgn,
                                           sedp = ? + sedp,
                                           no3 = ? + no3,
                                           nh3 = ? + nh3,
                                           cbod = ? + cbod,
                                           solp = ? + solp
                                       WHERE recall_rec_id = ?''',
                                 (cabal, ptl_n, fosfor, no3_n, nh3_n, dbo, fosfats, edar["id_swat"],))

                except Error as error:
                    print(error)
                    print("No es pot afegir WWTP ", edar["eu_code"])

            conn.commit()

        except Error as e:
            print(e)
            raise e
        finally:
            if conn:
                conn.close()

