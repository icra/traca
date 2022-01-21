import sqlite3
from sqlite3 import Error
from lib.utils import distance


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

    def populate_table(self, table_name, data_from_db, threshold):
        """
        Per cada depuradora de data_from_db,assigna depuradora de table_name més propera (dins del threshold)
        """
        try:
            data = []
            result = self.__read_table(table_name)
            for wwtp in data_from_db:
                wwtp_db = {
                    "dp_id": wwtp[1],
                    "name": wwtp[2],
                    "lat": wwtp[3],
                    "lon": wwtp[4]
                }

                closest_point = min(result, key=lambda i: distance(wwtp_db, i))
                closest_point_distance = round(distance(wwtp_db, closest_point), 3)
                if closest_point_distance <= float(threshold):
                    data.append([
                        wwtp_db["dp_id"], wwtp_db["name"], closest_point["id"], closest_point["name"],
                        closest_point_distance
                    ])
                else:
                    data.append([
                        wwtp_db["dp_id"], wwtp_db["name"], "-", "-", "-"
                    ])
            return data
        except Error as e:
            raise e

    def rename_db(self, data_rename):
        try:
            conn = sqlite3.connect(self.url)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            new_data_table = []

            for row in data_rename:
                dp_id = row[0]
                name = row[1]
                id_sql = row[2]
                name_sql = row[3]
                distance_between = row[4]

                if row[2] != "-":
                    c.execute('DELETE FROM recall_dat WHERE recall_rec_id = ('
                              'SELECT rec_id FROM recall_con WHERE name=?)', (name_sql,))

                    c.execute('UPDATE recall_con SET name=? where name=?', (dp_id, name_sql,))
                    #c.execute('UPDATE recall_rec SET name=?, rec_typ=1 where name=?', (dp_id, name_sql,))  #Diari
                    c.execute('UPDATE recall_rec SET name=?, rec_typ=4 where name=?', (dp_id, name_sql,))   #Constant

                    new_data_table.append([
                        dp_id, name, id_sql, dp_id, distance_between
                    ])
                else:  # No te cap depuradora de la db assignada
                    # c.execute('DELETE FROM recall_con where name=?', (id_sql,))
                    # c.execute('DELETE FROM recall_rec where id=?', (id_sql,))
                    new_data_table.append(row)

            """
            c.execute('DELETE FROM recall_con_out WHERE recall_con_id IN ('
                      'SELECT id FROM recall_con where rec_id IN ('
                      'SELECT id FROM recall_rec where rec_typ!=1))')

            c.execute('DELETE FROM recall_con WHERE rec_id IN ('
                      'SELECT id FROM recall_rec where rec_typ!=1)')

            c.execute('DELETE FROM recall_dat WHERE recall_rec_id IN ('
                      'SELECT id FROM recall_rec where rec_typ!=1)')

            c.execute('DELETE FROM recall_rec WHERE rec_typ!=1')
            """

            conn.commit()
            return new_data_table

        except Error as e:
            raise e
        finally:
            if conn:
                conn.close()

    def add_data_to_swat(self, estimated_concentrations):
        try:
            conn = sqlite3.connect(self.url)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            for dp_id in estimated_concentrations.keys():
                dp = estimated_concentrations[dp_id]
                dbo = dp["dbo"]
                fosfor = dp["fosfor"]
                ptl_n = dp["nitrogen_org"]  #organic nitrogen
                nh3_n = dp["amoni"]  #ammonia
                no3_n = dp["nitrats"]  #nitrate
                cabal = dp["cabal"]
                try:
                    #Diari
                    """
                    for i in range(1, 366):
                        c.execute('INSERT INTO recall_dat (recall_rec_id, yr, t_step, flo, sed, ptl_n, ptl_p, no3_n, sol_p, chla, nh3_n, no2_n, cbn_bod, oxy, sand, silt, clay, sm_agg, lg_agg, gravel, tmp) '
                          'VALUES ((SELECT id FROM recall_rec WHERE name = ?), 2021, ?, ?, 0, 0, ?, 0, 0, 0, 0, 0, ?, 0, 0, 0, 0, 0, 0, 0, 0)', (dp_id, i, cabal, fosfor, dbo))
                    """
                    # Constant
                    c.execute(
                        'INSERT INTO recall_dat (recall_rec_id, yr, t_step, flo, sed, ptl_n, ptl_p, no3_n, sol_p, chla, nh3_n, no2_n, cbn_bod, oxy, sand, silt, clay, sm_agg, lg_agg, gravel, tmp) '
                        'VALUES ((SELECT id FROM recall_rec WHERE name = ?), 2021, 0, ?, 0, ?, ?, ?, 0, 0, ?, 0, ?, 0, 0, 0, 0, 0, 0, 0, 0)',(dp_id, cabal, ptl_n, fosfor, no3_n, nh3_n, dbo))


                except Error:
                    print("No es pot afegir WWTP ", dp_id)
            conn.commit()

        except Error as e:
            raise e
        finally:
            if conn:
                conn.close()

