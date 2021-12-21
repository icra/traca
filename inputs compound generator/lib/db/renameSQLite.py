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
                if row[2] != "-":
                    dp_id = row[0]
                    name = row[1]
                    id_sql = row[2]
                    name_sql = row[3]
                    distance = row[4]
                    c.execute('UPDATE recall_con SET name=? where name=?', (dp_id, name_sql,))
                    c.execute('UPDATE recall_rec SET name=?, rec_typ=1 where name=?', (dp_id, name_sql,))
                    c.execute('DELETE FROM recall_dat where recall_rec_id=?', (id_sql,))
                    new_data_table.append([
                        dp_id, name, id_sql, dp_id, distance
                    ])
                else:
                    c.execute('DELETE FROM recall_dat where id=?', (id_sql,))   #No te cap depuradora de la db assignada
                    c.execute('DELETE FROM recall_rec where id=?', (id_sql,))
                    c.execute('DELETE FROM recall_con where recall_rec_id=?', (id_sql,))
                    new_data_table.append(row)
            conn.commit()
            return new_data_table

        except Error as e:
            raise e
        finally:
            if conn:
                conn.close()

    def add_data_to_swat(self, estimated_concentrations):
        print(estimated_concentrations)
