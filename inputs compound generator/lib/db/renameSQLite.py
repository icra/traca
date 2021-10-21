import sqlite3
from sqlite3 import Error
import math
from classes.Config import Config


# Distancia entre p1 i p2 (km) sobre una esfera. Han de ser un diccionari amb entrades "lan" i "lon"
def distance(p1, p2):
    r = 6373.0  # radius of the earth
    lat1 = math.radians(p1["lat"])
    lon1 = math.radians(p1["lon"])
    lat2 = math.radians(p2["lat"])
    lon2 = math.radians(p2["lon"])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    haversine_a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    haversine_c = 2 * math.atan2(math.sqrt(haversine_a), math.sqrt(1 - haversine_a))

    distance_between_points = r * haversine_c
    return distance_between_points


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

        # wwt_file_name = values["wwt_file_name"]
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
                    c.execute('DELETE FROM recall_dat where id=?', (id_sql,))
                    new_data_table.append([
                        dp_id, name, id_sql, dp_id, distance
                    ])
                else:
                    new_data_table.append(row)
            conn.commit()
            return new_data_table

        except Error as e:
            raise e
        finally:
            if conn:
                conn.close()