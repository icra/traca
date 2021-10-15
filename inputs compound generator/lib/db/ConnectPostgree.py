import psycopg2
import nlopt
from numpy import *

class ConnectDb:
    """A simple to connect to postgree db"""

    def __init__(self, host, database, user, password):
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password)
        try:
            # create a cursor
            cur = self.conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def testConnection(self, host, database, user, password):

        try:
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password)
            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            cur.close()

            return True, db_version
            # close the communication with the PostgreSQL

        except (Exception, psycopg2.DatabaseError) as error:
            return False, error

    def getAllWWTP(self):
        try:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM uwwtps_cod_aca')

            return cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getIndustriesToEdar(self):
        try:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM industry_to_edar_2')

            return cur.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def calibrateContaminant(self):

        opt = nlopt.opt(nlopt.LD_MMA, 3)
        print("Show Calibration nlopt version:")
        print(nlopt.version_major())