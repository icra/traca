import psycopg2
from numpy import *
from itertools import groupby
import statistics
import openpyxl


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

    def getIndustriesToRiver(self, list_of_industries):
        try:
            cur = self.conn.cursor()
            query = 'SELECT tid, "activitat/ubicacio", nom_abocament, cod_ccae, ccae, "Tipus (LLM)", "Subtipus (LLM)", nom_variable, valor_maxim, unitats FROM cens_v3_full WHERE "activitat/ubicacio" || \' \' || "nom_abocament" IN ('
            for i in list_of_industries:
                query += " '" + i.replace('\'', '\'\'') + "',"
            query = query[:-1]
            query += ')'
            cur.execute(query)
            return cur.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def get_contaminants_i_nutrients_tipics(self):
        cur = self.conn.cursor()
        cur.execute('SELECT component FROM tipus_components')
        components = list(map(lambda component: component[0], cur.fetchall()))  # [comp_1, ..., comp_n]
        return components

    def getIndustries(self, table='cens_v4_1_prova'):
        try:
            cur = self.conn.cursor()
            query = 'SELECT * FROM ' + table
            cur.execute(query)
            return cur.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def generate_industrial_data(self):
        cur = self.conn.cursor()

        #Compounds
        components = self.get_contaminants_i_nutrients_tipics()

        #Cens industrial
        cur.execute('SELECT nom_variable, unitats, valor_minim, valor_maxim, ccae_l, isic_l FROM cens_v4')
        cens = list(cur.fetchall())
        cens_filtrat = list(filter(lambda x: x[0] in components and x[1] is not None, cens))

        """
        #Tots els compostos estan en les mateixes unitats (µg/l, ng/l, mg/l)
        cens_filtrat.sort(key = lambda x: (x[0], x[1]))
        for key, group in groupby(cens_filtrat, lambda x: (x[0], x[1])):
            compostos_grouped = list(group)
            print(compostos_grouped)
        """

        contaminants_per_tipologia = {}
        contaminants_per_tipologia_mitjanes = {}
        isic_to_ccae = {}

        step_0 = 0
        step_1 = 0
        step_2 = 0
        step_3 = 0


        # Agrupar per contaminant i codi de cens
        cens_filtrat.sort(key=lambda x: (x[0], x[4]))
        for key, group in groupby(cens_filtrat, lambda x: (x[0], x[4])):
            compostos_grouped = list(group)
            for compost in compostos_grouped:
                (contaminant, unitats, valor_minim, valor_maxim, ccae_l, isic_l) = compost
                if isic_l not in isic_to_ccae:
                    isic_to_ccae[isic_l] = ccae_l   #Guardem traduccio de isic a ccae
                if valor_minim is not None and valor_maxim is not None:
                    valor_mitja = (float(valor_minim) + float(valor_maxim)) / 2
                elif valor_minim is not None:
                    valor_mitja = float(valor_minim)
                else:
                    valor_mitja = float(valor_maxim)
                #Passem tot a mg/l
                if "µg/l" in unitats:
                    valor_mitja = valor_mitja / 1000
                elif "ng/l" in unitats:
                    valor_mitja = valor_mitja / 1000000

                if ccae_l not in contaminants_per_tipologia:
                    contaminants_per_tipologia[ccae_l] = {}
                    contaminants_per_tipologia_mitjanes[ccae_l] = {}
                if contaminant not in contaminants_per_tipologia[ccae_l]:
                    contaminants_per_tipologia[ccae_l][contaminant] = []
                    contaminants_per_tipologia_mitjanes[ccae_l][contaminant] = None
                contaminants_per_tipologia[ccae_l][contaminant].append(valor_mitja)
                step_0 += 1

        for ccae_l in contaminants_per_tipologia:
            for compound in contaminants_per_tipologia[ccae_l]:
                valors = contaminants_per_tipologia[ccae_l][compound]
                if len(valors) >= 5:
                    mitjana = statistics.mean(valors)
                    sd = statistics.stdev(valors)
                    cv = sd / mitjana
                    if (cv <= 1.5):
                        contaminants_per_tipologia_mitjanes[ccae_l][compound] = mitjana
                        step_1 += 1


        #Info Nils
        wb_ptr = openpyxl.load_workbook("inputs/means&stds.xlsx")
        ws_ptr = wb_ptr["Sheet 1"]
        isFirst = True

        for row in ws_ptr.iter_rows():
            if isFirst:
                isFirst = False
                continue
            isic = row[0].value
            compound = row[1].value
            if row[2].value is None: break
            mean = float(row[2].value)
            sd = float(row[3].value)
            if isic in isic_to_ccae:
                cod_ccae = isic_to_ccae[isic]
                if compound in components:
                    if cod_ccae not in contaminants_per_tipologia_mitjanes:
                        contaminants_per_tipologia_mitjanes[cod_ccae] = {}
                    if compound not in contaminants_per_tipologia_mitjanes[cod_ccae]:
                        contaminants_per_tipologia_mitjanes[cod_ccae][compound] = None
                    if contaminants_per_tipologia_mitjanes[cod_ccae][compound] is None:
                        contaminants_per_tipologia_mitjanes[cod_ccae][compound] = mean/1000000  #ng/L a mg/L
                        step_2 += 1


        #DB francesa

        cur.execute('SELECT valor, variable, cod_ccae FROM bd_francesa')
        db = list(cur.fetchall())
        db_filtrat = list(filter(lambda x: x[1] in components, db))
        for (valor, compound, cod_ccae) in db_filtrat:
            if cod_ccae not in contaminants_per_tipologia_mitjanes:
                contaminants_per_tipologia_mitjanes[cod_ccae] = {}
            if compound not in contaminants_per_tipologia_mitjanes[cod_ccae]:
                contaminants_per_tipologia_mitjanes[cod_ccae][compound] = None
            if contaminants_per_tipologia_mitjanes[cod_ccae][compound] is None:
                contaminants_per_tipologia_mitjanes[cod_ccae][compound] = valor / 1000  #Passem de µg/l a mg/l
                step_3 += 1

        print(step_0, step_1, step_2, step_3)
        return contaminants_per_tipologia_mitjanes

    def add_industry_to_edar(self, industries_to_edar, industry):
        tid, activitat_ubicacio, tipus_activitat, cod_ccae, tipus_llm, subtipus_llm, nom_abocament, nom_variable, valor_minim, valor_maxim, unitats, ccae_l, nace_l, isic_l, blocs, tipus, cod_ccae_xs, origen, uwwCode = industry

        if uwwCode not in industries_to_edar:
            industries_to_edar[uwwCode] = {}
        if activitat_ubicacio + ' ' + nom_abocament not in industries_to_edar[uwwCode]:
            industries_to_edar[uwwCode][activitat_ubicacio + ' ' + nom_abocament] = []
        industries_to_edar[uwwCode][activitat_ubicacio + ' ' + nom_abocament].append({
            "tid": tid,
            "activitat/ubicacio": activitat_ubicacio,
            "nom_abocament": nom_abocament,
            "cod_ccae": cod_ccae,
            "Tipus (LLM)": tipus_llm,
            "Subtipus (LLM)": subtipus_llm,
            "nom_variable": nom_variable,
            "valor_minim": valor_minim,
            "valor_maxim": valor_maxim,
            "unitats": unitats,
            "uwwCode": uwwCode,
        })

    def add_industry_to_river(self, industries_to_river, industry):
        tid, activitat_ubicacio, tipus_activitat, cod_ccae, tipus_llm, subtipus_llm, nom_abocament, nom_variable, valor_minim, valor_maxim, unitats, ccae_l, nace_l, isic_l, blocs, tipus, cod_ccae_xs, origen, uwwCode = industry

        if activitat_ubicacio + ' ' + nom_abocament not in industries_to_river:
            industries_to_river[activitat_ubicacio + ' ' + nom_abocament] = []
        industries_to_river[activitat_ubicacio + ' ' + nom_abocament].append({
            "tid": tid,
            "activitat/ubicacio": activitat_ubicacio,
            "nom_abocament": nom_abocament,
            "cod_ccae": cod_ccae,
            "Tipus (LLM)": tipus_llm,
            "Subtipus (LLM)": subtipus_llm,
            "nom_variable": nom_variable,
            "valor_minim": valor_minim,
            "valor_maxim": valor_maxim,
            "unitats": unitats,
        })

    def get_industries_to_edar_and_industry_separated(self):

        industries = self.getIndustries()
        industries_to_edar = {}
        industries_to_river = {}

        for industry in industries:
            tid, activitat_ubicacio, tipus_activitat, cod_ccae, tipus_llm, subtipus_llm, nom_abocament, nom_variable, valor_minim, valor_maxim, unitats, ccae_l, nace_l, isic_l, blocs, tipus, cod_ccae_xs, origen, uwwCode = industry

            if uwwCode is not None:     #industria aboca a depuradora
                if tipus_llm in ["Abocament", "Depuradora", "Entrada EDAR"]:    #Influent depuradora
                    self.add_industry_to_edar(industries_to_edar, industry)
                else:   #No passa per depuradora
                    self.add_industry_to_river(industries_to_river, industry)
            else:    #No passa per depuradora
                self.add_industry_to_river(industries_to_river, industry)
        return industries_to_edar, industries_to_river

    #funcions auxiliars

    def read_all_data(self, table='cens_v4_1_prova'):
        industries = self.getIndustries(table)
        industries_to_river = {}
        industries_grouped = {}

        for industry in industries:
            tid, activitat_ubicacio, tipus_activitat, cod_ccae, tipus_llm, subtipus_llm, nom_abocament, nom_variable, valor_minim, valor_maxim, unitats, ccae_l, nace_l, isic_l, blocs, tipus, cod_ccae_xs, origen, uwwCode = industry
            if activitat_ubicacio + ' ' + nom_abocament not in industries_to_river:
                industries_to_river[activitat_ubicacio + ' ' + nom_abocament] = []
            industries_to_river[activitat_ubicacio + ' ' + nom_abocament].append({
                "tid": tid,
                "activitat/ubicacio": activitat_ubicacio,
                "tipus_activitat": tipus_activitat,
                "cod_ccae": cod_ccae,
                "Tipus (LLM)": tipus_llm,
                "Subtipus (LLM)": subtipus_llm,
                "nom_abocament": nom_abocament,
                "nom_variable": nom_variable,
                "valor_minim": valor_minim,
                "valor_maxim": valor_maxim,
                "unitats": unitats,
                "ccae_l": ccae_l,
                "nace_l": nace_l,
                "isic_l": isic_l,
                "blocs": blocs,
                "tipus": tipus,
                "cod_ccae_xs": cod_ccae_xs,
                "origen": origen,
                "uwwCode": uwwCode
            })

        for key, industry in industries_to_river.items():
            aux_point = {
                "activitat/ubicacio": industry[0]["activitat/ubicacio"],
                "tipus_activitat": industry[0]["tipus_activitat"],
                "cod_ccae": industry[0]["cod_ccae"],
                "Tipus (LLM)": industry[0]["Tipus (LLM)"],
                "Subtipus (LLM)": industry[0]["Subtipus (LLM)"],
                "nom_abocament": industry[0]["nom_abocament"],
                "ccae_l": industry[0]["ccae_l"],
                "nace_l": industry[0]["nace_l"],
                "isic_l": industry[0]["isic_l"],
                "blocs": industry[0]["blocs"],
                "tipus": industry[0]["tipus"],
                "cod_ccae_xs": industry[0]["cod_ccae_xs"],
                "uwwCode": industry[0]["uwwCode"]
            }

            for compound in industry:
                if compound["valor_minim"] is not None or compound["valor_maxim"] is not None:
                    if compound["valor_minim"] is not None and compound["valor_maxim"] is not None:
                        valor_mitja = (float(compound["valor_minim"]) + float(compound["valor_maxim"])) / 2
                    elif compound["valor_maxim"] is not None:
                        valor_mitja = float(compound["valor_maxim"])
                    elif compound["valor_minim"] is not None:
                        valor_mitja = float(compound["valor_minim"])

                    """
                    Passem tot a mg/l (tots els contaminents que tractem estan en µg/l, ng/l o mg/l, el cabal 
                    esta en m3/dia o m3/any i no es veu afectat)
                    """
                    unitats = compound["unitats"]
                    if unitats is not None:
                        if "µg/l" in unitats:
                            valor_mitja = valor_mitja / 1000
                        elif "ng/l" in unitats:
                            valor_mitja = valor_mitja / 1000000

                    aux_point[compound["nom_variable"]] = valor_mitja

            industries_grouped[key] = aux_point

        return industries_grouped

    def upload_data(self, industries_grouped, contaminants_i_nutrients, estimacions):

        tid = 0
        compounds = contaminants_i_nutrients.copy()
        compounds.append("Cabal diari")
        compounds.append("Cabal anual")
        c = self.conn.cursor()

        for industry in industries_grouped.values():

            activitat_ubicacio = industry["activitat/ubicacio"]
            tipus_activitat = industry["tipus_activitat"]
            cod_ccae = industry["cod_ccae"]
            tipus_llm = industry["Tipus (LLM)"]
            subtipus_llm =  industry["Subtipus (LLM)"]
            nom_abocament =  industry["nom_abocament"]
            ccae_l = industry["ccae_l"]
            nace_l = industry["nace_l"]
            isic_l = industry["isic_l"]
            blocs = industry["blocs"]
            tipus = industry["tipus"]
            cod_ccae_xs = industry["cod_ccae_xs"]
            uwwCode = industry["uwwCode"]

            for contaminant in compounds:
                estimation = None
                if ccae_l in estimacions:
                    if contaminant in estimacions[ccae_l]:
                        estimation = estimacions[ccae_l][contaminant]


                if contaminant in industry or estimation is not None:

                    tid += 1

                    nom_variable = contaminant

                    if nom_variable == 'Cabal diari':
                        unitats = "m3/dia"
                    elif nom_variable == 'Cabal anual':
                        unitats = "m3/any"
                    else:
                        unitats = "mg/l"

                    if contaminant in industry:
                        valor_maxim = industry[contaminant]
                        origen = "ACA"
                    elif estimation is not None:
                        valor_maxim = estimation
                        origen = "ICRA"

                    query = """INSERT INTO cens_v4_1_prova (tid, "activitat/ubicacio", "Tipus Activitat/Ubicació (A/U)", cod_ccae, "Tipus (LLM)", "Subtipus (LLM)", nom_abocament, nom_variable, valor_minim, valor_maxim, unitats, ccae_l, nace_l, isic_l, blocs, tipus, cod_ccae_xs, origen, "uwwCode") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    params = (tid, activitat_ubicacio, tipus_activitat, cod_ccae, tipus_llm, subtipus_llm, nom_abocament, nom_variable, None, valor_maxim, unitats, ccae_l, nace_l, isic_l, blocs, tipus, cod_ccae_xs, origen, uwwCode)

                    c.execute(query, params)
                    print(tid)

        self.conn.commit()
