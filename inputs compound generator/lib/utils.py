import math
import calibrationMainConcentration
import json

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


# Per cada discharge point, diu la concentració de cada contaminant a l'efluent
def estimate_effluent(calibrated_parameters, listOfUww):

    listEdars = calibrationMainConcentration.calcAllDataForNilsConcentration()

    with open('data.txt', 'w') as outfile:
        json.dump(listEdars, outfile)

    q_per_capita = 0.242  # m3/dia/habitant
    contaminants = ["dbo", "fosfor"]
    estimated_load_wwtp_effluent = {}

    for edar_key in listEdars.keys():

        try:
            wwtp = listEdars[edar_key]
            population = float(wwtp["population_real"])
            cabal_domestic = q_per_capita * population   #m3/dia
            cabal_influent_industrial = wwtp["industriesTotalInfluent"]["cabal"]    #m3/dia
            cabal_efluent_industrial = wwtp["industriesTotalEffluent"]["cabal"]     #m3/dia
            compounds_effluent = {
                "cabal": wwtp["industriesTotalInfluent"]["cabal"] + wwtp["industriesTotalEffluent"]["cabal"] + cabal_domestic   #m3/dia
            }

            for contaminant in contaminants:
                load_influent_domestic = population * calibrated_parameters[contaminant]["generation_per_capita"] / 1000  # kg/dia
                load_influent_industrial = wwtp["industriesTotalInfluent"][contaminant] * cabal_influent_industrial / 1000  # kg/dia
                load_efluent_industrial = wwtp["industriesTotalEffluent"][contaminant] * cabal_efluent_industrial / 1000  # kg/dia

                load_influent_filtered = load_influent_industrial + load_influent_domestic
                for configuration in wwtp["configuration"]:
                    load_influent_filtered *= (1 - (calibrated_parameters[contaminant][configuration] / 100))

                compounds_effluent[contaminant] = (load_influent_filtered + load_efluent_industrial) # kg

            eu_code = wwtp["eu_code"]
            dp_id = listOfUww[eu_code]["uwwDP"]
            estimated_load_wwtp_effluent[dp_id] = compounds_effluent

        except Exception as e:
            print(edar_key+" missing field: "+str(e))

    return estimated_load_wwtp_effluent
