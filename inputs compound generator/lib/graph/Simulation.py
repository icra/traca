import os.path
import time
import pandas
import numpy
numpy.seterr(all='raise')
"""
import shapefile_raster_functions
"""
#import graph_functions
import pickle
import networkx
import math
import pickle

class Simulation:
    def __init__(self):
        self.attenuation_df = pandas.read_csv("./inputs/percentatges_eliminacio_tots_calibrats.csv").set_index('Nom')


        open_file = open('./inputs/catalonia_graph.pkl', "rb")
        self.river_graph = networkx.read_gpickle(open_file)
        open_file.close()
        nodelist = []

        for node in self.river_graph:
            if self.river_graph.nodes[node]['basin'] == 12207:
                self.river_graph.nodes[node]['pixel_number'] = node
                nodelist.append(node)

        self.river_graph = networkx.subgraph(self.river_graph, nodelist)

        self.sorted_river_list = list(networkx.topological_sort(self.river_graph))
        """
        columns = list(self.contamination_df.columns)
        columns.pop(0)
        """


    def closest_pixel(self, coords, renameHelper):
        coords_pixel = list(map(lambda x: [x[1]['longitude'], x[1]['latitude'], x[0]], self.river_graph.nodes(data = True)))
        coord_to_pixel = {}

        for coord in coords:
            closest_point = renameHelper.shortest_dist(coords_pixel, coord)
            coord_to_pixel[str(coord[0])+" "+str(coord[1])] = closest_point[2]
        return coord_to_pixel

    def graph_to_df_with_tp(self, river_graph, topological_sort, attributes):
        """
         This code takes a river graph and converts it to a dataframe (WITH topological information).
         :rtype: pandas.DataFrame
         :river_graph: networkx.DiGraph or str: the river graph or the location on the computer
         :topological_sort: list of ints: list of pixel numbers that represent the hierarchy of the graph
         :attributes: list of str: the attributes that you wish to be in the dataframe.
         :return: dataframe with information on the nodes of the river graph. Contains topological information.
         """
        first_row = (len(attributes) + 2) * [0]
        graph_df = pandas.DataFrame(first_row)
        graph_df = graph_df.transpose()
        graph_df.columns = ['pixel_number'] + attributes + ['parents']
        graph_df['parents'] = graph_df['parents'].astype(object)
        for i in range(len(topological_sort)):
            new_row = []
            new_row.append(topological_sort[i])
            for attribute in attributes:
                new_row.append(river_graph.nodes[topological_sort[i]][attribute])
            parents = river_graph.predecessors(topological_sort[i])
            new_row.append(list(parents))
            graph_df.loc[i] = new_row


        graph_df = graph_df.set_index('pixel_number')

        return graph_df

    def run_graph(self, contamination_df):


        contamination_df = contamination_df.set_index('pixel_number')
        contamination_df = contamination_df.groupby(contamination_df.index).sum()
        contaminant_names = contamination_df.columns
        g = self.river_graph.copy()


        RT = 'RT_HR'
        dis = 'flow_HR'

        for contaminant in contaminant_names:
            attenuation = self.attenuation_df.at[contaminant, '% eliminació per hora'] / 100
            multiplicador = self.attenuation_df.at[contaminant, 'Multiplicador']


            """
            for pixel_number in contamination_df.index:
                if pixel_number in self.river_graph:
                    self.river_graph.nodes[pixel_number][contaminant] += contamination_df.at[pixel_number, contaminant]
            """
            for n in self.sorted_river_list:  # for all river pixels
                if n in contamination_df.index:
                    g.nodes[n][contaminant] = contamination_df.at[n, contaminant] * multiplicador

                else:
                    g.nodes[n][contaminant] = 0


                # Add the contamination of the parent cells
                parents = list(g.predecessors(n))
                for k in parents:
                    if k in contamination_df:
                        g.nodes[n][contaminant] += g.nodes[k][contaminant]

                g.nodes[n][contaminant] *= math.exp(-attenuation * g.nodes[n][RT])


                #self.river_graph.nodes[n][rel_cont] = self.river_graph.nodes[n][contam] / self.river_graph.nodes[n][dis]


        #output_name = os.path.join(os.getcwd(), 'no_calibrar_tots', name +'.tif')
        #self.print_graph(g, ["Trimetoprim", dis], 'inputs/reference_raster.tif', "trimetoprim.tif")
        return g