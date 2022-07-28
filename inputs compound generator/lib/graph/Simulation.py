import os.path
import time
import pandas
import numpy
numpy.seterr(all='raise')
import networkx
import math

class Simulation:
    def __init__(self, graph_location, river_attenuation):
        self.attenuation_df = pandas.read_csv(river_attenuation).set_index('Nom')


        open_file = open(graph_location, "rb")
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

            networkx.set_node_attributes(g, 0, name=contaminant)

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

                    """
                    if n == 117433900 and contaminant == 'Tetracloroetilè':
                        print(g.nodes[n][contaminant] / g.nodes[n][dis])
                    """

                else:
                    g.nodes[n][contaminant] = 0


                # Add the contamination of the parent cells
                parents = list(g.predecessors(n))
                for k in parents:
                    """
                    if k == 117433900:
                        print(n)
                    """

                    g.nodes[n][contaminant] += g.nodes[k][contaminant]

                g.nodes[n][contaminant] *= math.exp(-attenuation * g.nodes[n][RT])

                #self.river_graph.nodes[n][rel_cont] = self.river_graph.nodes[n][contam] / self.river_graph.nodes[n][dis]


        #self.print_graph(g, ["pixel_number"], "inputs/reference_raster.tif", "Venlafaxina.tif")


        return g