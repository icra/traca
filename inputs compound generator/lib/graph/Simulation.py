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
from osgeo import gdal

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

    def print_graph(self, graph_location: str, attribute_list: list, reference_raster_location: str, output_name: str,
                    datatype=gdal.GDT_Float32):
        """
        This function takes in a graph, and converts it to a raster
        :rtype: gdal raster
        :graph_location: str: location of the raster
        :attribute_list: list: list of strings that specify the attributes to be copied
        :reference_raster_location: str: location of the reference raster
        :output_name: str: the location name that the output raster should have
        :datatype: gdal.Datatype: the datatype of the raster
        """

        print(str)
        if isinstance(graph_location, str):
            open_graph = open(graph_location, "rb")
            graph = pickle.load(open_graph)
            open_graph.close()
        elif isinstance(graph_location, networkx.DiGraph):
            graph = graph_location
        else:
            raise TypeError('Pass either a Digraph or a string. your input was ' + str(type(graph_location)))
        reference_raster = gdal.Open(reference_raster_location)
        count = len(attribute_list)
        indicator = False

        if count == 0:
            count = 1
            indicator = True

        no_data_val = -523521
        raster_matrix = numpy.zeros([reference_raster.RasterYSize, reference_raster.RasterXSize, count]) + no_data_val
        iterations_number = 0
        tot = graph.number_of_nodes()
        if indicator:
            for node_id in graph:
                [i, j] = [graph.nodes[node_id]["x"], graph.nodes[node_id]["y"]]
                raster_matrix[i, j] = 1
        else:
            for node_id in graph:
                iterations_number += 1
                for index in range(count):
                    [i, j] = [graph.nodes[node_id]["x"],
                              graph.nodes[node_id]["y"]]  # recover pixel coordinates from node
                    raster_matrix[i, j, index] = graph.nodes[node_id][attribute_list[index]]

        gtiff_driver = gdal.GetDriverByName('GTiff')
        if not output_name.endswith('.tif'):
            output_name += '.tif'
        out_ds = gtiff_driver.Create(output_name, reference_raster.RasterXSize, reference_raster.RasterYSize,
                                     count, datatype)  # this creates a raster document with dimensions, bands, datatype

        out_ds.SetProjection(reference_raster.GetProjection())  # copy direction projection to output raster
        out_ds.SetGeoTransform(
            reference_raster.GetGeoTransform())  # copy direction resolution/location to output raster

        for index in range(1, count + 1):
            out_ds.GetRasterBand(index).WriteArray(raster_matrix[:, :, index - 1])
            try:
                out_ds.GetRasterBand(index).SetDescription(attribute_list[index - 1])
            except IndexError:
                out_ds.GetRasterBand(index).SetDescription('indicator')

            out_ds.GetRasterBand(index).SetNoDataValue(no_data_val)
        out_ds = None
        pass

    def give_pixel(self, coord: list, reference_raster_location: object, return_scalar: bool = False, reverse: bool = False):
        """
        This function gives the pixel of a coordinate (row/latitude, column/longitude). If reverse is specified, the function returns a coordinate for a
        pixel number.
        :rtype: location of the pixel as a list or as the pixel number. If reverse is specified, gives coordenates as a list
        :coord: list: The coordinations of the point. If reverse is specified, this needs to be the pixel number
        :reference_raster: object: A raster with the desired dimensions
        :return_scalar: bool: if true, the output is returned as a scalar (pixel number)
        :reverse: bool: if true, the function takes in a pixel number and returns a coordinate
        """
        reference_raster = gdal.Open(reference_raster_location)
        transformation = reference_raster.GetGeoTransform()
        if not reverse:
            inverse_transform = gdal.InvGeoTransform(transformation)  # coordinate to pixel instructions
            long_location, lat_location = map(int, gdal.ApplyGeoTransform(inverse_transform, coord[1], coord[0]))

            if return_scalar:
                pixel_number = lat_location * reference_raster.RasterXSize + long_location
                return pixel_number
            return [lat_location, long_location]
        i = int(coord / reference_raster.RasterXSize)
        j = coord - reference_raster.RasterXSize * i
        long_location, lat_location = gdal.ApplyGeoTransform(transformation, int(j), i)
        return [lat_location, long_location]


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