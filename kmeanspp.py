import numpy as np
from numpy import linalg as LA
import sys
import os
import warnings
from numbers import Integral

class KMeansPlusPlus:

    def __init__(self, data_frame, k, columns=None, max_iterations=None):
        if max_iterations is not None and max_iterations <= 0:
            raise Exception("max_iterations must be positive!")

        if not isinstance(k, Integral) or k <= 0:
            raise Exception("The value of k must be a positive integer")

        self.data_frame = data_frame  # m x n
        self.numRows = data_frame.shape[0]  # m

        # k x n, the i,j entry being the jth coordinate of center i
        self.centers = None

        # m x k , the i,j entry represents the distance
        # from point i to center j
        # (where i and j start at 0)
        self.distance_matrix = None

        # Series of length m, consisting of integers 0,1,...,k-1
        self.clusters = None

        # To keep track of clusters in the previous iteration
        self.previous_clusters = None

        self.max_iterations = max_iterations

        self.k = k

        if columns is None:
            self.columns = range(data_frame.shape[1])
        else:
            self.columns = columns

    def _populate_initial_centers(self):
        rows = []
        rows.append(self._grab_random_point())
        distances = None

        while len(rows) < self.k:
            if distances is None:
                distances = (np.arccos(-self._distances_from_point(rows[0])))**2
            else:
                distances = (np.arccos(-(self._distances_from_point_list(rows))))**2

            normalized_distances = distances / distances.sum()
            print('normalized_distances')
            print(normalized_distances)
            #normalized_distances.sort()
            dice_roll = np.random.rand()
            index = np.where(normalized_distances.cumsum() >= dice_roll)[0][0] # should be < if you use negative
            print('index=')
            print(index)
            rows.append(self.data_frame[index,self.columns])

        self.centers = rows # 1 list contains k arrays
        print('centers=')
        print(self.centers)

    def _compute_distances(self):
        if self.centers is None:
            raise Exception(
                "Must populate centers before distances can be calculated!")

        column_dict = np.zeros((self.numRows, self.k)) # m x k

        for i in list(range(self.k)):
            column_dict[:,i] = self._distances_from_point(self.centers[i])

        self.distance_matrix = column_dict
        print('distance_matrix=')
        print(self.distance_matrix)

    def _get_clusters(self):
        if self.distance_matrix is None:
            raise Exception(
                "Must compute distances before closest centers can be calculated")

        #min_distances = self.distance_matrix.min(axis=1)

        # We need to make sure the index
        self.clusters = np.argmin(self.distance_matrix, axis=1)
        print('clusters=')
        print(self.clusters)

    def _compute_new_centers(self):
        if self.centers is None:
            raise Exception("Centers not initialized!")

        if self.clusters is None:
            raise Exception("Clusters not computed!")

        for i in list(range(self.k)):
            temp = self.data_frame[self.clusters==i, :]
            temp = temp[:, self.columns] # maybe len(temp)==0
            self.centers[i] = temp.mean(axis=0)
            print('new centers=')
            print(self.centers)

    def cluster(self):

        self._populate_initial_centers()
        self._compute_distances()
        self._get_clusters()

        counter = 0

        while True:
            counter += 1

            self.previous_clusters = self.clusters.copy()

            self._compute_new_centers()
            self._compute_distances()
            self._get_clusters()

            if self.max_iterations is not None and counter >= self.max_iterations:
                break
            elif all(self.clusters == self.previous_clusters):
                break


    def _distances_from_point(self, point):

        # cos distance
        norm = LA.norm(self.data_frame,axis=1)*(LA.norm(point))
        cos_distance = -np.dot(self.data_frame[:,self.columns], point)/norm
        cos_distance[norm==0] = 0
        return cos_distance
        # L2 distance
        # return np.power(self.data_frame[:,self.columns] - point, 2).sum(axis=1)

    def _distances_from_point_list(self, point_list):
        result = None

        for point in point_list:
            if result is None:
                result = self._distances_from_point(point)
            else:
                result = np.column_stack((result, self._distances_from_point(point))).min(axis=1)

        return result

    def _grab_random_point(self):
        index = np.random.random_integers(0, self.numRows - 1)
        # NumPy array
        return self.data_frame[index,self.columns]

if __name__ == '__main__':
    np.random.seed(1234)  # For reproducibility
# We create a data set with three sets of 500 points each chosen from a normal distrubut
# The means for the distributions from which we sample are:
# (25,45), (-30,5), and (5,-20)
    x0 = 10 * np.random.randn(500) + 25
    y0 = 10 * np.random.randn(500) + 45
    x1 = 10 * np.random.randn(500) - 30
    y1 = 10 * np.random.randn(500) + 5
    x2 = 10 * np.random.randn(500) + 5
    y2 = 10 * np.random.randn(500) - 20

    x = np.concatenate((x0,x1,x2))
    y = np.concatenate((y0,y1,y2))
#x = np.array([1,3,4])
#y = np.array([2,5,5])
    data = np.column_stack((x,y))
# Grab a scatterplot
#import matplotlib.pyplot as plt
#plt.scatter(x, y, s=5)

# Cluster
    kmpp = KMeansPlusPlus(data, 3, max_iterations=1)
    kmpp.cluster()
    cls = kmpp.clusters
    print(cls)

