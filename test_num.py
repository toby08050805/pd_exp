
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon




from matplotlib import path
import matplotlib.pyplot as plt
import numpy as np


xv,yv = np.meshgrid(np.linspace(-3,3,6),np.linspace(-3,3,6))
print(xv)
print(xv.flatten())
print(xv.flatten()[:,np.newaxis])
