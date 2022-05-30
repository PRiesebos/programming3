import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

data = pd.read_csv('timings.txt', sep="", header=None)
y = data.blabla
x = np.linspace(1, 17)
plt.plot(x, y)

# load timings.txt
# export timings.png
