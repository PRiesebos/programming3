import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv(
    "/homes/pcriesebos/Documents/programming3/Assignment3/output/timings.txt",
    sep="\t",
    lineterminator="\n",
    header=None,
)
x = data.iloc[:, 0]
y = data.iloc[:, 1]
plt.scatter(x, y)
plt.plot(x, y)
plt.title("Blastp runtime with variating thread count")
plt.xlabel("Number of threads")
plt.ylabel("Time taken (s)")
plt.savefig("output/timings.png")
