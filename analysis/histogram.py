
##### Stata script to create a histgoram for time since #####
##### last COVID, flu, or either vaccine before index date ##
##### converted to Python ###################################

import pandas as pd

data = pd.read_csv("output/dataset.csv.gz")

fig = data.age.plot.hist().get_figure()
fig.savefig("output/Fig2.png")

## Runs in Python but no error is no data to plot - dataset 
## definition removing all patients from dummy data?