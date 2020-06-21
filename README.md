# Thesis
Master thesis repository.

Requirements:

- [Influxdb](https://docs.influxdata.com/influxdb/v1.8/introduction/install/)
- The whole process was run in Windows OS.

Python packages used:

- ast
- datetime
- functools
- gzip
- influxdb
- json
- matplotlib
- numpy
- os
- pandas
- threading
- time
- random
- requests
- sklearn
- sys
- tensorboard
- tensorflow-gpu
- websocket
- zlib

Following the structure of the designed architecture:

<p align="center">
  <img src="https://github.com/DanielMCM/Thesis/blob/master/Image/Process_diagram.png" />
</p>

- Steps 1+2 are included in folders ./API and ./Database, main files for launch are ./Database/Main.py and ./Database/Functions/Loaddb.py.
- Steps 3+4+5 are included in ./Data_Processing/03_Dataset_Creation.ipynb
- Step 6 is included in ./Data_Processing/06_Data_Overview.ipynb
- Step 7 is included in ./Data_Processing/07_Models.ipynb

Base code for steps 1+2 was obtained from https://github.com/sammchardy/python-binance, it was adapted for the rest of the exchanges.

