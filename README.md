# Microsimulation with Python

This code implements a simple microsimulation using Python and the neworder microsimulation framework (<https://github.com/virgesmith/neworder>).

The microsimulation models 20 years of births and deaths within a given population.

The model can be run from the Run.ipynb Jupyter notebook within which three paths need to be set:

  population "path to synthetic population file in protobuf format from UATK-CPS."

  assumptions "path to a Parquet file containing assumptions."

  output "path to parquet file were results will be stored."

The synthetic population can be downloaded from (the file will need unpacking to get the protobuf file):
<https://alan-turing-institute.github.io/uatk-spc/using_england_outputs.html>

The assumptions are included in the Data folder.
