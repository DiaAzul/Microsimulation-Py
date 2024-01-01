"""Example microsimulation

The model can be run either using this file or the Jupyter notebook.
"""

from model import PopModel
import neworder

assumptions_file_path: str = r"./Data/assumptions.parquet"
population_file_path: str =r"C:/workspace/Data/Population 2020/bristol.pb/bristol.pb"
output_file_path: str = r"./Output/populationPyramids.parquet"

model = PopModel(assumptions_file_path, population_file_path, output_file_path)

print(model.population.count())
neworder.run(model)

print(model.population.count())
print("end")
