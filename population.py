from typing import Any, Dict, List
import synthpop_pb2
import pandas as pd


class Population:
    """Class holding imported population data."""

    protobuf_population: Any
    population_df: pd.DataFrame

    def __init__(self, filePath: str):
        """Initialise population data.

        Population data is held in protobuf file format.

        Args:
            filePath (str): Path to population data file.
        """
        self.protobuf_population = synthpop_pb2.Population()      

        with open(filePath, "rb") as f:
            self.protobuf_population.ParseFromString(f.read())

        newPopulation: List[Dict[str, Any]] = []
        for person in self.protobuf_population.people:
            newPerson: Dict[str, Any] = {
                "ageYears": person.demographics.age_years,
                "ethnicity": person.demographics.ethnicity,
                "sex": person.demographics.sex,
                "lifeSatisfaction": person.health.life_satisfaction,
                "hasCardiovascularDisease": person.health.has_cardiovascular_disease,
                "hasDiabetes": person.health.has_diabetes,
                "hasHighBloodPressure": person.health.has_high_blood_pressure,
                "bmi": person.health.bmi,
            }
            newPopulation.append(newPerson)

        self.population_df = pd.DataFrame(newPopulation).astype({
                "ageYears": "int",
                "ethnicity": "category",
                "sex": "category",
                "bmi": "float",
        }).set_index("sex", append=True, drop=False)
