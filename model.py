
from datetime import date
from typing import Any, Dict, List
import pandas as pd  # type: ignore
import numpy as np
import neworder
import synthpop_pb2
import fastparquet

from assumptions import Assumptions
from population import Population


class PopModel(neworder.Model):

    population: pd.DataFrame
    birth_rate: pd.DataFrame
    female_mortality_rate: pd.DataFrame
    male_mortality_rate: pd.DataFrame
    mortality_rate: pd.DataFrame
    randy: Any
    populationPyramids: List[pd.DataFrame] = []
    populationPyramids_df: pd.DataFrame
    ageBands: List[str]
    ageBins: List[int]

    output_file_path: str

    def __init__(self, assumptions_file_path: str, population_file_path: str, output_file_path: str):
        """Initialise the model.

        Set the timeline and random number generator.
        Load population and assumptions into the class.
        """
        timeline = neworder.CalendarTimeline(date(2021, 1, 1), date(2041, 1, 1), 1, "y")
        super().__init__(timeline, neworder.MonteCarlo.deterministic_identical_stream)

        self.ageBands = [
            "A00-04", "A05-09", "A10-14", "A15-19", "A20-24", "A25-29", "A30-34",
            "A35-39", "A40-44", "A45-49", "A50-54", "A55-59", "A60-64", "A65-69",
            "A70-74", "A75-79", "A80-84", "A85-89", "A90-94", "A95+"]
        
        bins = [x for x in range(0, 100, 5)]
        bins.append(1000)
        self.ageBins = bins
        
        self.output_file_path = output_file_path

        population = Population(population_file_path)
        self.population = population.population_df.set_index(["sex"])

        assumptions = Assumptions(assumptions_file_path)

        ageYears = pd.Series(range(101), dtype=int, name="ageYears")
        birthRate = pd.Series(data=assumptions.birth_rate, dtype=np.float32, name="birthRate")
        self.birth_rate = pd.concat([ageYears, birthRate], axis=1).set_index("ageYears")

        femaleMortalityRate = pd.Series(data=assumptions.female_mortality, dtype=np.float32, name="mortalityRate")
        self.female_mortality_rate = (pd.concat([ageYears, femaleMortalityRate], axis=1)
        )
        self.female_mortality_rate["sex"] = synthpop_pb2.FEMALE # type: ignore

        maleMortalityRate = pd.Series(data=assumptions.male_mortality, dtype=np.float32, name="mortalityRate")
        self.male_mortality_rate = (pd.concat([ageYears, maleMortalityRate], axis=1)
        )
        self.male_mortality_rate["sex"] = synthpop_pb2.MALE # type: ignore

        self.mortality_rate = (pd.concat([
                                self.female_mortality_rate,
                                self.male_mortality_rate
                                ]).set_index(["sex", "ageYears"])
        )
    
        self.randy = np.random.default_rng(10000000000)

    def step(self) -> None:
        self.births()
        self.deaths()
        self.age()
        self.statistics()

    def finalise(self) -> None:
        self.populationPyramids_df = pd.concat(self.populationPyramids)
        fastparquet.write(self.output_file_path, self.populationPyramids_df, compression='GZIP')

    def age(self) -> None:
        """Increment age by one year
        """
        self.population.ageYears = self.population.ageYears + 1 

    def births(self) -> None:
        """Iterate over each female and apply birth rate by age.
        """        
        # First consider only females
        females = (self.population[self.population.index
                                    .get_level_values("sex")
                                    .isin([synthpop_pb2.FEMALE]) # type: ignore
                                ]
                                .filter(["ageYears", "ethnicity"], axis=1)
                                .copy()
        )
        females = females.join(self.birth_rate, on="ageYears", how="left")
        females["rnd"] = self.randy.random(females.shape[0])
        mothers: pd.DataFrame = females.query("rnd < birthRate")
        newborns: List[Dict[str, Any]] = []
        for index, mother in mothers.iterrows():
            newborn: Dict[str, Any] = {
                "ageYears": 0,
                "ethnicity": mother.ethnicity,
                "sex": synthpop_pb2.MALE if self.randy.random() < 0.5 else synthpop_pb2.FEMALE, # type: ignore
                "lifeSatisfaction": synthpop_pb2.MEDIUM, # type: ignore
                "hasCardiovascularDisease": False,
                "hasDiabetes": False,
                "hasHighBloodPressure": False,
                "bmi": 10,
            }
            newborns.append(newborn)

        newborns_df = pd.DataFrame(newborns).set_index(["sex"])

        self.population = pd.concat((self.population, newborns_df))

    def deaths(self) -> None:
        """Iterate over every person and apply mortality rate.
        """        
        self.population = self.population.join(self.mortality_rate, on=["sex", "ageYears"], how="left")
        self.population["rnd"] = self.randy.random(self.population.shape[0])
        self.population = self.population.query("mortalityRate < rnd")
        self.population = self.population.drop(["mortalityRate", "rnd"], axis="columns", errors="ignore")

    def statistics(self) -> None:
        pop5yr = self.population[["ageYears"]].copy().reset_index()

        pop5yr["ageBand"] = pd.cut(x=pop5yr["ageYears"], bins=self.ageBins, labels=self.ageBands)
        
        pop5yr = (pop5yr
                  .groupby(by=["sex", "ageBand"], as_index=False, observed=True)
                  .count()
                  .rename(columns=({"ageYears": "persons"}))
        )
        pop5yr["year"] = self.timeline.time.year # type: ignore
        self.populationPyramids.append(pop5yr)
