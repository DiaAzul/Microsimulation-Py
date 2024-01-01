import pandas as pd
import numpy as np
import numpy.typing as npt
import fastparquet


class Assumptions:
    """Class holding model assumptions
    """    

    birth_rate: npt.NDArray[np.float64]
    female_mortality: npt.NDArray[np.float64]
    male_mortality: npt.NDArray[np.float64]

    def __init__(self, file_path: str):
        """Initialise model from parquet file.

        Args:
            file_path (str): Path to parquet file holding assumptions.
        """        
        pq = fastparquet.ParquetFile(file_path)
        df: pd.DataFrame = pq.to_pandas()

        self.birth_rate = df["Birth Rate - Female"].to_numpy()
        self.female_mortality = df["Mortality Rate - Female"].to_numpy()
        self.male_mortality = df["Mortality Rate - Male"].to_numpy()
