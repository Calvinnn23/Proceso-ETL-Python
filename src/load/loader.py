class Loader:
    def __init__(self, destination: str):
        self.destination = destination

    # Metodo carga
    def load_to_csv(self, data: dict):
        import os

        if not os.path.exists(self.destination):
            os.makedirs(self.destination)
        for sheet, df in data.items():
            try:
                file_path = f"{self.destination}/{sheet}.csv"
                # Si el df es de Pandas
                if hasattr(df, "to_csv"):
                    df.to_csv(file_path, index=False)
                else:
                    # Si es un Spark df
                    df.coalesce(1).write.csv(file_path, header=True, mode="overwrite")
                print(f"Datos de {sheet} cargados en {file_path}")
            except Exception as e:
                print(f"Error cargando datos de {sheet}: {e}")
                raise
