import argparse
import re
import pandas as pd
from sqlalchemy import create_engine


def parse_arguments():
    """
    Analiza los argumentos de la línea de comandos para la ruta del archivo.
    """
    parser = argparse.ArgumentParser(description="Procesa un archivo CSV")
    parser.add_argument("file_path", type=str, help="Ruta al archivo CSV")
    args = parser.parse_args()
    return args.file_path


def extract_year_from_path(file_path):
    """
    Extrae el año del nombre de la ruta.
    """
    match = re.search(r"\b(19|20)\d{2}\b", file_path)
    if match:
        return int(match.group(0))
    else:
        raise ValueError("No se encontró un año en la ruta del archivo")


def data_already_exists(engine, table_name, year):
    """
    Verifica si ya existen registros para un año en la tabla.
    """
    query = f"SELECT COUNT(*) AS total FROM {table_name} WHERE year = {year}"
    try:
        result = pd.read_sql(query, engine)
        return result.iloc[0, 0] > 0
    except Exception:
        # si la tabla no existe, asumimos que no hay datos
        return False


def load_data(file_path):
    """
    Carga los datos en un DataFrame.
    """
    df = pd.read_csv(file_path)
    return df


def preprocess_data(df, engine, threshold=0.5):
    """
    Limpia y estandariza el DataFrame.
    """
    # Número de columnas
    num_columns = df.shape[1]

    # Número de '*' permitidos
    allowed_stars = int(num_columns * threshold)

    # Filtrar filas con exceso de '*'
    cleaned_df = df[df.apply(lambda x: (x == '*').sum()
                             <= allowed_stars, axis=1)]

    # Ejemplo de conversión de columnas a entero (ajusta según tus datos)
    for col in cleaned_df.columns:
        if "id" in col.lower() or "year" in col.lower():
            cleaned_df[col] = pd.to_numeric(
                cleaned_df[col], errors="coerce").fillna(0).astype(int)

    # Renombrar columnas (ajusta según tu CSV)
    new_names = [f"col_{i}" for i in range(len(cleaned_df.columns))]
    cleaned_df.columns = new_names

    return cleaned_df


def create_db_engine(db_name):
    """
    Crea una conexión SQLite con SQLAlchemy.
    """
    connection_string = f"sqlite:///{db_name}.db"
    engine = create_engine(connection_string)
    print(f"Conexión a {db_name}.db creada con éxito")
    return engine


def save_to_database(df, engine, table_name):
    """
    Guarda el DataFrame en la base de datos.
    """
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"Datos guardados en la tabla {table_name}")
