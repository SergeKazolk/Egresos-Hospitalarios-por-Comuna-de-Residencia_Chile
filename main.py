from etl_pipeline import (
    parse_arguments, extract_year_from_path,
    data_already_exists, load_data, preprocess_data,
    create_db_engine, save_to_database
)


def main():
    # 1. Leer ruta desde argumentos
    file_path = parse_arguments()

    # 2. Extraer año
    year = extract_year_from_path(file_path)

    # 3. Crear conexión
    engine = create_db_engine("db/mi_base")

    # 4. Validar si ya existen datos
    if data_already_exists(engine, "mi_tabla", year):
        print(f"Los datos del año {year} ya existen en la base de datos.")
        return

    # 5. Cargar y procesar
    df = load_data(file_path)
    df_clean = preprocess_data(df, engine)

    # 6. Guardar en la base
    save_to_database(df_clean, engine, "mi_tabla")

    print("✅ ETL finalizado con éxito")


if __name__ == "__main__":
    main()
