#importar las librerias
from fastapi import FastAPI   
import pandas as pd  
import gzip 

''' 
Instanciamos la clase
FastAPI es una clase de Python que provee toda la funcionalidad para tu API
'''
app = FastAPI()

'''
Cargamos los datos con los que se trabajaran
'''
def function_df(ruta):
    # Descomprimir el archivo JSON usando gzip y cargar los datos en un DataFrame
    with gzip.open(ruta, "rb") as archivo_comprimido:
    # Cargar los datos JSON
        df = pd.read_json(archivo_comprimido, lines=True)
    return df 

df_steam_games = function_df('./Datasets/clean_steam_games.json.gz')
df_user_reviews = function_df('./Datasets/clean_user_reviews.json.gz')
df_user_items = function_df('./Datasets/clean_user_items.json.gz')

# Convierte la columna 'release_date' a tipo datetime
df_steam_games['release_date'] = pd.to_datetime(df_steam_games['release_date'], errors='coerce')

# Elimina los registros con NaT en la columna 'release_date'
df_steam_games = df_steam_games.dropna(subset=['release_date'])

''' 
Escribimos el decorador de path para cada funcion
Este decorador le dice a FastAPI que la funcion que esta por debajo
corresponde al path / con una operacion GET
Usamos async para que la llamada al servidor sea asincrona
De esta forma pueda ejecutar otras tareas en vez de tener que estar esperando a que se devuelva una respuesta
'''
@app.get("/A")
async def developer( desarrollador : str ):
    '''
    Calcula cantidad de items y porcentaje de contenido Free por año según empresa desarrolladora

    Parametros
    ----------
    desarrollador : str

    Retorno
    -------
    Año, Cantidad de items, porcentaje contenido free

    '''
    #Transformar la columna "developer" y "price" en str en letra minuscula
    df_steam_games['developer'] = df_steam_games['developer'].astype(str).str.lower()
    df_steam_games['price'] = df_steam_games['price'].astype(str).str.lower()
    desarrollador.lower()

    #Filtrar por desarrollador
    developer_df = df_steam_games[df_steam_games['developer'].str.contains(desarrollador, case=False)]

    if developer_df.empty:
        print(f"No hay registros para el desarrollador {desarrollador}.")
        return None

    estadisticas_desarrolladores = []

    for developer, juegos in developer_df.groupby('developer'):
        total_juegos = len(juegos)
        juegos_free = juegos[juegos['price'].str.contains('free', case=False)]
        total_juegos_free = len(juegos_free)

        porcentaje_free = (total_juegos_free / total_juegos) * 100 if total_juegos > 0 else 0

        estadisticas = {
            'desarrollador': developer,
            'total_juegos': total_juegos,
            'porcentaje_free': porcentaje_free
        }

        estadisticas_desarrolladores.append(estadisticas)

    return estadisticas_desarrolladores


@app.get("/B")
async def userdata( User_id : str ):
    '''
    Calcula cantidad de dinero gastado por el usuario, el porcentaje de recomendación en base a reviews.recommend y cantidad de items

    Parametros
    ----------
    User_id : str

    Retorno
    -------
    {"Usuario X" : nombre de usuario, "Dinero gastado": x USD, "% de recomendación": x%, "cantidad de items": x}

    '''
    # Unir los dataframes usando 'item_id' como clave
    df_merged = pd.merge(df_steam_games, df_user_reviews, on='item_id')

    # Filtrar juegos comprados por el usuario
    juegos_comprados = df_merged[df_merged['user_id'] == User_id].copy()

    # Convertir la columna 'price' a tipo cadena
    juegos_comprados['price'] = juegos_comprados['price'].astype(str)
    # Reemplazar "free" o "Free" en la columna 'price' por 0
    juegos_comprados.loc[juegos_comprados['price'].str.contains(r'(?i)\bfree\b'), 'price'] = 0
    # Convertir la columna 'price' a tipo float para poder sumar los valores
    juegos_comprados['price'] = juegos_comprados['price'].astype(float)
    
    # Convertir la columna 'price' a tipo numérico
    juegos_comprados['price'] = pd.to_numeric(juegos_comprados['price'], errors='coerce')

    # Calcular dinero gastado por el usuario
    dinero_gastado = round(juegos_comprados['price'].sum(), 2)

    # Calcular el porcentaje de recomendación
    total_reviews = juegos_comprados.shape[0]
    reviews_recomendadas = juegos_comprados[juegos_comprados['recommend'] == True].shape[0]
    porcentaje_recomendacion = (reviews_recomendadas / total_reviews) * 100 if total_reviews > 0 else 0

    # Calcular cantidad de items
    cantidad_items = juegos_comprados.shape[0]

    return {
        "Usuario": User_id,
        "Dinero gastado": f"{dinero_gastado} USD",
        "% de recomendación": f"{porcentaje_recomendacion}%",
        "Cantidad de items": cantidad_items
    }


@app.get("/C")
async def UserForGenre( genero : str ):
    '''
    Devuelve el usuario que acumula más horas jugadas para el género dado y una lista de la acumulación de horas jugadas por año de lanzamiento.

    Parametros
    ----------
    genero : str 

    Retorno
    -------
    {"Usuario con más horas jugadas para Género X" : nombre de usuario, "Horas jugadas":[{Año: x1, Horas: n}, {Año: x2, Horas: n2}, {Año: x3, Horas: n3}]}

    '''
    genero.lower()
    df_steam_games['genres'] = df_steam_games['genres'].astype(str).str.lower()

    # Convierte la columna 'release_date' a tipo datetime
    df_steam_games['release_date'] = pd.to_datetime(df_steam_games['release_date'], errors='coerce')

    # Filtrar el DataFrame df_steam_games por el género dado
    df_filtered = df_steam_games[df_steam_games['genres'].str.contains(genero, na=False)].copy()
        
    # Extraer el año de lanzamiento
    df_filtered['release_year'] = df_filtered['release_date'].dt.year
        
    # Combinar los DataFrames utilizando 'item_id'
    df_combined = pd.merge(df_user_items, df_filtered, on='item_id', how='inner')
        
    # Obtener el usuario con más horas jugadas para el género dado
    usuario_max_horas = df_combined.groupby('user_id')['playtime_forever'].sum().idxmax()
        
    # Obtener las horas jugadas por el usuario con más horas jugadas\n",
    horas_usuario_max = df_combined[df_combined['user_id'] == usuario_max_horas]['playtime_forever'].sum()
        
    #renombrar columnas
    df_combined = df_combined.rename(columns={'release_year': 'Año', 'playtime_forever': 'Horas Jugadas'})
        
    # Agrupar por año de lanzamiento y sumar las horas jugadas\n",
    horas_por_año = df_combined.groupby('Año')['Horas Jugadas'].sum().reset_index()
        
    return {
        "Usuario con más horas jugadas para " + genero: usuario_max_horas,
        "Horas jugadas": horas_por_año.to_dict(orient='records'),
        "Horas totales jugadas por el usuario": horas_usuario_max
    }


@app.get("/D")
async def best_developer_year( año : int ):
    '''
    Devuelve el top 3 de desarrolladores con juegos MÁS recomendados por usuarios para el año dado. (reviews.recommend = True y comentarios positivos)

    Parametros
    ----------
    año : int 

    Retorno
    -------
    [{"Puesto x1" : X}, {"Puesto x2" : Y},{"Puesto x3" : Z}]

    '''
    # Convierte la columna 'release_date' a tipo datetime
    df_steam_games['release_date'] = pd.to_datetime(df_steam_games['release_date'], errors='coerce')

    # Unir los DataFrames en función de la columna 'item_id'
    df_completo = pd.merge(df_steam_games, df_user_reviews, on='item_id', how='inner')

    # Filtrar los juegos por el año especificado
    juegos_del_año = df_completo[df_completo['release_date'].dt.year == año]

    # Filtrar los juegos recomendados y los comentarios positivos
    juegos_recomendados = juegos_del_año[juegos_del_año['recommend'] == True]
    comentarios_positivos = juegos_del_año[juegos_del_año['sentiment_analysis'] == 2]

    # Calcular la suma de recomendaciones y comentarios positivos por desarrollador
    recomendaciones_por_desarrollador = juegos_recomendados.groupby('developer').size()
    comentarios_por_desarrollador = comentarios_positivos.groupby('developer').size()

    # Calcular la puntuación total por desarrollador sumando recomendaciones y comentarios positivos
    puntuacion_por_desarrollador = recomendaciones_por_desarrollador.add(comentarios_por_desarrollador, fill_value=0)

    # Seleccionar los tres primeros desarrolladores con la puntuación más alta
    top_3_desarrolladores = puntuacion_por_desarrollador.nlargest(3)

    # Crear la lista de retorno en el formato especificado
    retorno = [{"Puesto {}: {}".format(i+1, desarrollador): puntuacion} for i, (desarrollador, puntuacion) in enumerate(top_3_desarrolladores.items())]

    if top_3_desarrolladores.empty:
        print(f"No hay registros para el año {año}.")
        return None
    
    return retorno


@app.get("/E")
async def developer_reviews_analysis( desarrolladora : str ): 
    '''
    Según el desarrollador, se devuelve un diccionario con el nombre del desarrollador como llave y una lista con la cantidad total de registros de reseñas de usuarios que se encuentren categorizados con un análisis de sentimiento como valor positivo o negativo.

    Parametros
    ----------
    desarrolladora : str 

    Retorno
    -------
    {'desarrollador x' : [Negative = x1, Positive = x2]}

    '''
    desarrolladora.lower()
    df_steam_games['developer'] = df_steam_games['developer'].astype(str).str.lower()

    # Fusionar los DataFrames por item_id
    merged_df = pd.merge(df_steam_games, df_user_reviews, how='inner', on='item_id')

    # Filtrar por el desarrollador buscado
    desarrolladora_df = merged_df[merged_df['developer'].str.contains(desarrolladora, case=False)]

    if desarrolladora_df.empty:
        print(f"No hay registros para el desarrollador {desarrolladora}.")
        return None

    #contar la cantidad de reseñas
    for developer in desarrolladora_df.groupby('developer'):
        cantidad_positivas = (desarrolladora_df['sentiment_analysis'] == 2).sum()
        cantidad_negativas = (desarrolladora_df['sentiment_analysis'] == 0).sum()

        resultado = {
            'positivas': cantidad_positivas,
            'negativas': cantidad_negativas
        }

    return {desarrolladora: [f"{key} = {value}" for key, value in resultado.items()]}
