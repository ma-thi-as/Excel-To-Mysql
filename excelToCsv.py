import mysql.connector as msql
from mysql.connector import Error
import pandas, os


#Obtenemos la ruta del proyecto
ruta = os.getcwd()
#Listamos los archivos de la carpeta
files = os.listdir(ruta)
#Funcion lambda para filtrar los archivos en terminacion .xlsx
files_xls = [f for f in files if f.endswith('.xlsx')]
#Inicializamos el dataFrame
dataFrame = pandas.DataFrame()
#Dict encargado de remplazar los valores de las columnas por el ID de Categoria en la base de datos

categorias = {'Diario':1,'Online':2,'Televisión':3,'Radio':4,'Web':5,'Revista':6,'Blog':7,'N/A':8, 'Televisión digital': 9}
#Por cada archivo del listado de archivos xlsx
for f in files_xls:
    #Leemos los excels
    totalData = pandas.read_excel(f,index_col=None)
    #Filtramos las columnas requeridas
    totalData = totalData.loc[:, ['Categoría', 'Nombre', 'Cargo','Mail', 'Teléfono','Medio']]






    #Adjuntamos los diferentes archivos (f)
    dataFrame = dataFrame.append(totalData)
    #Rellenamos campos NaN de Categoría con el valor del dict categorias en la posicion ['N/A']
    dataFrame['Categoría'].fillna(value = categorias['N/A'], inplace=True)
    dataFrame.fillna("No disponible", inplace=True)
    #Remplazamos el valor de Categoría por el diccionario categorias
    dataFrame.replace({"Categoría": categorias}, inplace=True)
    #Exportamos el dataFrame a csv.
    dataFrame.to_csv('SampleData.csv', index = False)

    #No limitamos el numero de filas / columnas y rellenamos los campos NaN con No disponible
    with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
    #Read csv file / Leer el archivo csv.
        readCsv = pandas.read_csv(f'{ruta}/SampleData.csv',index_col=False, delimiter= ',')
        #Remplazo de emails en columna Cargo / Replacement of mail account in column Cargo
        readCsv.loc[readCsv['Cargo'].str.contains("@", case=False), 'Cargo'] = 'No disponible'

        readCsv.head()

        #Intentamos generar la conexion a la base de datos MySQL
        try:
            conn = msql.connect(host='localhost', database='cliente', user='root', password='Extend123')
            #Si conseguimos la conexion seleccionamos la base de datos
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("select database();")
                nombreBD = cursor.fetchone()
                print("Estas conectado a la base de datos: ", nombreBD)
                #Si ya existe la dropeamos y la creamos de nuevo
                cursor.execute('DROP TABLE IF EXISTS cliente_data;')
                print('Creando tabla....')

                #Generamos la query con la creacion de la tabla

                # (**Cambiar categoria a FK, elminar relevancias**)
                
                cursor.execute("CREATE TABLE cliente_data(categoriaId int ,FOREIGN KEY (categoriaId) REFERENCES categoria (ID),  nombre VARCHAR(255), cargo VARCHAR (255), mail VARCHAR (255), telefono VARCHAR (255),medio VARCHAR (255)) ;")
                print("Tabla creada....")

                #Bucle que itera por cada fila del csv e insertamos en la tabla cliente_data
                for i,row in readCsv.iterrows():
                    # %s representa el valor de cada columna del csv en la consulta.
                    sql = "INSERT INTO cliente.cliente_data VALUES (%s,%s,%s,%s,%s,%s);"
                    cursor.execute(sql, tuple(row))
                    conn.commit()
                    print("Registro insertado.... 👌")
                
        #Generamos la excepcion en caso de no tener conexion o cualquier error
        except Error as e:
                    print("Ha ocurrido un error inseperado con MySQL: ", e)

