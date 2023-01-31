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

#Por cada archivo del listado de archivos xlsx
for f in files_xls:
    #Leemos los excels
    totalData = pandas.read_excel(f)
    #Adjuntamos los diferentes archivos (f)
    dataFrame = dataFrame.append(totalData)

    #No limitamos el numero de filas / columnas y rellenamos los campos NaN con No disponible
    with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
        dataFrame.fillna("No disponible", inplace=True)

    #Read csv file / Leer el archivo csv.
        readCsv = pandas.read_csv(r'C:\Users\murrutia\Documents\Proyecto\SampleData.csv',index_col=False, delimiter= ',')
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
                cursor.execute("CREATE TABLE cliente_data(categoria VARCHAR(255), medio VARCHAR(255), nombre VARCHAR(255), cargo VARCHAR (255), relevancia_rs VARCHAR (255), relevancia_dm VARCHAR (255), mail VARCHAR (255), telefono VARCHAR (255));")
                print("Tabla creada....")

                #Hacemos un bucle con la iteracion del csv
                for i,row in readCsv.iterrows():
                    # %s representa el valor de cada columna del csv en la consulta.
                    sql = "INSERT INTO cliente.cliente_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"
                    cursor.execute(sql, tuple(row))
                    print("Registro insertado....")

                    conn.commit()
        #Generamos la excepcion en caso de no tener conexion
        except Error as e:
                    print("Ha ocurrido un error inseperado con MySQL: ", e)


