miConexion = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=DESKTOP-28IM258\SQLEXPRESS; DATABASE=JohnDeere;Trusted_Connection=yes')

miCursor = miConexion.cursor()








miConexion.commit()
miCursor.close()
miConexion.close()