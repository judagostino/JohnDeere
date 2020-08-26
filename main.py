import pyodbc
import json
import requests
from requests_oauthlib import OAuth1

#CONEXION CON LA API
url = 'https://sandboxapi.deere.com/platform/organizations'
auth = OAuth1('johndeere-JcFsHU6CV0klrsgkDHewTLuSAP1QZ2Q8Tx9sFCOs',
              '5b67d63b58a9e80facb93354b18a79eb9be3b668f02e331cace8f5a4190254fa',
               'd8d3c1b7-8f56-4cc3-96a6-0b6da898eea6',
                '19Ryi+uXOFxS3/8Q0h+pHjlGVXaYndLhTOl64Wg4cdA4/5ly0f48DxcN+nBsfFgXP71ruvCHMb4T5fYXeGzQ5uAzGYDKyfONgE0Mp6y1bbA=')
headers = {"Accept":"application/vnd.deere.axiom.v3+json"}
r = requests.get('https://sandboxapi.deere.com/platform/organizations', headers=headers, auth=auth)
data = r.json()

#CONEXION CON LA BASE DE DATOS
miConexion = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=DESKTOP-28IM258\SQLEXPRESS; DATABASE=JohnDeere;Trusted_Connection=yes')

miCursor = miConexion.cursor()
#CREACION DE LAS TABLAS
'''
miCursor.execute("CREATE TABLE Organizacion(name varchar(50),type varchar(50),member varchar(50),id varchar(50),bandera bit)")

miCursor.execute("CREATE TABLE Machine(visualizationCategory varchar(50),category varchar(MAX),make varchar(MAX),model varchar(MAX),detailMachineCode varchar(MAX),productKey varchar(MAX),engineSerialNumber varchar(MAX), telematicState varchar(MAX),capabilities varchar(MAX),terminals varchar(MAX), displays varchar(MAX),guid varchar(MAX),modelYear varchar(MAX),id varchar(MAX),vin varchar(MAX),name varchar(MAX), bandera varchar(50))")

miCursor.execute("CREATE TABLE MachineCategory(name varchar(50),id varchar(50),type varchar(50),bandera bit)")

miCursor.execute("CREATE TABLE Alert(type varchar(50),duration varchar(50),ocurrences varchar(50),engineHours varchar(50),"
                 "machineLearnTime varchar(50),bus varchar(50), definition varchar(50),id varchar(50),time varchar(50),location varchar(50),"
                 "color varchar(50),severy varchar(50),ignored varchar(50),invisible varchar(50),bandera bit)")


miCursor.execute("CREATE TABLE DeviceStateReports(time datetime,gatewayType integer,latitud varchar(MAX),longitud varchar(50),altitud varchar(50),minRSSI varchar(MAX),maxRSSI varchar(MAX),averageRSSI varchar(MAX),gpsFixTimestamp varchar(MAX),"
                         "engineState varchar(50),terminalPowerState varchar(50),cellModemState varchar(50),cellModemAntennaState varchar(50),gpsModemState varchar(50),gpsAntennaState varchar(50),"
                         "gpsError varchar(50),gpsFirmawareLevelError varchar(50),network varchar(50),rssi varchar(50),bandera bit)")


miCursor.execute("CREATE TABLE HorasMotor(value varchar(50),unit varchar(50),reportTime varchar(50),bandera bit)")

miCursor.execute("CREATE TABLE HorasOperacion(startDate datetime,endDate datetime,engineState varchar(50),bandera bit)")

miCursor.execute("CREATE TABLE HistorialUbicacion(latitud varchar(50),longitud varchar(MAX),eventTimeStamp varchar(50),gpsFixTimeStamp varchar(50),bandera bit)")

miCursor.execute("CREATE TABLE Cliente(name varchar(50),id varchar(50),bandera bit)")

miCursor.execute("CREATE TABLE Granja(name varchar(50),id varchar(50),bandera bit)")

miCursor.execute("CREATE TABLE Campo(name varchar(50),id varchar(50),bandera bit)")

miCursor.execute("CREATE TABLE Limite(name varchar(MAX),sourceType varchar(MAX),createdTime varchar(MAX),modifiedTime varchar(MAX),area varchar(MAX),workableArea varchar(MAX),extent varchar(MAX),archived varchar(MAX),id varchar(MAX),active varchar(MAX),irrigated varchar(MAX),bandera bit)")


miCursor.execute("CREATE TABLE Operacion(fieldOperationType varchar(50),cropSeason varchar(50),modifiedTime varchar(50),startDate varchar(50),endDate varchar(50),cropName varchar(50),id varchar(50),bandera bit)")

miCursor.execute("CREATE TABLE Medicion(measurementName varchar(50),measurementCategory varchar(50),area varchar(50),yield varchar(50),averageYield varchar(50),averageMoisture varchar(50),"
                         "westMass varchar(50),averageWestMass varchar(50),averageSpeed varchar(50),bandera bit)")


'''


#INSERCION AUTOMATICA DE ORGANIZACION Y MACHINE

for org in data['values']:
    print(org)
    name = str(org['name'])
    type = str(org['type'])
    member = str(org['member'])
    id = str(org['id'])
    miCursor.execute("INSERT INTO Organizacion(name,type,member,id,bandera) values(?,?,?,?,?)",name,type,member,id,0)

    for link in org['links']:
        if link['rel'] == 'machines':
            rMachine = requests.get(link['uri'], headers=headers, auth=auth)
            if rMachine.status_code == 200:
                dataMachine = rMachine.json()
                if dataMachine['total'] != 0:
                    for machine in dataMachine['values']:
                    # insert machine
                        print(machine)
                        visualizationCategory = str(machine['visualizationCategory'])
                        category = machine['category']
                        categoryId = None
                        if category :
                            miCursor.execute("INSERT INTO MachineCategory(name, id, type,bandera) values(?,?,?,?)", category['name'],category['id'],category['type'],0)
                        categoryId = category['id']
                        make = machine['make']
                        makeId = None
                        if make:
                            miCursor.execute("INSERT INTO MachineCategory(name, id, type,bandera) values(?,?,?,?)", make['name'],
                                 make['id'], make['type'],0)
                        makeId = make['id']

                        model = machine['model']
                        modelId = None
                        if model:
                            miCursor.execute("INSERT INTO MachineCategory(name, id, type,bandera) values(?,?,?,?)", model['name'],
                                 model['id'], model['type'],0)
                        modelId = model['id']
                        detailMachineCode = machine['detailMachineCode']
                        DMC = None
                        DMC = detailMachineCode['name']
                        productKey = str(machine['productKey'])
                        engineSerialNumber = str(machine['engineSerialNumber'])
                        telematicState = str(machine['telematicsState'])
                        capabilities = str(machine['capabilities'])
                        terminals = str(machine['terminals'])
                        displays = str(machine['displays'])
                        guid = str(machine['GUID'])
                        modelYear = str(machine['modelYear'])
                        id = str(machine['id'])
                        vin = str(machine['vin'])
                        name = str(machine['name'])
                        miCursor.execute(
                            "INSERT INTO Machine(visualizationCategory,category,make,model,detailMachineCode,productKey,engineSerialNumber,telematicState,capabilities,terminals,displays,guid,modelYear,vin,id,name,bandera) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            visualizationCategory, categoryId, makeId, modelId, DMC, productKey,
                            engineSerialNumber, telematicState, capabilities, terminals, displays, guid, modelYear, vin, id,
                            name,0)

                    for link in machine['links']:
                        if link['rel'] == 'alerts':
                            rAlert = requests.get(link['uri'], headers=headers, auth=auth)
                            if rAlert.status_code == 200:
                                dataAlert = rAlert.json()
                                if dataAlert['total'] != 0:
                                    for alerta in dataAlert['values']:
                                        print(alerta)
                                        type = str(alerta['type'])
                                        duration = alerta['duration']
                                        ocurrences = alerta['ocurrences']
                                        engineHours = alerta['engineHours']
                                        machineLearnTime = alerta['machineLearnTime']
                                        bus = alerta['bus']
                                        definition = alerta['definition']
                                        id = alerta['id']
                                        time = alerta['time']
                                        location = alerta['location']
                                        color = alerta['color']
                                        severy = alerta['severy']
                                        ignored = alerta['ignored']
                                        invisible = alerta['invisible']
                                        miCursor.execute(
                                            "INSERT INTO Alert(type,duration,ocurrences,engineHours,machineLearnTime,bus,"
                                            "definition,id,time,location,color,severy,ignored,invisible,bandera)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                            type, duration, ocurrences, engineHours, machineLearnTime, bus, definition, id,
                                            time, location, color, severy, ignored, invisible, 0)

                    for link in machine['links']:
                        if link['rel'] == 'deviceStateReports':
                            rReports = requests.get(link['uri'], headers=headers, auth=auth)
                            if rReports.status_code == 200:
                                dataReports = rReports.json()
                                if dataReports['total'] != 0:
                                    for informe in dataReports['values']:
                                        print(informe)
                                        time = str(informe['time'])
                                        gatewayType = str(informe['gatewayType'])
                                        location = informe['location']
                                        latitud = None
                                        longitud = None
                                        latitud = location['lat']
                                        longitud = location['lon']
                                        alt = location['altitude']
                                        Altitud = None
                                        Altitud = str(alt['valueAsDouble']) + '' + str(alt['unit'])

                                        minimoRSSI = informe['minRSSI']
                                        minRSSI = None
                                        minRSSI = str(minimoRSSI['valueAsDouble']) + '' + str(minimoRSSI['unit'])

                                        maximoRSSI = informe['maxRSSI']
                                        maxRSSI = None
                                        maxRSSI = str(maximoRSSI['valueAsDouble']) + '' + str(maximoRSSI['unit'])

                                        averRSSI = informe['averageRSSI']
                                        averageRSSI = None
                                        averageRSSI = str(averRSSI['valueAsDouble']) + '' + str(averRSSI['unit'])

                                        gpsFixTimestamp = str(informe['gpsFixTimestamp'])
                                        engineState = str(informe['engineState'])
                                        terminalPowerState = str(informe['terminalPowerState'])
                                        cellModemState = str(informe['cellModemState'])
                                        cellModemAntennaState = str(informe['cellModemAntennaState'])
                                        gpsModemState = str(informe['gpsModemState'])
                                        gpsAntennaState = str(informe['gpsAntennaState'])
                                        gpsError = str(informe['gpsError'])
                                        gpsFirmawareLevelError = str(informe['gpsFirmwareLevelError'])
                                        network = str(informe['network'])
                                        rssi = str(informe['rssi'])
                                        miCursor.execute(
                                            "INSERT INTO deviceStateReports(time,gatewayType,latitud,longitud,altitud,minRSSI,maxRSSI,averageRSSI,gpsFixTimestamp,"
                                            "engineState,terminalPowerState,cellModemState,cellModemAntennaState,gpsModemState,gpsAntennaState,"
                                            "gpsError,gpsFirmawareLevelError,network,rssi,bandera) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                            time, gatewayType, latitud, longitud, Altitud, minRSSI, maxRSSI, averageRSSI,
                                            gpsFixTimestamp, engineState, terminalPowerState, cellModemState,
                                            cellModemAntennaState,
                                            gpsModemState, gpsAntennaState, gpsError, gpsFirmawareLevelError, network, rssi, 0)

                    for link in machine['links']:
                        if link['rel'] == 'engineHours':
                            rhsMotor = requests.get(link['uri'], headers=headers, auth=auth)
                            if rhsMotor.status_code == 200:
                                datahsMotor = rhsMotor.json()
                                if datahsMotor['total'] != 0:
                                    for motor in datahsMotor['values']:
                                        print(motor)
                                        reading = motor['reading']
                                        unit = None
                                        valueReading = None
                                        if reading:
                                            valueReading = reading['valueAsDouble']
                                            unit = reading['unit']
                                            reportTime = str(motor['reportTime'])

                                        miCursor.execute(
                                            "INSERT INTO HorasMotor(value,unit,reportTime,bandera) values(?,?,?,?)",
                                            valueReading, unit, reportTime, 0)
                    for link in machine['links']:
                        if link['rel'] == 'hoursOfOperation':
                            rhsOp = requests.get(link['uri'], headers=headers, auth=auth)
                            if rhsOp.status_code == 200:
                                datahsOp = rhsOp.json()
                                if datahsOp['total'] != 0:
                                    for hsoperacion in datahsOp['values']:
                                        print(hsoperacion)
                                        startDate = hsoperacion['startDate']
                                        endDate = hsoperacion['endDate']
                                        engineState = hsoperacion['engineState']
                                        miCursor.execute(
                                            "INSERT INTO HorasOperacion(startDate,endDate,engineState,bandera) values(?,?,?,?)",
                                            startDate,endDate,engineState, 0)

                    for link in machine['links']:
                        if link['rel'] == 'locationHistory':
                            rHistorial = requests.get(link['uri'], headers=headers, auth=auth)
                            if rHistorial.status_code == 200:
                                dataHistorial = rHistorial.json()
                                if dataHistorial['total'] != 0:
                                    for historial in dataHistorial['values']:
                                        print(historial)
                                        point = historial['point']
                                        latitud = point['lat']
                                        longitud = point['lon']
                                        eventTimeStamp = str(historial['eventTimestamp'])
                                        gpsFixTimeStamp = str(historial['gpsFixTimestamp'])

                                        miCursor.execute(
                                            "INSERT INTO HistorialUbicacion(latitud,longitud,eventTimeStamp,gpsFixTimeStamp,bandera) values(?,?,?,?,?)",
                                            latitud, longitud, eventTimeStamp, gpsFixTimeStamp, 0)

    for link in org['links']:
        if link['rel'] == 'clients':
            rClient = requests.get(link['uri'], headers=headers, auth=auth)
            if rClient.status_code == 200:
                dataClient = rClient.json()
                if dataClient['total'] != 0:
                    for cliente in dataClient['values']:
                        print(cliente)
                        name = str(cliente['name'])
                        id = str(cliente['id'])
                        miCursor.execute("INSERT INTO Cliente(name,id,bandera) values(?,?,?)", name, id, 0)

    for link in org['links']:
        if link['rel'] == 'farms':
            rFarm = requests.get(link['uri'], headers=headers, auth=auth)
            if rFarm.status_code == 200:
                dataFarm = rFarm.json()
                if dataFarm['total'] != 0:
                    for granja in dataFarm['values']:
                        print(granja)
                        name = str(granja['name'])
                        id = str(granja['id'])
                        miCursor.execute("INSERT INTO Granja(name,id,bandera) values(?,?,?)", name, id, 0)

                        for link in granja['links']:
                            if link['rel'] == 'fields':
                                rCampo = requests.get(link['uri'], headers=headers, auth=auth)
                                if rCampo.status_code == 200:
                                    dataCampo = rCampo.json()
                                    if dataCampo['total'] != 0:
                                        for campo in dataCampo['values']:
                                            print(campo)
                                            name = str(campo['name'])
                                            id = str(campo['id'])
                                            miCursor.execute("INSERT INTO Campo(name,id,bandera) values(?,?,?)", name, id, 0)

                                            for link in campo['links']:
                                                if link['rel'] == 'boundaries':
                                                    rLimite = requests.get(link['uri'], headers=headers, auth=auth)
                                                    if rLimite.status_code == 200:
                                                        dataLimite = rLimite.json()
                                                        if dataLimite['total'] != 0:
                                                            for limite in dataLimite['values']:
                                                                print(limite)
                                                                name = str(limite['name'])
                                                                sourceType = str(limite['sourceType'])
                                                                createdTime = str(limite['createdTime'])
                                                                modifiedTime = str(limite['modifiedTime'])
                                                                area = str(limite['area'])
                                                                workableArea = None
                                                                extent = str(limite['extent'])
                                                                archived = str(limite['archived'])
                                                                id = str(limite['id'])
                                                                active = str(limite['active'])
                                                                irrigated = str(limite['irrigated'])
                                                                miCursor.execute(
                                                                    "INSERT INTO Limite(name,sourceType,createdTime,modifiedTime,area,workableArea,extent,archived,id,active,irrigated,bandera) values(?,?,?,?,?,?,?,?,?,?,?,?)",
                                                                    name, sourceType, createdTime, modifiedTime, area,
                                                                    workableArea, extent,
                                                                    archived, id, active, irrigated, 0)

                                            for link in campo['links']:
                                                if link['rel'] == 'fieldOperations':
                                                    rOperacion = requests.get(link['uri'], headers=headers, auth=auth)
                                                    if rOperacion.status_code == 200:
                                                        dataOperacion = rOperacion.json()
                                                        if dataOperacion['total'] != 0:
                                                            for op in dataOperacion['values']:
                                                                print(op)
                                                                fieldOperationType = str(op['fieldOperationType'])
                                                                cropSeason = str(op['cropSeason'])
                                                                modifiedTime = str(op['modifiedTime'])
                                                                startDate = str(op['startDate'])
                                                                endDate = str(op['endDate'])
                                                                cropName = str(op['cropName'])
                                                                id = str(op['id'])
                                                                miCursor.execute(
                                                                    "INSERT INTO Operacion(fieldOperationType,cropSeason,modifiedTime,startDate,endDate,cropName,id,bandera) values(?,?,?,?,?,?,?,?)",
                                                                    fieldOperationType, cropSeason, modifiedTime, startDate,
                                                                    endDate, cropName,
                                                                    id, 0)

                                                                for link in op['links']:
                                                                    if link['rel'] == 'measurementTypes':
                                                                        rMedicion = requests.get(link['uri'], headers=headers,auth=auth)
                                                                        if rMedicion.status_code == 200:
                                                                            dataMedicion = rMedicion.json()
                                                                            if dataMedicion['total'] != 0:
                                                                                for med in dataMedicion['values']:
                                                                                    print(med)
                                                                                    measurementName = str(med['measurementName'])
                                                                                    measurementCategory = str(
                                                                                        med['measurementCategory'])

                                                                                    area = med['area']
                                                                                    value = None
                                                                                    value = str(area['value']) + str(area['unitId'])

                                                                                    yields = med['yield']
                                                                                    value = None
                                                                                    valueY = str(yields['value']) + str(
                                                                                        yields['unitId'])

                                                                                    averageYield = med['averageYield']
                                                                                    valueAY = None
                                                                                    valueAY = str(averageYield['value']) + str(
                                                                                        averageYield['unitId'])

                                                                                    averageMoisture = med['averageMoisture']
                                                                                    valueAM = None
                                                                                    valueAM = str(averageMoisture['value']) + str(
                                                                                        averageMoisture['unitId'])

                                                                                    wetMass = med['wetMass']
                                                                                    valueWM = None
                                                                                    valueWM = str(wetMass['value']) + str(
                                                                                        wetMass['unitId'])

                                                                                    averageWetMass = med['averageWetMass']
                                                                                    valueAWM = None
                                                                                    valueAWM = str(averageWetMass['value']) + str(
                                                                                        averageWetMass['unitId'])

                                                                                    averageSpeed = med['averageSpeed']
                                                                                    valueAS = None
                                                                                    valueAS = str(averageSpeed['value']) + str(
                                                                                        averageSpeed['unitId'])

                                                                                    miCursor.execute(
                                                                                        "INSERT INTO Medicion(measurementName,measurementCategory,area,yield,averageYield,averageMoisture,"
                                                                                        "westMass,averageWestMass,averageSpeed,bandera) values(?,?,?,?,?,?,?,?,?,?)",
                                                                                        measurementName, measurementCategory, value,
                                                                                        valueY, valueAY,
                                                                                        valueAM, valueWM, valueAWM, valueAS, 0)


# ELIMINACION DE REGISTROS DE TABLAS
'''
miCursor.execute("DROP TABLE Organizacion")
miCursor.execute("DROP TABLE Machine")
miCursor.execute("DROP TABLE MachineCategory")
miCursor.execute("DROP TABLE Alert")
miCursor.execute("DROP TABLE DeviceStateReports")
miCursor.execute("DROP TABLE HorasMotor")
miCursor.execute("DROP TABLE HorasOperacion")
miCursor.execute("DROP TABLE HistorialUbicacion")
miCursor.execute("DROP TABLE Cliente")
miCursor.execute("DROP TABLE Granja")
miCursor.execute("DROP TABLE Campo")
miCursor.execute("DROP TABLE Limite")
miCursor.execute("DROP TABLE Operacion")
miCursor.execute("DROP TABLE Medicion")
'''

miConexion.commit()
miCursor.close()
miConexion.close()
