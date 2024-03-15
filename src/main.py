from db.mssql_connector import MottuDatabase
import h3
import pandas as pd


sql = MottuDatabase()

usuarioResidencia = sql.usuarioResidencia()
listCompanies = []
i = 0
for company in usuarioResidencia['usuarioId']:
    company = dict(
        usuarioId = usuarioResidencia['usuarioId'][i],
        nome = usuarioResidencia['nome'][i],
        bairro = usuarioResidencia['bairro'][i],
        cidade = usuarioResidencia['cidade'][i],
        Latitude = usuarioResidencia['latitude'][i],
        Longitude = usuarioResidencia['longitude'][i],
        H3_7 = h3.geo_to_h3(
            usuarioResidencia['latitude'][i],
            usuarioResidencia['longitude'][i], 7 )
    )
    i+=1
    listCompanies.append(company)

df = pd.DataFrame(listCompanies).sort_values(by=['H3_7'])
print(df)
# df.to_excel('clientes_h3.xlsx',sheet_name="main")