import pymssql
import pandas as pd

class MottuDatabase:
    
    def database_connection(self):
        
        sqlUSer = 'bruno.alves'
        sqlPassword = 'M@rlboro122'
        sqlServer = '34.27.35.12'
        sqlDatabase = 'prod-mottu'
        
        self.conn = pymssql.connect(
            server = sqlServer,
            user = sqlUSer,
            psw = sqlPassword,
            database = sqlDatabase
        )

    def usuarioResidencia(self):
        self.database_connection()
        with self.conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT 
                    UE.id,
                    UE.usuarioId,
                    U.nome,
                    CASE WHEN U.matriz = 1 THEN U.id ELSE U.usuarioMatrizId END AS usuarioMatrizId,
                    UE.residenciaLatitude,
                    UE.residenciaLongitude,
                    UE.residenciaCidade,
                    UE.residenciaEstado,
                    UE.residenciaBairro,
                    U.marketplaceTipoCobranca

                FROM usuarioEndereco UE
                JOIN usuario U ON UE.usuarioId = U.id
                WHERE 1=1
                    AND U.perfilId IN (7,29)
                    AND U.marketplaceTipoCobranca = 20

                    ORDER BY 1 
                """
            )
            
            dtcolumns = ['id','usuarioId','nome','usuarioMatrizId','Latitude','Longitude'
                         , 'Cidade','Estado','Bairro', 'tipoCobranca'
                         ]

            return pd.DataFrame(cursor.fetchall(), columns=dtcolumns)