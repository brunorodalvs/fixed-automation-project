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
            password = sqlPassword,
            database = sqlDatabase
        )

    def usuarioResidencia(self):
        self.database_connection()
        with self.conn.cursor() as cursor:
            cursor.execute(
                f"""
        DECLARE @SLA_TMA_Standard DECIMAL(5, 2) = 90.00;
        DECLARE @SLA_ChegadaNaColeta_Standard DECIMAL(5, 2) = 90.00;
        DECLARE @TaxaDeCancelamentoStandard DECIMAL(5, 2) = 0.00;
        DECLARE @DataInicio DATE = DATEADD(DAY, -7, GETUTCDATE());
        declare @matrizID int = 243081;

        ;WITH EventosOrdenados AS (
            -- Primeira CTE para ordenar eventos por criacaoData
            SELECT
                pedidoId,
                pedidoEventoTipoId,
                criacaoData,
                ROW_NUMBER() OVER(PARTITION BY pedidoId, pedidoEventoTipoId ORDER BY criacaoData DESC) AS Rn
            FROM pedidoEvento
            WHERE criacaoData >= @DataInicio
        ), EventosAgregados AS (
            -- Segunda CTE para agregar os dados da primeira CTE
            SELECT
                EO.pedidoId,
        		P.usuarioOrigemId,
        		MAX(CASE WHEN pedidoEventoTipoId = 5 AND Rn = 1 THEN EO.criacaoData END) AS criacaodata,
                MAX(CASE WHEN pedidoEventoTipoId = 1 AND Rn = 1 THEN EO.criacaoData END) AS dataAceito,
                MAX(CASE WHEN pedidoEventoTipoId = 3 AND Rn = 1 THEN EO.criacaoData END) AS dataEntregue,
                MIN(CASE WHEN pedidoEventoTipoId = 10 AND Rn = 1 THEN EO.criacaoData END) AS dataChegadaColeta,
                MAX(CASE WHEN pedidoEventoTipoId = 2 AND Rn = 1 THEN EO.criacaoData END) AS dataRetirada,
                MAX(CASE WHEN pedidoEventoTipoId = 11 AND Rn = 1 THEN EO.criacaoData END) AS dataChegadanaEntrega,
        		MAX(CASE WHEN pedidoEventoTipoId = 21 AND Rn = 1 THEN EO.criacaoData END) AS dataInicioRetorno,
        		MAX(CASE WHEN pedidoEventoTipoId = 19 AND Rn = 1 THEN EO.criacaoData END) AS dataEntregueRetorno,
        		MAX(CASE WHEN pedidoEventoTipoId in (3,6,8,19) AND Rn = 1 THEN EO.criacaoData END) AS dataFim,
        		MAX(CASE WHEN pedidoEventoTipoId = 18 AND Rn = 1 THEN EO.criacaoData END) AS dataPrevisto,
                COUNT(CASE WHEN pedidoEventoTipoId = 7 AND Rn = 1 THEN 1 END) AS Kicks_totais,
                COUNT(CASE WHEN pedidoEventoTipoId = 14 AND Rn = 1 THEN 1 END) AS Desistencias,
        		P.situacao as pedidosituacao,
        		count(CASE WHEN pedidoEventoTipoId in (3,8,19) AND Rn = 1 THEN 1 END) as Entregas

            FROM EventosOrdenados EO inner join pedido P on P.id = EO.pedidoid
        	left join PedidoEntrega PE on PE.pedidoid = P.id
        	where P.pedidotipoid != 3
        	and P.Origem != 'mottu'
            GROUP BY EO.pedidoId,P.usuarioOrigemId,P.situacao
        ), AnaliseUsuarios as (
        SELECT
            U.id as usuarioid, 
        	U.nome,
        	count(EA.criacaoData) as PedidosTotais,
        	sum(EA.Entregas) as Entregas,
        	COUNT(CASE WHEN datediff(minute,EA.criacaoData,dataChegadaColeta)<=15 THEN 1 ELSE NULL END) AS PedidosDentrodoSLA,
        	COUNT(CASE WHEN EA.pedidosituacao in (200,50,70,171) THEN 1 ELSE NULL END) AS PedidosCanceladosTotal,
        	COUNT(CASE WHEN EA.pedidosituacao in (200,50,70,171) and datediff(minute,EA.criacaoData,dataFim)>=30 and dataPrevisto is null THEN 1 ELSE NULL END) AS PedidosCancelados,
        	COUNT(CASE WHEN EA.pedidosituacao in (171) THEN 1 ELSE NULL END) AS PedidosExtraviados,
        	COUNT(CASE WHEN EA.pedidosituacao in (30) and datediff(minute,EA.criacaoData,dataFim)<=60 and dataPrevisto is null THEN 1 ELSE NULL END) AS PedidosEntreguesTMA,
        	max(UE.residenciaLatitude) as residenciaLatitude,
        	max(UE.residenciaLongitude) as residenciaLongitude,
        	max(Ue.distritoH3Resolucao7) as distritoH3Resolucao7,
			UE.residenciaBairro,
			UE.residenciaCidade


        FROM EventosAgregados EA 
        	inner join usuario U on EA.usuarioOrigemId = U.id 
        	left join usuarioEndereco UE on U.id = UE.usuarioId

        where EA.dataAceito >= @DataInicio
        
        	and U.perfilId in (7,29)


        group by U.id, U.nome, UE.residenciaBairro,
			UE.residenciaCidade),
        PrincipaisIndicadores as (
        
        select nome,
        	usuarioid,
        	residenciaLatitude,
        	residenciaLongitude,
			residenciaCidade,
			residenciaBairro,
        	
        	PedidosTotais,
        	PedidosDentrodoSLA ,
        	PedidosEntreguesTMA,
        	PedidosCanceladosTotal,
        	PedidosCancelados,
        	PedidosExtraviados
        	from AnaliseUsuarios AU
         )

         select * 
        	from PrincipaisIndicadores

        order by 2 desc
        """
            )
            
            dtcolumns = ['nome','usuarioId','latitude','longitude','cidade','bairro','pedidosTotais','pedidosDentroDoSLA'
                         , 'pedidosEntreguesTMA','pedidosCanceladosTotal','pedidosCancelados', 'pedidosExtraviados'
                         ]

            return pd.DataFrame(cursor.fetchall(), columns=dtcolumns)