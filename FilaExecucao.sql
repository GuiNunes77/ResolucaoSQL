-- Questões sobre desenvolvimento SQL e conceitos


-- Criando a tabela FilaExecucao
CREATE TABLE FilaExecucao (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    NomeTarefa VARCHAR(100) NOT NULL,
    Status VARCHAR(20) NOT NULL DEFAULT 'Pendente',
    DataCriacao DATETIME DEFAULT GETDATE(),
    DataInicio DATETIME NULL,
    DataFim DATETIME NULL,
    MensagemErro VARCHAR(500) NULL
);

-- Criando stored procedure AdicionarTarefa
CREATE PROCEDURE AdicionarTarefa
    @NomeTarefa VARCHAR(100)
AS
BEGIN
    INSERT INTO FilaExecucao (NomeTarefa, Status)
    VALUES (@NomeTarefa, 'Pendente');
END;


-- Criando stored procedure ExecutarTarefas utilizando execução em lote (padrão 5), tabela temporária e bloqueio de linhas
CREATE PROCEDURE ExecutarTarefas
    @BatchSize INT = 5
AS
BEGIN
    SET NOCOUNT ON;

    -- Cria tabela temporária para armazenar tarefas selecionadas
    CREATE TABLE #ExecucaoTemp (
        ID INT,
        NomeTarefa VARCHAR(100)
    );

    -- Seleciona e bloqueia um lote de tarefas pendentes
    ;WITH TarefasSelecionadas AS (
        SELECT TOP (@BatchSize) ID, NomeTarefa
        FROM FilaExecucao WITH (ROWLOCK, READPAST, UPDLOCK)
        WHERE Status = 'Pendente'
        ORDER BY ID
    )
    UPDATE FilaExecucao
    SET Status = 'Em andamento',
        DataInicio = GETDATE()
    OUTPUT inserted.ID, inserted.NomeTarefa
    INTO #ExecucaoTemp (ID, NomeTarefa)
    FROM TarefasSelecionadas;

    -- Executa cada tarefa do lote
    DECLARE @ID INT, @NomeTarefa VARCHAR(100);

    DECLARE tarefas CURSOR FOR
        SELECT ID, NomeTarefa FROM #ExecucaoTemp;

    OPEN tarefas;
    FETCH NEXT FROM tarefas INTO @ID, @NomeTarefa;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            PRINT 'Executando tarefa: ' + @NomeTarefa;
            WAITFOR DELAY '00:00:02'; -- simulação

            UPDATE FilaExecucao
            SET Status = 'Concluída',
                DataFim = GETDATE()
            WHERE ID = @ID;
        END TRY
        BEGIN CATCH
            UPDATE FilaExecucao
            SET Status = 'Falha',
                MensagemErro = ERROR_MESSAGE(),
                DataFim = GETDATE()
            WHERE ID = @ID;
        END CATCH;

        FETCH NEXT FROM tarefas INTO @ID, @NomeTarefa;
    END;

    CLOSE tarefas;
    DEALLOCATE tarefas;
END;

-- TESTES

-- Adicionando tarefas
EXEC AdicionarTarefa @NomeTarefa = 'Processar Pedido #101';
EXEC AdicionarTarefa @NomeTarefa = 'Enviar Email #101';
EXEC AdicionarTarefa @NomeTarefa = 'Gerar Nota Fiscal #101';

-- Executando em paralelo (simulação)
EXEC ExecutarTarefas @BatchSize = 2;
EXEC ExecutarTarefas @BatchSize = 2;

-- Consultando resultados
SELECT * FROM FilaExecucao;
