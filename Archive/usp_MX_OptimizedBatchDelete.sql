SET NOCOUNT ON;  -- 避免返回行數計數，減少網路負擔

-- 宣告變數
DECLARE @BatchSize INT = 500                 -- 初始批次大小
DECLARE @MinBatchSize INT = 100              -- 最小批次大小
DECLARE @MaxBatchSize INT = 10000            -- 最大批次大小
DECLARE @DeletedRows INT = 1                  -- 已刪除的行數（初始化為1以確保進入迴圈）
DECLARE @TotalDeletedRows INT = 0             -- 總共刪除的行數
DECLARE @RetainDate VARCHAR(10)               -- 保留日期
DECLARE @StartTime DATETIME = GETDATE();      -- 腳本開始執行的時間
DECLARE @BatchStartTime DATETIME;             -- 每個批次開始的時間
DECLARE @BatchDuration INT;                    -- 每個批次執行的持續時間（毫秒）
DECLARE @MaxRuntime INT = 3600;               -- 最大運行時間（秒）
DECLARE @TotalBatchDuration INT = 0;          -- 總批次執行時間
DECLARE @BatchCount INT = 0;                  -- 批次計數

-- 設置保留日期為三個月前
SET @RetainDate = CONVERT(VARCHAR(8), DATEADD(MONTH, -3, GETDATE()), 112)

-- 主要執行邏輯
BEGIN TRY
    WHILE @DeletedRows > 0  -- 當還有記錄可以刪除時，繼續執行
    BEGIN
        -- 檢查是否達到最大運行時間
        IF DATEDIFF(SECOND, @StartTime, GETDATE()) > @MaxRuntime
        BEGIN
            PRINT '達到最大運行時間限制，腳本停止執行。'
            BREAK
        END

        SET @BatchStartTime = GETDATE();  -- 記錄批次開始時間

        -- 使用公用資料表運算式（CTE）來選擇要刪除的記錄
        WITH CTE AS (
            SELECT TOP (@BatchSize) *
            FROM [BSADBTW].[dbo].[SwiftMX_DetectionResult_NotHit] WITH (NOLOCK)
            WHERE UpdatedDate < @RetainDate
            ORDER BY UpdatedDate
        )
        DELETE FROM CTE;

        -- 更新已刪除的行數
        SET @DeletedRows = @@ROWCOUNT;
        SET @TotalDeletedRows += @DeletedRows;  -- 總刪除行數累加
        
        -- 計算批次執行時間
        SET @BatchDuration = DATEDIFF(MILLISECOND, @BatchStartTime, GETDATE());
        SET @TotalBatchDuration += @BatchDuration;
        SET @BatchCount += 1;

        -- 動態調整批次大小
        IF @BatchDuration < 1000 AND @BatchSize < @MaxBatchSize
            SET @BatchSize *= 2;  -- 如果執行太快，增加批次大小
        ELSE IF @BatchDuration > 5000 AND @BatchSize > @MinBatchSize
            SET @BatchSize /= 2;  -- 如果執行太慢，減少批次大小

        -- 確保批次大小在允許的範圍內
        SET @BatchSize = 
            CASE 
                WHEN @BatchSize < @MinBatchSize THEN @MinBatchSize
                WHEN @BatchSize > @MaxBatchSize THEN @MaxBatchSize
                ELSE @BatchSize
            END;

        -- 輸出執行情況
        PRINT '已刪除 ' + CAST(@DeletedRows AS VARCHAR(10)) + ' 行。批次執行時間: ' + 
              CAST(@BatchDuration AS VARCHAR(10)) + '毫秒。新的批次大小: ' + CAST(@BatchSize AS VARCHAR(10));

        -- 每刪除5000行執行一次檢查點，以管理交易日誌大小
        IF @TotalDeletedRows >= 5000
        BEGIN
            CHECKPOINT;
            PRINT '已執行檢查點（CHECKPOINT），刪除5000行後。';
            SET @TotalDeletedRows = 0;  -- 重置計數器
        END
        
        -- 短暫延遲以減少系統負載
        WAITFOR DELAY '00:00:01';  -- 等待1秒
    END
END TRY
BEGIN CATCH
    -- 錯誤處理
    PRINT '錯誤: ' + ERROR_MESSAGE();
END CATCH

-- 輸出總執行時間和平均批次執行時間
PRINT '總執行時間: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + ' 秒';
IF @BatchCount > 0
    PRINT '平均批次執行時間: ' + CAST(@TotalBatchDuration / @BatchCount AS VARCHAR(10)) + ' 毫秒';
ELSE
    PRINT '沒有執行任何批次';
