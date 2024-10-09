DECLARE @BatchSize INT = 500  -- 每批次刪除的數量
DECLARE @DeletedRows INT = 1   -- 初始化為 1，確保進入 WHILE 迴圈
DECLARE @MinDate VARCHAR(10)   -- 儲存目前最早的日期
DECLARE @RetainDate VARCHAR(10) = '20230101'  -- 設定要保留的資料日期範圍
DECLARE @StartTime DATETIME = GETDATE();  -- 紀錄開始時間

-- 在 TRY...CATCH 中執行刪除操作
BEGIN TRY
    WHILE @DeletedRows > 0
    BEGIN
        -- 找出目前最早的 UpdatedDate，且早於保留日期
        SELECT @MinDate = MIN(UpdatedDate)
        FROM MX
        WHERE UpdatedDate < @RetainDate
        
        -- 如果沒有最早日期，則停止
        IF @MinDate IS NULL
            BREAK
        
        -- 刪除最早日期的 @BatchSize 筆資料
        DELETE TOP (@BatchSize)
        FROM MX
        WHERE UpdatedDate = @MinDate
        
        -- 檢查刪除了多少行
        SET @DeletedRows = @@ROWCOUNT
        
        -- 紀錄進度
        PRINT CAST(@DeletedRows AS VARCHAR(10)) + ' rows deleted for date ' + @MinDate
        
        -- 加入延遲，避免對系統造成過大負載 (可選)
        WAITFOR DELAY '00:00:03'  -- 等待 3 秒再進行下一批刪除
    END
END TRY
BEGIN CATCH
    -- 捕捉錯誤並顯示錯誤訊息
    PRINT 'Error: ' + ERROR_MESSAGE()
END CATCH

-- 顯示總執行時間
PRINT 'Total time taken: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + ' seconds';