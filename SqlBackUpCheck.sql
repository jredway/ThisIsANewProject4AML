在 SQL Server 中，要檢查日誌檔案是否成功截斷，可以使用以下幾種方法來查看日誌狀態和備份情況。

### 1. **使用 `sys.databases` 檢查日誌重用等待描述**
   - `sys.databases` 系統視圖中的 `log_reuse_wait_desc` 列可以告訴你日誌空間無法重用（即未能成功截斷）的原因。

   ```sql
   SELECT name AS DatabaseName, log_reuse_wait_desc 
   FROM sys.databases;
   ```

   - 結果中的 `log_reuse_wait_desc` 可能的值包括：
     - **NOTHING**: 日誌空間可以重用，沒有等待任何操作。
     - **CHECKPOINT**: 正在等待檢查點來進行截斷。
     - **LOG_BACKUP**: 等待日誌備份。這意味著尚未進行足夠的日誌備份來釋放空間。
     - **ACTIVE_TRANSACTION**: 有未完成的交易，阻止日誌截斷。
     - **REPLICATION**: 正在等待複寫日誌同步完成。
     - **DATABASE_MIRRORING**: 正在等待資料庫鏡像操作完成。

   透過查看這個欄位，可以了解日誌檔案無法被截斷的具體原因。

### 2. **檢查最近的日誌備份時間**
   - 使用以下查詢來查看最近一次日誌備份的時間，以確保日誌備份操作已經執行：

   ```sql
   SELECT 
       database_name,
       MAX(backup_finish_date) AS LastLogBackupTime
   FROM msdb.dbo.backupset
   WHERE type = 'L'
   GROUP BY database_name;
   ```

   - 這會顯示每個資料庫的最新日誌備份完成時間。如果日誌檔案過大且最近沒有進行日誌備份，這可能是問題的根源。

### 3. **使用 `DBCC SQLPERF(logspace)` 檢查日誌檔案大小**
   - `DBCC SQLPERF(logspace)` 命令可以用來檢查資料庫日誌檔案的大小和使用情況：

   ```sql
   DBCC SQLPERF(logspace);
   ```

   - 結果會顯示每個資料庫的日誌檔案總大小 (`Log Size (MB)`)、已使用的百分比 (`Log Space Used (%)`)、以及未使用的空間。通過查看未使用空間，你可以判斷日誌檔案是否在備份後得到有效釋放。

### 4. **檢查 SQL Server Agent 作業歷史**
   - 如果你的備份是通過 SQL Server Agent 作業自動進行的，可以檢查作業歷史來確認是否有任何失敗的備份作業。

   ```sql
   EXEC msdb.dbo.sp_help_jobhistory @job_name = 'YourLogBackupJobName';
   ```

   - 這將顯示指定作業的執行歷史，並提供關於成功與失敗的詳細資訊。

### 5. **檢查 SQL Server 錯誤日誌**
   - SQL Server 的錯誤日誌中也記錄了日誌截斷和備份操作。你可以使用以下命令查看最近的錯誤日誌，以查找與日誌截斷相關的訊息。

   ```sql
   EXEC xp_readerrorlog 0, 1, N'Backup Log';
   ```

   - 這將篩選出錯誤日誌中的備份日誌操作記錄，可以用來判斷是否有任何日誌備份或截斷操作失敗。

### 結論
透過上述方法，你可以檢查 SQL Server 中日誌檔案的狀態以及最近的日誌備份操作，從而判斷日誌檔案是否成功截斷。如果 `log_reuse_wait_desc` 顯示有其他等待狀態，則需要進一步調查具體的原因。