USE BSADBTW
SET NOCOUNT ON;
DECLARE @FileName VARCHAR(200) = 'F:\MSSQL\Archive\ETLARCHIVE_BSADBTW_2019.mdf'
  
DECLARE
    @BankNo VARCHAR(5),
    @DateUTC DATETIME,
    @DateLocal DATETIME,
    @FileDate VARCHAR(8),
    @ValidMonth INT,
    @DBName VARCHAR(100),
    @IsExist INT = 1,
    @Version INT = 1,
    @SqlStr VARCHAR(MAX),
    @EndRevision INT;
  
	SET @BankNo = REPLACE(DB_NAME(), 'BSADB', '')
	SET @DateUTC = GETUTCDATE() 
	SET @DateLocal = CONVERT(VARCHAR(6),dbo.ConvertToBankLocalTime(@DateUTC),112)+'01'
  
SELECT @ValidMonth = CtrlVal
FROM dbo.PO_DefaultData WITH (NOLOCK)
WHERE Module = N'Archieve' AND CtrlKey = N'ETLSARArchieveValidMonth'
SET @DBName= 'ETLARCHIVE_BSADBTW_2019'

--EXEC Master.dbo.xp_fileexist @FileName, @IsExist OUTPUT  
--IF @IsExist = 1
--BEGIN
--    SET @DBName= 'ETLACHIVE_BSADBTW_2020'
--END 
--ELSE
--BEGIN
--	PRINT 'NOT CREATE THIS DATABASE';
--	GOTO EEND
--END

/*建立ARHICVE LOG TABLE*/
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET ANSI_PADDING ON
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Archieve_setting_table]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[Archieve_setting_table](
	[ArchieveName] [varchar](200) NULL,
	[ActionDT_UTC] [datetime] NULL,
	[HouseKeepDuration] [int] NULL,
	[BSADB_Version_EndRevision] [int] NULL
) ON [PRIMARY]
END


BEGIN 
   /*ETL Facts*/
   /*Transaction*/
   BEGIN
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CTS_TXN_FACT
            FROM dbo.CTS_TXN_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
              
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..OTHER_TXN_FACT
            FROM dbo.OTHER_TXN_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_TXN_FACT
            FROM dbo.WIRE_TXN_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CASHIERCHK_FACT
            FROM dbo.CASHIERCHK_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
              
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CHK_TXN_FACT
            FROM dbo.CHK_TXN_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ACH_TXN_FACT
            FROM dbo.ACH_TXN_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ATM_TXN_FACT
            FROM dbo.ATM_TXN_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRSF_TXN_FACT
            FROM dbo.TRSF_TXN_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CCARD_TXN_FACT
            FROM dbo.CCARD_TXN_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..DCARD_TXN_FACT
            FROM dbo.DCARD_TXN_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..SFB_TXN_FACT
            FROM dbo.SFB_TXN_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      --SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRADE_LOC_TRXN_FACT
      --      FROM dbo.TRADE_LOC_TRXN_FACT WITH (NOLOCK)
      --      WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      --EXEC (@SQLSTR)
           
      --SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRADE_BOL_TRXN_FACT
      --      FROM dbo.TRADE_BOL_TRXN_FACT WITH (NOLOCK)
      --      WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', TRANDT) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      --EXEC (@SQLSTR)
     
      --SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRADE_INVOICE_TRXN_FACT
      --      FROM dbo.TRADE_INVOICE_TRXN_FACT WITH (NOLOCK)
      --      WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', Invoice_Date) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      --EXEC (@SQLSTR)
   END
   PRINT ('Transaction Archive Done!! >> ' + CONVERT(VARCHAR,GETDATE(),121));

   /*Daily Fact*/
   BEGIN
      --*_DAILY_FACT 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ACH_DAILY_FACT
            FROM dbo.ACH_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
             
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CCD_DAILY_FACT
            FROM dbo.CCD_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ATM_CSH_DAILY_FACT
            FROM dbo.ATM_CSH_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ATM_NCSH_DAILY_FACT
            FROM dbo.ATM_NCSH_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
              
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CHK_DAILY_FACT
            FROM dbo.CHK_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CTS_DAILY_FACT
            FROM dbo.CTS_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..DCD_DAILY_FACT
            FROM dbo.DCD_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..MOI_DAILY_FACT
            FROM dbo.MOI_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..OTHER_DAILY_FACT
            FROM dbo.OTHER_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRSF_DAILY_FACT
            FROM dbo.TRSF_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_DAILY_FACT
            FROM dbo.WIRE_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TTL_DAILY_FACT
            FROM dbo.TTL_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
 
      --*_CTRY_DAILY_FACT    
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ACH_CTRY_DAILY_FACT
            FROM dbo.ACH_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CCD_CTRY_DAILY_FACT
            FROM dbo.CCD_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CHK_CTRY_DAILY_FACT
            FROM dbo.CHK_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CTS_CTRY_DAILY_FACT
            FROM dbo.CTS_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..DCD_CTRY_DAILY_FACT
            FROM dbo.DCD_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..MOI_CTRY_DAILY_FACT
            FROM dbo.MOI_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_CTRY_DAILY_FACT
            FROM dbo.WIRE_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TTL_CTRY_DAILY_FACT
            FROM dbo.TTL_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..OTHER_CTRY_DAILY_FACT
            FROM dbo.OTHER_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR) 
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRSF_CTRY_DAILY_FACT
            FROM dbo.TRSF_CTRY_DAILY_FACT WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      --*_DAILY_SMRY
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ACH_DAILY_ACCT_SMRY
            FROM dbo.ACH_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CCD_DAILY_ACCT_SMRY
            FROM dbo.CCD_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR) 
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CHK_DAILY_ACCT_SMRY
            FROM dbo.CHK_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CTS_DAILY_ACCT_SMRY
            FROM dbo.CTS_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..DCD_DAILY_ACCT_SMRY
            FROM dbo.DCD_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..MOI_DAILY_ACCT_SMRY
            FROM dbo.MOI_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..OTHER_DAILY_ACCT_SMRY
            FROM dbo.OTHER_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRSF_DAILY_ACCT_SMRY
            FROM dbo.TRSF_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRXN_D_TTL_DAILY_ACCT_SMRY
            FROM dbo.TRXN_D_TTL_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TTL_DAILY_ACCT_SMRY
            FROM dbo.TTL_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_DAILY_ACCT_SMRY
            FROM dbo.WIRE_DAILY_ACCT_SMRY WITH (NOLOCK)
           WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMMDD) < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
   END
   PRINT ('Daily Fact Archive Done!! >> ' + CONVERT(VARCHAR,GETDATE(),121));
  
   /*Montly Fact*/
   BEGIN
      --*_MTHLY_FACT 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ACH_MTHLY_FACT
            FROM dbo.ACH_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
              
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ATM_MTHLY_FACT
            FROM dbo.ATM_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CCD_MTHLY_FACT
            FROM dbo.CCD_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
	  
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..OTHER_MTHLY_FACT
            FROM dbo.OTHER_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)	  
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ATM_CSH_MTHLY_FACT
            FROM dbo.ATM_CSH_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
              
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ATM_NCSH_MTHLY_FACT
            FROM dbo.ATM_NCSH_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CHK_MTHLY_FACT
            FROM dbo.CHK_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CTS_MTHLY_FACT
            FROM dbo.CTS_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..DCD_MTHLY_FACT
            FROM dbo.DCD_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..MOI_MTHLY_FACT
            FROM dbo.MOI_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRSF_MTHLY_FACT
            FROM dbo.TRSF_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_MTHLY_FACT
            FROM dbo.WIRE_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_MTHLY_FACT_Domestic
            FROM dbo.WIRE_MTHLY_FACT_Domestic WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_MTHLY_FACT_Foreign
            FROM dbo.WIRE_MTHLY_FACT_Foreign WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TTL_MTHLY_FACT
            FROM dbo.TTL_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
 
      --*_CTRY_MTHLY_FACT    
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ACH_CTRY_MTHLY_FACT
            FROM dbo.ACH_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
           
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CCD_CTRY_MTHLY_FACT
            FROM dbo.CCD_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
     
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CHK_CTRY_MTHLY_FACT
            FROM dbo.CHK_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..DCD_CTRY_MTHLY_FACT
            FROM dbo.DCD_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..MOI_CTRY_MTHLY_FACT
            FROM dbo.MOI_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..OTHER_CTRY_MTHLY_FACT
            FROM dbo.OTHER_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRSF_CTRY_MTHLY_FACT
            FROM dbo.TRSF_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_CTRY_MTHLY_FACT
            FROM dbo.WIRE_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_CTRY_MTHLY_FACT_Domestic
            FROM dbo.WIRE_CTRY_MTHLY_FACT_Domestic WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_CTRY_MTHLY_FACT_Foreign
            FROM dbo.WIRE_CTRY_MTHLY_FACT_Foreign WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
     
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TTL_CTRY_MTHLY_FACT
            FROM dbo.TTL_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CTS_CTRY_MTHLY_FACT
            FROM dbo.CTS_CTRY_MTHLY_FACT WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
      --*_MTHLY_SMRY
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..ACH_MTHLY_ACCT_SMRY
            FROM dbo.ACH_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR) 
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CCD_MTHLY_ACCT_SMRY
            FROM dbo.CCD_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CHK_MTHLY_ACCT_SMRY
            FROM dbo.CHK_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..CTS_MTHLY_ACCT_SMRY
            FROM dbo.CTS_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)       
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..DCD_MTHLY_ACCT_SMRY
            FROM dbo.DCD_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)    
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..MOI_MTHLY_ACCT_SMRY
            FROM dbo.MOI_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)       
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..OTHER_MTHLY_ACCT_SMRY
            FROM dbo.OTHER_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR) 
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRSF_MTHLY_ACCT_SMRY
            FROM dbo.TRSF_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR) 
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TRXN_TTL_MTHLY_ACCT_SMRY
            FROM dbo.TRXN_TTL_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR) 
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..TTL_MTHLY_ACCT_SMRY
            FROM dbo.TTL_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR)       
 
      SET @SQLSTR = 'SELECT * INTO ' + @DBName + '..WIRE_MTHLY_ACCT_SMRY
            FROM dbo.WIRE_MTHLY_ACCT_SMRY WITH (NOLOCK)
            WHERE DATEADD(M, ' + CONVERT(VARCHAR(10), @ValidMonth) + ', YYYYMM' + ' + ''01''' + ') < ''' +  CONVERT(VARCHAR(8), @DateLocal, 112) + ''''
           
      EXEC (@SQLSTR) 
     
   END
   PRINT ('Montly Fact Archive Done!! >> ' + CONVERT(VARCHAR,GETDATE(),121));
   /*Backup Archieve Setting*/
   SELECT TOP 1 @EndRevision = EndRevision
   FROM GCMAINDB..InstallationPackage_log WITH (NOLOCK)
   WHERE DatabaseName = 'BSADB'
   ORDER BY EndRevision DESC
  
   INSERT INTO Archieve_setting_table(ArchieveName, ActionDT_UTC, HouseKeepDuration, BSADB_Version_EndRevision)
   VALUES(@DBName, @DateUTC, @ValidMonth, @EndRevision)

END 

EEND:
PRINT 'CONNECTION CLOSE~'
GO
