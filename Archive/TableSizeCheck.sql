SELECT TOP 100 
    t.NAME AS TableName,
    p.rows AS RowCounts,
    SUM(a.total_pages) * 8 / 1024 / 1024 AS TotalSpaceGB,
    SUM(a.used_pages) * 8 / 1024 / 1024 AS UsedSpaceGB,
    (SUM(a.total_pages) - SUM(a.used_pages)) * 8 / 1024 / 1024 AS UnusedSpaceGB
FROM 
    sys.tables t
INNER JOIN      
    sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN 
    sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN 
    sys.allocation_units a ON p.partition_id = a.container_id
WHERE 
    t.NAME NOT LIKE 'dt%' AND
    i.OBJECT_ID > 255 AND   
    i.index_id <= 1
GROUP BY 
    t.NAME, p.Rows
ORDER BY 
    TotalSpaceGB DESC;