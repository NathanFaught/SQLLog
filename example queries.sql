select * from sdeadmin.RawLogs
where source = 'Basemap_FGDB_UNC.MapServer'
and message = 'End ExportMapImage'

select CONVERT(VARCHAR(10), time, 103) as Date, count(message) as Redraws
from sdeadmin.RawLogs
where source = 'Basemap_FGDB_UNC.MapServer'
and message = 'End ExportMapImage'
group by CONVERT(VARCHAR(10), time, 103)
order by CONVERT(VARCHAR(10), time, 103) desc

select distinct source
from sdeadmin.RawLogs

select COUNT(*) from sdeadmin.RawLogs

select distinct CONVERT(VARCHAR(10), time, 103) from sdeadmin.RawLogs

select CONVERT(VARCHAR(10), time, 103) as Date,datepart(hh,time) as "Hour", count(user)as "Draws", MIN(elapsed) as MinTime,MAX(elapsed) as MaxTime
from sdeadmin.RawLogs
where source = 'Basemap_FGDB_UNC.MapServer'
and message = 'End ExportMapImage'
group by CONVERT(VARCHAR(10), time, 103),datepart(hh,time)
order by CONVERT(VARCHAR(10), time, 103),datepart(hh,time) desc

select CONVERT(VARCHAR(10), time, 103) as Date,datepart(hh,time) as "Hour", count(user)as "Draws", MIN(elapsed) as MinTime,MAX(elapsed) as MaxTime
from sdeadmin.RawLogs
where source = 'Basemap_SQL.MapServer'
and message = 'End ExportMapImage'
group by CONVERT(VARCHAR(10), time, 103),datepart(hh,time)
order by CONVERT(VARCHAR(10), time, 103),datepart(hh,time) desc

--Map_draws_folder_per_hour (no limit)
SELECT [source] = SUBSTRING([source],1,charindex('/', [source])-1)
	,[Date]=CAST(DATEPART(year, [time]) as VARCHAR) + '-' + CAST(DATEPART(week, [time]) AS VARCHAR)
	,[Draws]=COUNT([code])
	,[MinTime]=MIN([elapsed])
	,[MaxTime]=MAX([elapsed])
	,[AvgTime]=AVG(CAST([elapsed] AS NUMERIC(38,4)))
	,[TotalTime]=(SUM(CAST([elapsed] AS NUMERIC(38,4))))
FROM sdeadmin.RawLogs
WHERE 1=1
	--AND source = 'Basemap.MapServer'
	AND code = 10011.00000000
GROUP BY SUBSTRING([source],1,charindex('/', [source])-1), CAST(DATEPART(year, [time]) as VARCHAR) + '-' + CAST(DATEPART(week, [time]) AS VARCHAR)
ORDER BY [Date] DESC, [source] ASC

--map_draws_per_day for past 60 days
SELECT [source]
	,[Date]=CAST([time] as date)
	,[Draws]=COUNT([code])
	,[MinTime]=MIN([elapsed])
	,[MaxTime]=MAX([elapsed])
	,[AvgTime]=AVG(CAST([elapsed] AS NUMERIC(38,4)))
	,[TotalTime]=(SUM(CAST([elapsed] AS NUMERIC(38,4))))
FROM sdeadmin.RawLogs
WHERE 1=1
	--AND source = 'Basemap.MapServer'
	AND code = 10011.00000000
	AND [time] > GETDATE() -60
GROUP BY [source], CAST([time] as date)
ORDER BY CAST([time] as date) DESC, [source] ASC

--map_draws_per_hour for past 15 days
SELECT [source]
	,[Date]=CAST([time] as date)
	,[Hour]=DATEPART(hh,time)
	,[Draws]=COUNT([code])
	,[MinTime]=MIN([elapsed])
	,[MaxTime]=MAX([elapsed])
	,[AvgTime]=AVG(CAST([elapsed] AS NUMERIC(38,4)))
	,[TotalTime]=(SUM(CAST([elapsed] AS NUMERIC(38,4))))
FROM sdeadmin.RawLogs
WHERE 1=1
	--AND source = 'Basemap.MapServer'
	AND [code] = 10011.00000000
	AND [time] > GETDATE() -15
GROUP BY [source], CAST([time] as date), DATEPART(hh,time)
ORDER BY CAST([time] as date) DESC, DATEPART(hh,time) ASC, [source] ASC

--map_draws_per_week (no limit)
SELECT [source]
	,[Date]=CAST(DATEPART(year, [time]) as VARCHAR) + '-' + CAST(DATEPART(week, [time]) AS VARCHAR)
	,[Draws]=COUNT([code])
	,[MinTime]=MIN([elapsed])
	,[MaxTime]=MAX([elapsed])
	,[AvgTime]=AVG(CAST([elapsed] AS NUMERIC(38,4)))
	,[TotalTime]=(SUM(CAST([elapsed] AS NUMERIC(38,4))))
FROM sdeadmin.RawLogs
WHERE 1=1
	--AND source = 'Basemap.MapServer'
	AND code = 10011.00000000
GROUP BY [source], CAST(DATEPART(year, [time]) as VARCHAR) + '-' + CAST(DATEPART(week, [time]) AS VARCHAR)
ORDER BY [source] ASC, [Date] DESC

--Purge query, to clean out old logs (greater than 40 days)
--This will NOT purge code 10011. 10011 is the code all of our reports are based on.
--You may want to purge cod 10011 after 1 or 2 years, depending on storage and need.
DELETE FROM [sdeadmin].[RawLogs] 
WHERE 1=1
AND (code <> 10011.00000000 OR code IS NULL)
AND [time] < GETDATE()-40