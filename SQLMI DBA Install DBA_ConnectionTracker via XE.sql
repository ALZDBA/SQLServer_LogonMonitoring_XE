/*

DBA_ConnectionTracker: Maintain an overview of which connections are made to this SQLServer instance

-- This version uses Extended Events ( no more SQLServer Service Brocker with Event Notifications needed )

*/
set QUOTED_IDENTIFIER on;
go

USE DDBAServerPing;
go
--if object_id( 'dbo.T_DBA_ConnectionTracker') is not null
--begin
--	drop table dbo.T_DBA_ConnectionTracker
--end
go
if object_id('dbo.T_DBA_ConnectionTracker') is null
begin
	print 'Table [T_DBA_ConnectionTracker] Created';
	CREATE TABLE [dbo].[T_DBA_ConnectionTracker](
		[host_name] [varchar](128) NOT NULL,
		[program_name] [varchar](128) NOT NULL,
		[DbName] [varchar](128) NOT NULL,
		[nt_user_name] [varchar](128) NOT NULL,
		[login_name] [varchar](128) NOT NULL,
		[original_login_name] [varchar](128) NOT NULL,
		[is_dac] bit not null, 
		[tsRegistration] datetime NOT NULL default getdate(),
		[tsLastUpdate] datetime NOT NULL default getdate(),
		[FastTrack_CheckSum]  AS (checksum([host_name],[program_name],[DbName],[nt_user_name],[login_name],[original_login_name],[is_dac])) PERSISTED
			) ;
	Create clustered index clX_DBA_ConnectionTracker on [dbo].[T_DBA_ConnectionTracker] ([tsRegistration]);
	Create index X_DBA_ConnectionTracker on [dbo].[T_DBA_ConnectionTracker] ([login_name], [program_name]);
	CREATE INDEX [X_T_DBA_ConnectionTracker_FastTrack_CheckSum] ON [dbo].[T_DBA_ConnectionTracker] ( [FastTrack_CheckSum] ASC );

end 

if object_id('dbo.T_DBA_ConnectionTracker_XE_Log') is null
begin
	print 'Table dbo.T_DBA_ConnectionTracker_XE_Log Created';
	CREATE TABLE [dbo].[T_DBA_ConnectionTracker_XE_Log](
		[timestamp_utc] [datetime2](7) NOT NULL PRIMARY KEY,
		[file_name] [nvarchar](500)  NOT NULL,
		[file_offset] [bigint] NOT NULL
	)
	;

end

go
/*
OPGELET: BLOB Storage MOET type V2 of later zijn !!! 
ref: https://techcommunity.microsoft.com/t5/azure-database-support-blog/extended-events-azure-storage-account-types-matter/ba-p/369098

It turns out, the blob storage account type only supports block and append blobs. page blobs are not supported. 

Extended events are written as page blobs, as they are written to storage as random-accessible pages.
This was why an error occurred when attempting to write extended event data to an Azure blob storage account type .

--Msg 25602, Level 17, State 23, Line 38
--The target, "5B2DA06D-898A-43C8-9309-39BBBE93EBBD.package0.event_file", encountered a configuration error during initialization. 
--Object cannot be added to the event session. (null)

Unfortunately, "Azure storage" and "blob storage" are referenced interchangeably as service concepts throughout many of our documents, 
which can inadvertently give the illusion that, "blob storage" supports all blob types ( block , append , and page ) 
when, in reality, the blob storage account type specifically does not.  

As for a solution to the matter, options are limited to upgrading the blob storage account type to a general-purpose v2 
or, to use another storage account that is of type general-purpose (v2 preferably, since v2 uses Azure Resource Manager).  
Instructions on the former can be found here: https://docs.microsoft.com/en-us/azure/storage/common/storage-account-upgrade . 

*/

--  sqlmi-awe-gp-dev-001.weu1169eecf89258.database.windows.net
-- DROP EVENT SESSION DBA_TrackLogins ON SERVER;

-- N'https://<AzureStorageAccount>.blob.core.windows.net/sqlxeventsdev/DBA_XE_Logins_sqlmi-awe-gp-dev-001_20210921_0705.xel',
if exists ( Select *
			from sys.dm_xe_sessions
			where name = 'DBA_TrackLogins' )
BEGIN
	DROP EVENT SESSION DBA_TrackLogins ON SERVER
END
GO

Declare @xEFileName Nvarchar(1000)
Select top (1) @xEFileName = N'https://<AzureStorageAccount>.blob.core.windows.net/'+ value +'/'
	+ N'DBA_XE_TrackLogins'
	+ N'_' + replace(replace(replace(convert(char(10),getdate(),121),'-',''),' ','_'),':','') + '_' + '.xel' -- extension mandatory for SQLMI
FROM string_split ( convert(varchar(500), SERVERPROPERTY('servername')), '.') 
;
-- Select @xEFileName 

-- DROP EVENT SESSION DBA_TrackLogins ON SERVER
Declare @XEDDL Nvarchar(max) = '
CREATE EVENT SESSION DBA_TrackLogins 
ON SERVER
 ADD EVENT sqlserver.login
 (
   ACTION 
   (
     sqlserver.client_app_name,
     sqlserver.client_hostname,
     sqlserver.nt_username,
	 sqlserver.session_nt_username,
	 sqlserver.username,
	 sqlserver.database_id
    )
  )
  ADD TARGET package0.event_file
    (SET
        filename = N'''+ @xEFileName +''',
        max_file_size = 25,
        max_rollover_files = 3
    )
    WITH (
        MAX_MEMORY = 2048 KB,
        EVENT_RETENTION_MODE = ALLOW_MULTIPLE_EVENT_LOSS,
        MAX_DISPATCH_LATENCY = 3 SECONDS,
        MAX_EVENT_SIZE = 0 KB,
        MEMORY_PARTITION_MODE = NONE,
        TRACK_CAUSALITY = OFF,
        STARTUP_STATE = ON 
    );

Print ''EVENT SESSION DBA_TrackLogins installed'';
'
exec sys.sp_executesql @stmt = @XEDDL ;

ALTER EVENT SESSION DBA_TrackLogins ON SERVER
  STATE = START;
GO


CREATE OR ALTER PROCEDURE spc_DBA_Process_XE_ConnectionTracker   
	@Debug BIT = 0
AS
BEGIN
/*
Process XE connectiondata Files
20211007 Johan Bijnens @alzdba 

exec dbo.spc_DBA_Process_XE_ConnectionTracker

exec dbo.spc_DBA_Process_XE_ConnectionTracker @Debug = 1

*/
	SET NOCOUNT ON;

    CREATE TABLE #XeEventData
		([event_data]    [XML] NULL, 
			[file_name]     [NVARCHAR](260)  NOT NULL, 
			[file_offset]   [BIGINT] NOT NULL, 
			[timestamp_utc] DATETIME2(7) NOT NULL
		);
    CREATE TABLE #XEConn
		([client_hostname]     [VARCHAR](128)  NULL, 
			[client_app_name]     [VARCHAR](500)  NULL, 
			[username]            [VARCHAR](128)  NULL, 
			[nt_username]         [VARCHAR](128)  NULL, 
			[session_nt_username] [VARCHAR](128)  NULL, 
			[database_id]         [INT] NULL, 
			[is_dac]              bit not NULL, 
			[is_cached]           bit not NULL, 
			[event_timestamp]     [DATETIME] NULL, 
			[DbName]              [VARCHAR](128)  NULL, 
			event_date            DATETIME NOT NULL, 
			[FastTrack_CheckSum]  AS (checksum([client_hostname],[client_app_name],[DbName],[nt_username],[username],[session_nt_username],[is_dac])) ,
			[event_rwno]          [BIGINT] NULL
		);

    DECLARE @XEFilename [NVARCHAR](500)
			, @XEfileoffset BIGINT
			, @wrkxml XML
			, @XERootFileName NVARCHAR(4000)
			, @RwCount int ;

	/* delete EX_Log > 24h */
	Delete 
	from dbo.T_DBA_ConnectionTracker_XE_Log
	where timestamp_utc < dateadd(hh, datediff(hh, 0, SYSUTCDATETIME()) - 24 , 0 ) ;

	Set @RwCount = @@rowcount ;

	if @Debug = 1
	begin
		Select '000' as Debug,  @RwCount as nRows_deleted_T_DBA_ConnectionTracker_XE_Log
	end

	/* get session target information */
    SELECT TOP 1 @wrkxml = CONVERT(XML, XST.target_data)
	FROM sys.dm_xe_session_targets AS XST
    INNER JOIN sys.dm_xe_sessions AS XS
            ON XS.address = XST.event_session_address
    WHERE XS.name = 'DBA_TrackLogins';
	
	Set @RwCount = @@rowcount ;

    SELECT @XERootFileName = @wrkxml.value('(EventFileTarget/File/@name)[1]', 'nvarchar(4000)');
	/* only keep root part ( as used with the XE event creation ) */ 
    -- not for SQLMI SELECT @XERootFileName = SUBSTRING(@XERootFileName, 0, CHARINDEX('__', @XERootFileName) + 1);
	
	if @Debug = 1
	begin
		Select '001' as Debug,  @XERootFileName as XERootFileName
	end

	/* get last recorde XE Log info */
    SELECT TOP (1) @XEFilename = [file_name]
                    , @XEfileoffset = file_offset
	FROM dbo.T_DBA_ConnectionTracker_XE_Log
    ORDER BY timestamp_utc DESC;
	
	Set @RwCount = @@rowcount ;

    IF @RwCount = 0
        BEGIN
            SELECT @XEFilename = NULL
                 , @XEfileoffset = NULL;
        END;
	BEGIN TRY  
     	INSERT INTO #XeEventData
            SELECT CONVERT(XML, event_data) AS event_data
                , file_name
                , file_offset
                , timestamp_utc
            FROM sys.fn_xe_file_target_read_file(
					  @XERootFileName -- not for SQLMI + '*.xel'
					, NULL -- NULL for SQLMI  @XERootFileName + '*.xem'
					, @XEFilename, @XEfileoffset);
			
		Set @RwCount = @@rowcount ;

	END TRY  
	BEGIN CATCH  
		 INSERT INTO #XeEventData
            SELECT CONVERT(XML, event_data) AS event_data
                , file_name
                , file_offset
                , timestamp_utc
            FROM sys.fn_xe_file_target_read_file(
					  @XERootFileName  -- not for SQLMI + '*.xel'
					, NULL -- NULL for SQLMI  @XERootFileName + '*.xem'
					, NULL, NULL);	

		Set @RwCount = @@rowcount ;

	END CATCH  
	
	if @Debug = 1
	begin
		Select '010' as Debug,  @RwCount [nRows_inserted_to_#XeEventData]

		Select top ( 50 ) '011' as Debug,  *
		from #XeEventData
		order by timestamp_utc desc

	end

    /* store last row for XE_Log */
    INSERT INTO dbo.T_DBA_ConnectionTracker_XE_Log
            SELECT TOP (1) [timestamp_utc]
						, [file_name]
                        , [file_offset]
            FROM #XeEventData
            ORDER BY [timestamp_utc] DESC;
    
	Set @RwCount = @@rowcount ;
	if @Debug = 1
	begin
		Select '020' as Debug,  @RwCount nRows_inserted_to_T_DBA_ConnectionTracker_XE_Log
	END

	;WITH cteTabular
            AS (SELECT event_data.value('(event/action[@name="client_hostname"]/value)[1]', 'nvarchar(4000)') AS client_hostname
                    , event_data.value('(event/action[@name="client_app_name"]/value)[1]', 'nvarchar(4000)') AS client_app_name
                    , event_data.value('(event/action[@name="username"]/value)[1]', 'nvarchar(4000)') AS username
                    , event_data.value('(event/action[@name="nt_username"]/value)[1]', 'nvarchar(4000)') AS nt_username
                    , event_data.value('(event/action[@name="session_nt_username"]/value)[1]', 'nvarchar(4000)') AS session_nt_username
                    , event_data.value('(event/action[@name="database_id"]/value)[1]', 'int') AS database_id
                    , event_data.value('(event/data[@name="is_dac"]/value)[1]', 'nvarchar(4000)') AS is_dac
                    , event_data.value('(event/data[@name="is_cached"]/value)[1]', 'nvarchar(4000)') AS is_cached
                    , event_data.value('(event/@timestamp)[1]', 'datetime2') AS event_timestamp
                FROM #XeEventData),
            ctePrepDedup
            AS (SELECT client_hostname
                    , case when client_app_name like 'DatabaseMail - %' then 'DatabaseMail' 
						-- avoid programe name for these logins
						-- 'UABEPRD\EXECBTS' geef message GUID als application name mee !! register this user only once 
                            when upper(session_nt_username) like 'UABEPRD\EXECBTS%' then 'DBA-Excluded-EAI'
							else client_app_name 
						end as client_app_name
                    , ISNULL(username, '') as username
                    , nt_username
                    , session_nt_username
                    , database_id
                    , case [is_dac] when 'True' then 1 else 0 end is_dac
                    , case is_cached when 'True' then 1 else 0 end is_cached
                    , CONVERT(DATETIME, event_timestamp) AS event_timestamp
                    , DB_NAME(database_id) AS DbName
                    , DATEADD(dd, DATEDIFF(dd, 0, event_timestamp), 0) AS event_date
                    , row_number() OVER(PARTITION BY client_hostname
                                            , client_app_name
                                            , username
                                            , nt_username
                                            , session_nt_username
                                            , database_id
                                            , is_dac
                                            , is_cached
                                            , DATEADD(dd, DATEDIFF(dd, 0, event_timestamp), 0)
                    ORDER BY event_timestamp) AS event_rwno
                FROM cteTabular)
        INSERT INTO #XEConn
            SELECT *
            FROM ctePrepDedup
            WHERE event_rwno = 1
			  and is_cached = 0; /* cached connections do not provide username, nt_username or session_nt_username !! */
	
	Set @RwCount = @@rowcount ;

    IF @Debug = 1
        BEGIN
			SELECT '030' as Debug, @RwCount as [nRows_inserted_to_#XEConn]
			
			SELECT '035' as Debug, *
			FROM #XEConn AS NEW
			ORDER BY NEW.event_timestamp
				   , NEW.FastTrack_CheckSum;
        END;

	Begin Try 
		/* update existing rows */
		/* only update first occurence of the day */
		UPDATE ct
			SET tsLastUpdate = NEW.event_timestamp
		 -- OUTPUT inserted.*
		FROM #XEConn NEW
		INNER JOIN dbo.T_DBA_ConnectionTracker ct
				ON ct.[host_name] = NEW.client_hostname
				AND ct.[program_name] = NEW.client_app_name
				AND ct.[nt_user_name] = NEW.nt_username
				AND ct.[login_name] = NEW.username
				AND ct.[original_login_name] = NEW.session_nt_username
				AND ct.[is_dac] = NEW.[is_dac]
				AND ct.[tsRegistration] < NEW.event_timestamp
				and ct.FastTrack_CheckSum = NEW.FastTrack_CheckSum 
		WHERE ct.[tsLastUpdate] < NEW.event_date;
		
		Set @RwCount = @@rowcount ;

		if @Debug = 1 
		begin
			SELECT '040' as Debug, @RwCount as nRows_Updated_in_T_DBA_ConnectionTracker;

			SELECT '041' as Debug, *
				FROM #XEConn AS NEW
				inner join  dbo.T_DBA_ConnectionTracker AS ct
									on ct.[host_name] = NEW.client_hostname
											AND ct.[program_name] = NEW.client_app_name
											AND ct.[nt_user_name] = NEW.nt_username
											AND ct.[login_name] = NEW.username
											AND ct.[original_login_name] = NEW.session_nt_username
											AND ct.[DbName] = NEW.DbName
											AND ct.[is_dac] = NEW.[is_dac] 
											AND ct.[tsRegistration] <= NEW.event_timestamp
				WHERE NEW.event_rwno = 1
				  and ct.FastTrack_CheckSum <> NEW.FastTrack_CheckSum
									;

		end

		/* insert new rows */
		INSERT INTO dbo.T_DBA_ConnectionTracker([host_name]
												, [program_name]
												, [login_name]
												, [nt_user_name]
												, [original_login_name]
												, [tsRegistration]
												, [tsLastUpdate]
												, [DbName]
												, [is_dac]
												)
				--	OUTPUT inserted.*
				SELECT client_hostname
					, client_app_name
					, username 
					, nt_username
					, session_nt_username
					-- , database_id
					-- , is_cached
					, event_timestamp
					, event_timestamp -- tslastupdate
					, DbName
					, is_dac
				FROM #XEConn AS NEW
				WHERE event_rwno = 1
						AND NOT EXISTS
									(
									SELECT *
									FROM dbo.T_DBA_ConnectionTracker AS ct
									-- assume +/- unique 
									WHERE ct.FastTrack_CheckSum = NEW.FastTrack_CheckSum
									);
		
		Set @RwCount = @@rowcount ;
		
		if @Debug = 1
		begin
			SELECT '050' as Debug, @RwCount as nRows_Inserted_to_T_DBA_ConnectionTracker;
		end
			
    END TRY
    BEGIN CATCH
		-- no problem if conflictis occur
		if @Debug = 1
		begin
			SELECT '055' as Debug, ERROR_PROCEDURE() AS ERROR_PROCEDURE
				 , SUSER_SNAME() as SUSER_SNAME
				 , ERROR_NUMBER() as ERROR_NUMBER
				 , ERROR_SEVERITY() as ERROR_SEVERITY
				 , ERROR_STATE() as ERROR_SEVERITY
				 , ERROR_MESSAGE() as ERROR_SEVERITY

		end
    END CATCH
	
	if @Debug = 1
	begin
		Select top ( 250 ) '060' as Debug, *
		from dbo.T_DBA_ConnectionTracker with (nolock)
		order by [tsLastUpdate] desc
	end
	
    /* cleanup */
    DROP TABLE #XeEventData;
    DROP TABLE #XEConn;

END;

go

Print 'Stored procedure spc_DBA_Process_XE_ConnectionTracker installed';


/* 
install job 
*/

USE [msdb]
GO

/****** Object:  Job [DBA_Process_ConnectionTracker_XE]    Script Date: 20/09/2021 9:11:01 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 20/09/2021 9:11:01 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA_Process_ConnectionTracker_XE', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Process ConnectionTracker XE', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'aweadmin', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [spc_DBA_Process_XE_ConnectionTracker]    Script Date: 20/09/2021 9:11:02 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'spc_DBA_Process_XE_ConnectionTracker', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec spc_DBA_Process_XE_ConnectionTracker /* @Debug = 1 */ ', 
		@database_name=N'DDBAServerPing', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Send email when ]    Script Date: 20/09/2021 9:11:02 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Send email when ', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=2, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'declare @subject varchar(1000)
	set @subject = convert(varchar(128), serverproperty(''servername'')) + '': job DBA_Process_ConnectionTracker_XE  !''
declare @body varchar(1000)
	set @body = convert(varchar(128), serverproperty(''servername'')) + '': job DBA_Process_ConnectionTracker_XE  and needs to be checked ! '' + convert(char(26), getutcdate(),121) + '' utc''

EXEC msdb.dbo.sp_send_dbmail 
	@recipients = ''SQLMI.DBA.Group@yourplace.com'', 
	@subject = @subject, 
	@body=@body;
', 
		@database_name=N'msdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
Declare @Today int = convert(char(8), getdate(),112)
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Every 15minutes', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=15, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=@Today, 
		@active_end_date=99991231, 
		@active_start_time=100, 
		@active_end_time=235959 

IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
Print 'job DBA_Process_ConnectionTracker_XE installed';
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO



USE DDBAServerPing;
go