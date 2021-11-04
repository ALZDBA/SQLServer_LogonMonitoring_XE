/*

DBA_FailedConnectionTracker: Maintain an overview of which connections are made to this Azure Managed Instance and failed

-- This version uses Extended Events ( no more SQLServer Service Brocker with Event Notifications needed )

*/

USE DDBAServerPing;

if object_id('dbo.T_DBA_FailedConnectionTracker') is null
begin
	print 'Table [T_DBA_FailedConnectionTracker] Created';
	CREATE TABLE [dbo].[T_DBA_FailedConnectionTracker](
		[host_name] [varchar](128) NOT NULL,
		[program_name] [varchar](128) NOT NULL,
		[nt_user_name] [varchar](128) NOT NULL,
		[login_name] [varchar](128) NOT NULL,
		[original_login_name] [varchar](128) NOT NULL,
		[client_net_address] [varchar](48) NOT NULL,
		[Database_Name] [varchar](128) not null,
		[tsRegistration] datetime NOT NULL default getdate(),
		[FailedLoginData] XML
			) ;
	Create clustered index clX_DBA_FailedConnectionTracker on [dbo].[T_DBA_FailedConnectionTracker] ([tsRegistration]);
	Create index X_DBA_FailedConnectionTracker on [dbo].[T_DBA_FailedConnectionTracker] ([login_name], [program_name]);
		
end 


if object_id('dbo.T_DBA_FailedConnectionTracker_XE_Log') is null
begin
	print 'Table [T_DBA_FailedConnectionTracker_XE_Log] Created';
	CREATE TABLE [dbo].[T_DBA_FailedConnectionTracker_XE_Log](
		[timestamp_utc] [datetime2](7) NOT NULL PRIMARY KEY,
		[file_name] [nvarchar](500) NOT NULL,
		[file_offset] [bigint] NOT NULL
		
	) 
end

set nocount on;


/*
Must be BLOB Storage V2 or later !!
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

-- N'https://<AzureStorageAccount>.blob.core.windows.net/<ServerName>/DBA_XE_FailedLogins_20211007_0705.xel',

if exists ( Select *
			from sys.dm_xe_sessions
			where name = 'DBA_TrackFailedLogins' )
BEGIN
	DROP EVENT SESSION DBA_TrackFailedLogins ON SERVER
END
GO

Declare @xEFileName Nvarchar(1000)
Select top (1) @xEFileName = N'https://<AzureStorageAccount>.blob.core.windows.net/'+ value +'/'
	+ N'DBA_XE_TrackFailedLogins'
	+ N'_' + replace(replace(replace(convert(char(10),getdate(),121),'-',''),' ','_'),':','') + '_' + '.xel' -- extension mandatory for SQLMI
FROM string_split ( convert(varchar(500), SERVERPROPERTY('servername')), '.') 
;
--Select @xEFileName

Declare @XEDDL Nvarchar(max) = '
CREATE EVENT SESSION DBA_TrackFailedLogins
ON SERVER
 ADD EVENT sqlserver.error_reported
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
    WHERE severity = 14
      AND error_number = 18456 
      AND state > 1 -- removes redundant state 1 event
  )
  ADD TARGET package0.event_file
    (SET
        filename = N'''+ @xEFileName +''',
        max_file_size = 25,
        max_rollover_files = 5
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

Print ''EVENT SESSION DBA_TrackFailedLogins installed'';
'

--Select @XEDDL as XEDDL

exec sys.sp_executesql @stmt = @XEDDL ;

ALTER EVENT SESSION DBA_TrackFailedLogins ON SERVER
  STATE = START;
GO

set nocount off
go

CREATE OR ALTER PROCEDURE [dbo].[spc_DBA_Process_XE_FailedConnectionTracker]     
		@Debug BIT = 0
AS
BEGIN
/*
Process XE connectiondata Files
20210915 Johan Bijnens 

exec spc_DBA_Process_XE_FailedConnectionTracker

exec spc_DBA_Process_XE_FailedConnectionTracker @Debug = 1

*/
-- Declare @Debug BIT = 1

    SET NOCOUNT ON;
    CREATE TABLE #XeEventData
    ([event_data]    [XML] NULL, 
        [file_name]     [NVARCHAR](260)  NOT NULL, 
        [file_offset]   [BIGINT] NOT NULL, 
        [timestamp_utc] DATETIME2(7) NOT NULL
    );
   
    DECLARE @XEFilename [NVARCHAR](500)
			, @XEfileoffset BIGINT
			, @wrkxml XML
			, @XERootFileName NVARCHAR(4000)
			, @RwCount int ;

	/* delete EX_Log > 24h */
	Delete 
	from dbo.T_DBA_failedConnectionTracker_XE_Log
	where timestamp_utc < dateadd(hh, datediff(hh, 0, SYSUTCDATETIME()) - 24 , 0 ) ;
	
	Set @RwCount = @@ROWCOUNT ;

	if @Debug = 1
	begin
		Select '000' Debug, @RwCount as nRows_Deleted_From_T_DBA_failedConnectionTracker_XE_Log
	end

	/* get session target information */
    SELECT TOP 1 @wrkxml = CONVERT(XML, target_data)
	-- select * 
    FROM sys.dm_xe_session_targets AS XST
    INNER JOIN sys.dm_xe_sessions AS XS
            ON XS.address = XST.event_session_address
    WHERE XS.name = 'DBA_TrackFailedLogins';
	
	Set @RwCount = @@ROWCOUNT ;

	if @Debug = 1
	begin
		Select '001' Debug,  @wrkxml as wrkxml
	end


    SELECT @XERootFileName = @wrkxml.value('(EventFileTarget/File/@name)[1]', 'nvarchar(4000)');
	/* only keep root part ( as used with the XE event creation ) */ 
    -- not for SQLMI SELECT @XERootFileName = SUBSTRING(@XERootFileName, 0, CHARINDEX('__', @XERootFileName) + 1);

	if @Debug = 1
	begin
		Select '010' Debug,  @XERootFileName as XERootFileName
	end

	/* get last recorde XE Log info */
    SELECT TOP (1) @XEFilename = [file_name]
                    , @XEfileoffset = file_offset
    -- select * 
	FROM dbo.T_DBA_FailedConnectionTracker_XE_Log
    ORDER BY timestamp_utc DESC;
	
	Set @RwCount = @@ROWCOUNT ;

    IF @RwCount = 0
        BEGIN
            SELECT @XEFilename = NULL
                 , @XEfileoffset = NULL;
        END;

	if @Debug = 1
	BEGIN
		/* path, mdpath, initial_file_name, initial_offset */
		Select @XERootFileName as XERootFileName, NULL as mdpath , @XEFilename as XEFilename, @XEfileoffset as XEfileoffset ;
	END

	BEGIN TRY  
     	INSERT INTO #XeEventData
            SELECT CONVERT(XML, event_data) AS event_data
                , file_name
                , file_offset
                , timestamp_utc
            FROM sys.fn_xe_file_target_read_file(
					  @XERootFileName -- not for SQLMI + '*.xel'
					, NULL  -- NULL for SQLMI 
					, @XEFilename, @XEfileoffset);
		Set @RwCount = @@ROWCOUNT ;
	END TRY  
	BEGIN CATCH  
		 INSERT INTO #XeEventData
            SELECT CONVERT(XML, event_data) AS event_data
                , file_name
                , file_offset
                , timestamp_utc
            FROM sys.fn_xe_file_target_read_file(
					  @XERootFileName -- not for SQLMI '*.xel'
					, NULL  -- NULL for SQLMI 
					, NULL, NULL);
		Set @RwCount = @@ROWCOUNT ;
	END CATCH  
	
	if @Debug = 1
	begin
		Select '020' Debug, @RwCount as  [Inserted_to_#XeEventData];

		Select  top (10) '021' Debug, *
		from #XeEventData 
		order by timestamp_utc
    end

	BEGIN TRY  
		/* store last row for XE_Log */
		INSERT INTO dbo.T_DBA_FailedConnectionTracker_XE_Log
            SELECT TOP (1) [timestamp_utc]
						, [file_name]
                        , [file_offset]
            FROM #XeEventData
            ORDER BY [timestamp_utc] DESC;
    
		Set @RwCount = @@ROWCOUNT ;

		if @Debug = 1
		begin
			Select '030' Debug, @RwCount as Inserted_to_T_DBA_FailedConnectionTracker_XE_Log
		end
	END TRY
    BEGIN CATCH
		-- no problem if conflictis occur
		if @Debug = 1
		begin
			SELECT '035' Debug
				 , ERROR_PROCEDURE() AS ERROR_PROCEDURE
				 , SUSER_SNAME() as SUSER_SNAME
				 , ERROR_NUMBER() as ERROR_NUMBER
				 , ERROR_SEVERITY() as ERROR_SEVERITY
				 , ERROR_STATE() as ERROR_SEVERITY
				 , ERROR_MESSAGE() as ERROR_SEVERITY

		end
    END CATCH

	BEGIN TRY
		;WITH cteTabular
            AS (SELECT 
					ED.event_data.value('(event/action[@name="client_hostname"]/value)[1]','nvarchar(4000)') as client_hostname,
					ED.event_data.value('(event/action[@name="client_app_name"]/value)[1]','nvarchar(4000)') as client_app_name,
					ED.event_data.value('(event/action[@name="username"]/value)[1]','nvarchar(4000)') as username,
					ED.event_data.value('(event/action[@name="nt_username"]/value)[1]','nvarchar(4000)') as nt_username,
					ED.event_data.value('(event/action[@name="session_nt_username"]/value)[1]','nvarchar(4000)') as session_nt_username,
					ED.event_data.value('(event/action[@name="database_id"]/value)[1]','int') as database_id,
					ED.event_data.value('(event/@timestamp)[1]','datetime2') as event_timestamp,
					ED.event_data.value('(event/data[@name="error_number"]/value)[1]','int') as [error],
					ED.event_data.value('(event/data[@name="state"]/value)[1]','tinyint') as [state],
					ED.event_data.value('(event/data[@name="message"]/value)[1]','nvarchar(250)') as [message],
					ED.event_data.value('(event/data[@name="destination"]/text)[1]','nvarchar(250)') as [destination],
					ED.event_data
				  FROM #XeEventData ED)
		/* insert rows */
		INSERT INTO dbo.T_DBA_FailedConnectionTracker([host_name]
													 , [program_name]
													 , [nt_user_name]
													 , [login_name]
													 , [original_login_name]
													 , [client_net_address]
													 , [Database_Name]
													 , [tsRegistration]
													 , [FailedLoginData]
													)
            --	OUTPUT inserted.*
            SELECT client_hostname
                , client_app_name
                , nt_username
                , substring ( message
							, charindex('''', message, (CHARINDEX('Login failed for ', message, 0) + 17)) + 1 
							, charindex('''', substring ( message, charindex('''', message, (CHARINDEX('Login failed for ', message, 0) + 17)) + 1 , 150), 0 ) - 1) as [login_name]
                , session_nt_username
				, replace(substring ( message, CHARINDEX(':', message, len(message) - 50) +2 , 50),']','') as IPAddress
				, db_name(database_id) as DbName 
                , event_timestamp
				, event_data
            FROM cteTabular
			where  [destination] like 'BUFFER%'
			;
			
		Set @RwCount = @@ROWCOUNT ;

		if @Debug = 1
		begin
			Select '040' Debug, @RwCount as Inserted_to_T_DBA_FailedConnectionTracker
		end
    END TRY
    BEGIN CATCH
		-- no problem if conflictis occur
		if @Debug = 1
		begin
			SELECT '045' Debug
				 , ERROR_PROCEDURE() AS ERROR_PROCEDURE
				 , SUSER_SNAME() as SUSER_SNAME
				 , ERROR_NUMBER() as ERROR_NUMBER
				 , ERROR_SEVERITY() as ERROR_SEVERITY
				 , ERROR_STATE() as ERROR_SEVERITY
				 , ERROR_MESSAGE() as ERROR_SEVERITY

		end
    END CATCH
	
	if @Debug = 1
	begin
		Select top ( 100 ) '050' Debug, *
		from dbo.T_DBA_FailedConnectionTracker with (nolock)
		order by [tsRegistration] desc
	end

    /* cleanup */
    DROP TABLE #XeEventData;

END;
GO

print 'Stored procedure spc_DBA_Process_XE_FailedConnectionTracker installed';

/*  

job DBA_Process_FailedConnectionTracker_XE

*/

USE [msdb]


/****** Object:  Job [DBA_Process_FailedConnectionTracker_XE]    Script Date: 22/09/2021 12:56:04 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 22/09/2021 12:56:04 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA_Process_FailedConnectionTracker_XE', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Process Failed ConnectionTracker XE', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'aweadmin', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [spc_DBA_Process_XE_ConnectionTracker]    Script Date: 22/09/2021 12:56:05 ******/
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
		@command=N'exec spc_DBA_Process_XE_FailedConnectionTracker /* @Debug = 1 */ ', 
		@database_name=N'DDBAServerPing', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Send email when failed]    Script Date: 22/09/2021 12:56:05 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Send email when failed', 
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
	set @subject = convert(varchar(128), serverproperty(''servername'')) + '': job DBA_Process_FailedConnectionTracker_XE failed !''
declare @body varchar(1000)
	set @body = convert(varchar(128), serverproperty(''servername'')) + '': job DBA_Process_FailedConnectionTracker_XE failed and needs to be checked ! '' + convert(char(26), getutcdate(),121) + '' utc''

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
Print 'job DBA_Process_FailedConnectionTracker_XE installed';
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

go

use DDBAServerPing
go