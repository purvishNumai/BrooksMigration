USE [EDW_PROD]
GO

/****** Object:  StoredProcedure [dbo].[edw_log_start_job]    Script Date: 12/10/2025 1:26:46 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[edw_log_start_job] (@PROCESS_NAME VARCHAR(300)) AS
BEGIN
--
-- set process to zombie state if there is a process that system thinks is still processing.
--
UPDATE REF.EDW_PROCESSING_LOG SET STATUS = 'Z', message_text = 'Unable to determine process status.',completion_date = GETDATE() 
WHERE process_name = @PROCESS_NAME 
AND STATUS = 'P';
--
--
INSERT INTO REF.EDW_PROCESSING_LOG (PROCESS_NAME, START_DATE, COMPLETION_DATE, STATUS, MESSAGE_TEXT)
VALUES(@PROCESS_NAME,getdate(),null,'P','Processing');
END;
GO


