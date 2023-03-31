USE [DCS-Shop]
GO

SELECT [OrderNo]
      ,[JobNo]
      ,[PartNo]
      ,[PartDesc]
      ,[DelType]
      ,[Qty]
      ,[DueDate]
      ,[DateComplete]
      ,[DeliveryTicketNo]
      ,[Comments]
      ,[Releases_ID]
      ,[ItemNo]
      ,[DestJobNo]
      ,[MfgJobNo]
      ,[BinLocation]
      ,[LotNo]
      ,[EDISoftItemNo]
      ,[LastModDate]
      ,[LastModUser]
  FROM [dbo].[Releases]

GO

