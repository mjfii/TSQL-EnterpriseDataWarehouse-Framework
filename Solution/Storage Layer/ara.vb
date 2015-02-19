Imports System
Imports System.Data
Imports System.Data.Sql
Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports System.Runtime.InteropServices
Imports StorageLayer.Common.SqlClientOutbound
Imports StorageLayer.AnalyticReportingArea.FrameworkInstallation

Public Class ARA

#Region "CLR Exposed Methods"

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub BuildEntities(DatabaseSchema As String, DatabaseObject As String)

        ' convert database null values to empty strings
        If IsNothing(DatabaseSchema) Then DatabaseSchema = ""
        If IsNothing(DatabaseObject) Then DatabaseObject = ""

        'Dim cnn As New SqlConnection("Data Source=mjfii\demo;Initial Catalog=master;Integrated Security=SSPI")
        Dim cnn As New SqlConnection("context connection=true")
        cnn.Open()

        PrintClientMessage("EDW Framework - Analytics & Reporting Area ('ARA') Definition")
        PrintClientMessage("Slalom Consulting | Copyright © 2014 | www.slalom.com" & vbCrLf & vbCrLf)

        cnn.Close()
    End Sub

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub DropEntities(DatabaseSchema As String, DatabaseObject As String)

        ' convert database null values to empty strings
        If IsNothing(DatabaseSchema) Then DatabaseSchema = ""
        If IsNothing(DatabaseObject) Then DatabaseObject = ""

    End Sub

#End Region

End Class
