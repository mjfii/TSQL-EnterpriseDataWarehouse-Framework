Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports EDW.Common.SqlClientOutbound

Namespace Common

#Region "Common Types"

    ''' <summary>
    ''' A type defining the SQL Server compatibility level
    ''' </summary>
    ''' <remarks>
    ''' The type is used during the build to ensure the desired framework is compliant with the destination database.  The value is equated to the below query.
    ''' <code language = "sqlserver">
    '''select
    '''   [name], 
    '''   [compatibility_level]
    '''from 
    '''   sys.databases 
    '''where
    '''   [name]=N'edw_psa';
    ''' </code>
    ''' </remarks>
    Public Enum SQLServerCompatibility As UShort
        ''' <summary>
        ''' SQL Server 2008[-R2] : Compatibility 100
        ''' </summary>
        SQLServer2008 = 100
        ''' <summary>
        ''' SQL Server 2012 : Compatibility 110
        ''' </summary>
        SQLServer2012 = 110
        ''' <summary>
        ''' SQL Server 2014 : Compatibility 120
        ''' </summary>
        SQLServer2014 = 120
        ''' <summary>
        ''' SQL Server 2016 : Compatibility 130
        ''' </summary>
        SQLServer2016 = 130
    End Enum

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Public Enum YesNoType As UShort
        ''' <summary>
        ''' 
        ''' </summary>
        Yes = 1
        ''' <summary>
        ''' 
        ''' </summary>
        No = 2
    End Enum

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Public Enum EntityType As UShort
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        TypeI = 1104
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        TypeII = 1984
    End Enum

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Public Enum BuildAction As UShort
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        AddEntity = 1
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        UpdateEntity = 2
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        DeleteEntity = 3
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        None = 4
    End Enum

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Public Enum AttributeType As UShort
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        BusinessIdentifier = 2
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        Atomic = 4
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        Both = 8
    End Enum

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Public Enum SortOrderType As UShort
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        asc = 1
        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        desc = 2
    End Enum

#End Region

    ''' <summary>
    ''' A partial public class that holds six (6) methods to comunicate with the database catalog, connection user, or both.
    ''' </summary>
    ''' <remarks>
    ''' This class relies heavily on the .NET Framework Data Provider for SQL Server, as well as the Common Language Runtime ("CLR") namespace.
    ''' </remarks>
    ''' <example>
    ''' <code language = "vb" title = ".NET Framework Data Provider for SQL Server (Scalar Call)">
    ''' Dim cmd As New SqlCommand(Command, SqlCnn)
    ''' Dim ScalarObject As Object = cmd.ExecuteScalar()
    '''
    ''' If ScalarObject Is Nothing Then
    '''    Exit Try
    ''' Else
    '''    Return ScalarObject.ToString
    ''' End If
    ''' </code>
    ''' <code language = "vb" title = ".NET Framework Data Provider for SQL Server (DDL Execution)">
    ''' Dim cmd As New SqlCommand(Command, SqlCnn)
    '''
    ''' cmd.ExecuteNonQuery()
    ''' </code>
    ''' <code language = "vb" title = "CLR Namespace">
    ''' Dim cmd As New SqlCommand(Command, SqlCnn)
    ''' Dim rdr As SqlDataReader = cmd.ExecuteReader
    ''' SqlContext.Pipe.Send(rdr)
    ''' </code>
    ''' </example>
    ''' <seealso cref="Microsoft.SqlServer.Server"></seealso>
    ''' <seealso cref="System.Data.SqlClient"></seealso>
    Partial Public Class SqlClientOutbound

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="Command">DDL command string, e.g. <codeInline>create table [dbo].[x]...;</codeInline>.</param>
        ''' <param name="SqlCnn">Connection string created at time of execution</param>
        ''' <remarks></remarks>
        Friend Shared Sub ExecuteDDLCommand(ByVal Command As String,
                                            ByVal SqlCnn As SqlConnection)

            Try
                Dim cmd As New SqlCommand(Command, SqlCnn)

                cmd.ExecuteNonQuery()
            Catch ex As Exception
                PrintClientError(New StackFrame().GetMethod().Name, ex)
            End Try

        End Sub

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="Command">Scalar command string, e.g <codeInline>select [x] from [dbo].[x] where [x]=1;</codeInline>.</param>
        ''' <param name="SqlCnn"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Friend Shared Function ExecuteSQLScalar(ByVal Command As String,
                                                ByVal SqlCnn As SqlConnection) As String

            Try
                Dim cmd As New SqlCommand(Command, SqlCnn)

                Dim ScalarObject As Object = cmd.ExecuteScalar()

                If ScalarObject Is Nothing Then
                    Exit Try
                Else
                    Return ScalarObject.ToString
                End If

            Catch ex As Exception
                PrintClientError(New StackFrame().GetMethod().Name, ex)
            End Try

            Return String.Empty

        End Function

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="ExistingDataset"></param>
        ''' <param name="Command"></param>
        ''' <param name="SourceTableName"></param>
        ''' <param name="SqlCnn"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Friend Shared Function ReturnInternalResults(ByVal ExistingDataset As DataSet,
                                                     ByVal Command As String,
                                                     ByVal SourceTableName As String,
                                                     ByVal SqlCnn As SqlConnection) As DataSet

            Try

                Dim cmd As New SqlCommand(Command, SqlCnn)

                Dim da As New SqlDataAdapter(cmd)
                da.Fill(ExistingDataset, SourceTableName)
                Return ExistingDataset

            Catch ex As Exception
                PrintClientError(New StackFrame().GetMethod().Name, ex)
            End Try

            Return Nothing

        End Function

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="Command"></param>
        ''' <param name="SqlCnn"></param>
        ''' <param name="DatabaseName"></param>
        ''' <remarks></remarks>
        Friend Shared Sub ReturnClientResults(ByVal Command As String,
                                              ByVal SqlCnn As SqlConnection,
                                              Optional ByVal DatabaseName As String = "master")

            Try

                ExecuteDDLCommand("use [" & DatabaseName & "];", SqlCnn)

                Dim cmd As New SqlCommand(Command, SqlCnn)

                Dim rdr As SqlDataReader = cmd.ExecuteReader
                SqlContext.Pipe.Send(rdr)

            Catch ex As Exception
                PrintClientError(New StackFrame().GetMethod().Name, ex)
            End Try

        End Sub

        ''' <summary></summary>
        ''' <param name="PrintString"></param>
        ''' <param name="LeadingSpaces"></param>
        ''' <remarks></remarks>
        Friend Shared Sub PrintClientMessage(ByVal PrintString As String,
                                             Optional LeadingSpaces As UShort = 0)

            Dim Pad As New String(" "c, LeadingSpaces)

            Dim NewPrintString As String = Pad & PrintString

            While Len(NewPrintString) > 0
                SqlContext.Pipe.Send(Left(NewPrintString, 4000))
                NewPrintString = Right(NewPrintString, Len(NewPrintString) - If(Len(NewPrintString) < 4000, Len(NewPrintString), 4000))
            End While

        End Sub

        ''' <summary></summary>
        ''' <param name="ErrorMethod"></param>
        ''' <param name="ErrorException"></param>
        ''' <param name="LeadingSpaces"></param>
        ''' <remarks></remarks>
        Friend Shared Sub PrintClientError(ByVal ErrorMethod As String,
                                           ByVal ErrorException As Exception,
                                           Optional ByVal LeadingSpaces As UShort = 0)

            SqlContext.Pipe.Send(New String(" "c, LeadingSpaces) & "Error on method: " & ErrorMethod)
            SqlContext.Pipe.Send(New String(" "c, LeadingSpaces) & "Exception message: " & ErrorException.Message.ToString)

        End Sub

    End Class ' SqlClientOutbound

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Partial Public Class InstanceSettings

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="SqlCnn"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Friend Shared Function MetadataObjectsInstalled(ByVal SqlCnn As SqlConnection) As Boolean

            ExecuteDDLCommand("use [master];", SqlCnn)

            Dim cmd As SqlCommand
            Dim oid As Integer

            ' PSA objects
            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'psa_attribute_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = CInt(cmd.ExecuteScalar())

            If oid = 0 Then Common.InstanceSettings.AddMetadataObjects(SqlCnn) : Return False

            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'psa_entity_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = CInt(cmd.ExecuteScalar())

            If oid = 0 Then Common.InstanceSettings.AddMetadataObjects(SqlCnn) : Return False

            ' ARA object
            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'ara_attribute_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = CInt(cmd.ExecuteScalar())

            If oid = 0 Then Common.InstanceSettings.AddMetadataObjects(SqlCnn) : Return False

            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'ara_entity_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = CInt(cmd.ExecuteScalar())

            If oid = 0 Then Common.InstanceSettings.AddMetadataObjects(SqlCnn) : Return False

            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'ara_abstract_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = CInt(cmd.ExecuteScalar())

            If oid = 0 Then Common.InstanceSettings.AddMetadataObjects(SqlCnn) : Return False

            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'ara_abstract_column_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = CInt(cmd.ExecuteScalar())

            If oid = 0 Then Common.InstanceSettings.AddMetadataObjects(SqlCnn) : Return False

            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'ara_attribute_mapping' and [schema_id]=1;select @x;", SqlCnn)
            oid = CInt(cmd.ExecuteScalar())

            If oid = 0 Then Common.InstanceSettings.AddMetadataObjects(SqlCnn) : Return False

            Return True
        End Function

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="SqlCnn"></param>
        ''' <remarks></remarks>
        Friend Shared Sub AddMetadataObjects(ByVal SqlCnn As SqlConnection)
            ' alert the tables arent there and make them
            ExecuteDDLCommand(My.Resources.SYS_MetadataDefinition, SqlCnn)
            PrintClientMessage(vbCrLf)
            PrintClientMessage("The metadata framework was not ready for use. The required system tables have NOW been built; you can now use the [ara | psa | oda]")
            PrintClientMessage("system tables in the [master] database to add the metadata construct elements to build each of the objects.")

        End Sub

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="SqlCnn"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Friend Shared Function UserIsSysAdmin(SqlCnn As SqlConnection) As Boolean

            Dim SqlCmd As New SqlCommand
            Dim oid As Integer

            With SqlCmd
                .Connection = SqlCnn
                .CommandText = "select is_srvrolemember(N'sysadmin') [ninja];"
                oid = CInt(SqlCmd.ExecuteScalar())
            End With

            If oid = 0 Then
                PrintClientMessage(vbCrLf)
                PrintClientMessage("You need elevated privileges (sysadmin) to conduct this framework task. Contact the database administrator.")
                Return False
            End If

            Return True
        End Function

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="DatabaseName"></param>
        ''' <param name="SqlConn"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Friend Shared Function DatabaseIsValid(DatabaseName As String, SqlConn As SqlConnection) As Boolean

            Dim SqlCmd As New SqlCommand
            Dim DatabaseID As Integer

            With SqlCmd
                .Connection = SqlConn
                .Parameters.Add("@DatabaseName", SqlDbType.NVarChar).Value = DatabaseName
                .CommandText = "select isnull(db_id(@DatabaseName),-1) N'?';"
                DatabaseID = CInt(.ExecuteScalar.ToString)
            End With

            ' if i have a invalid database id then return false and alert
            If DatabaseID < 0 Then
                PrintClientMessage(vbCrLf)
                PrintClientMessage(String.Format("A database with the name [{0}] does not exist.  Create that database or use an alternate one.", DatabaseName))
                Return False
            End If

            Return True
        End Function

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="InstanceConnection"></param>
        ''' <remarks></remarks>
        Friend Shared Sub AddInstanceObjects(ByVal InstanceConnection As SqlConnection)

            Try
                Dim chngstr As String = "use [master];"
                Dim chngdb As New SqlCommand(chngstr, InstanceConnection)
                chngdb.ExecuteNonQuery()

                ExecuteDDLCommand(My.Resources.SYS_TableMetadataDefinition, InstanceConnection)
                ExecuteDDLCommand(My.Resources.SYS_ColumnMetadataDefinition, InstanceConnection)
                PrintClientMessage("• Metadata managers in place [instance]")

                ExecuteDDLCommand(My.Resources.PSA_ServiceBrokerLoginDefinition, InstanceConnection)
                PrintClientMessage("• Service broker login exists [instance]")
            Catch ex As Exception
                PrintClientMessage(New StackFrame().GetMethod().Name)
                PrintClientMessage(ex.Message)
            End Try

        End Sub

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        Private Shared Sub PrintHeader()

            PrintClientMessage(My.Resources.SYS_SlalomTextArt1 & vbCrLf)
            PrintClientMessage("Enterprise Data Warehouse ('EDW') Framework")
            PrintClientMessage("Slalom Consulting | Copyright © 2014 | www.slalom.com" & vbCrLf & vbCrLf)

        End Sub

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        <Microsoft.SqlServer.Server.SqlProcedure> _
        Public Shared Sub InstallInstanceComponents()

            Dim SqlCnn As New SqlConnection("context connection=true")
            SqlCnn.Open()

            Try
                PrintHeader()

                If Not UserIsSysAdmin(SqlCnn) Then Exit Try

                AddInstanceObjects(SqlCnn)
                MetadataObjectsInstalled(SqlCnn)

                PrintClientMessage(" ")
                PrintClientMessage("The instance components have been successfully installed!")

            Catch ex As Exception
                PrintClientError(New StackFrame().GetMethod().Name, ex)
            End Try

            SqlCnn.Close()

        End Sub

    End Class ' InstanceSettings

End Namespace ' Common