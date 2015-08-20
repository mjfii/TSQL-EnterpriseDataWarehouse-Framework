Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports EDW.Common.SqlClientOutbound

Namespace Common

#Region "Common Types"

    ''' <summary></summary>
    Public Enum SQLServerCompatibility As UShort
        SQLServer2008 = 100
        SQLServer2012 = 110
        SQLServer2014 = 120
        SQLServer2016 = 130
    End Enum

    ''' <summary></summary>
    Public Enum YesNoType As UShort
        Yes = 1
        No = 2
    End Enum

    ''' <summary></summary>
    Public Enum EntityType As UShort
        TypeII = 1
        TypeI = 2
    End Enum

    ''' <summary></summary>
    Public Enum BuildAction As UShort
        AddEntity = 1
        UpdateEntity = 2
        DeleteEntity = 3
        None = 4
    End Enum

    ''' <summary></summary>
    Public Enum AttributeType As UShort
        BusinessIdentifier = 2
        Atomic = 4
        Both = 8
    End Enum

    ''' <summary></summary>
    Public Enum SortOrderType As UShort
        asc = 1
        desc = 2
    End Enum

#End Region

    Partial Public Class SqlClientOutbound

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="Command"></param>
        ''' <param name="SqlCnn"></param>
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
        ''' <param name="Command"></param>
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
        ''' <param name="print_string"></param>
        ''' <param name="tab_spaces"></param>
        ''' <remarks></remarks>
        Friend Shared Sub PrintClientMessage(ByVal print_string As String,
                                             Optional tab_spaces As UShort = 0)

            Dim pl As New String(" "c, tab_spaces)

            Dim ns As String = pl & print_string

            While Len(ns) > 0
                SqlContext.Pipe.Send(Left(ns, 4000))
                ns = Right(ns, Len(ns) - If(Len(ns) < 4000, Len(ns), 4000))
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

    Partial Public Class InstanceSettings

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="SqlCnn"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Friend Shared Function SystemObjectsInstalled(ByVal SqlCnn As SqlConnection) As Boolean

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
        ''' <remarks></remarks>
        <Microsoft.SqlServer.Server.SqlProcedure> _
        Public Shared Sub InstallInstanceComponents()

            Dim SqlCnn As New SqlConnection("context connection=true")
            SqlCnn.Open()

            Try
                PrintHeader()

                If Not UserIsSysAdmin(SqlCnn) Then Exit Try

                AddInstanceObjects(SqlCnn)
                PrintClientMessage(" ")
                PrintClientMessage("The instance components have been successfully installed!")

            Catch ex As Exception
                PrintClientError(New StackFrame().GetMethod().Name, ex)
            End Try

            SqlCnn.Close()

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
                ' TODO: add view logic
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
        ''' <param name="SqlCnn"></param>
        ''' <remarks></remarks>
        Friend Shared Sub AddMetadataObjects(ByVal SqlCnn As SqlConnection)
            ' alert the tables arent there and make them
            ExecuteDDLCommand(My.Resources.SYS_MetadataTableDefinition, SqlCnn)
            PrintClientMessage(vbCrLf)
            PrintClientMessage("The ARA Framework was not ready for use. The required system tables have NOW been built; you can now use the [dbo].[ara_*]")
            PrintClientMessage("tables in the [master] database to add the metadata construct elements to build each of the ARA objects.")

        End Sub

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        Private Shared Sub PrintHeader()

            PrintClientMessage(My.Resources.SYS_SlalomTextArt1 & vbCrLf)
            PrintClientMessage("EDW Framework")
            PrintClientMessage("Slalom Consulting | Copyright © 2015 | www.slalom.com" & vbCrLf & vbCrLf)

        End Sub

    End Class ' InstanceSettings

End Namespace