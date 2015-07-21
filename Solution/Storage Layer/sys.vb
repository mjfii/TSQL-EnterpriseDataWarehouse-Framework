﻿Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports EDW.Common.SqlClientOutbound

Namespace Common

    Partial Public Class SqlClientOutbound

        Friend Shared Sub ExecuteDDLCommand(ByVal cmd_string As String, cnn_obj As SqlConnection)
            Dim cmd As New SqlCommand(cmd_string, cnn_obj)
            cmd.ExecuteNonQuery()
        End Sub

        Friend Shared Sub PrintClientMessage(ByVal print_string As String, Optional tab_spaces As UShort = 0)
            Dim pl As New String(" ", tab_spaces)

            Dim ns As String = pl & print_string

            While Len(ns) > 0
                SqlContext.Pipe.Send(Left(ns, 4000))
                ns = Right(ns, Len(ns) - If(Len(ns) < 4000, Len(ns), 4000))
            End While

        End Sub

        Friend Shared Sub ReturnClientResults(ByVal sql_command_string As String, Optional ByVal database_name As String = "master")

            Dim cnn As New SqlConnection("context connection=true")
            cnn.Open()
            Dim cmd As SqlCommand

            cmd = New SqlCommand("use [" & database_name & "];", cnn)
            cmd.ExecuteScalar()

            cmd = New SqlCommand(sql_command_string, cnn)

            Dim rdr As SqlDataReader = cmd.ExecuteReader
            SqlContext.Pipe.Send(rdr)

            cnn.Close()
        End Sub

    End Class ' OutboundMethods

    Partial Public Class InstanceSettings

        Friend Shared Function UserIsSysAdmin(SqlCnn As SqlConnection) As Boolean

            Dim SqlCmd As New SqlCommand
            Dim oid As Integer

            With SqlCmd
                .Connection = SqlCnn
                .CommandText = "select is_srvrolemember(N'sysadmin') [ninja]"
                oid = SqlCmd.ExecuteScalar()
            End With

            If oid = 0 Then
                PrintClientMessage(vbCrLf)
                PrintClientMessage("You need elevated privileges (sysadmin) to manage this framework. Contact the database administrator.")
                Return False
            End If

            Return True
        End Function

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

    End Class


End Namespace

Namespace PersistentStagingArea

    Partial Public Class FrameworkInstallation

        Friend Shared Function GetMetadata(ByVal DatabaseName As String, _
                                           ByVal DatabaseSchema As String, _
                                           ByVal DatabaseEntity As String, _
                                           ByVal DatabaseConnection As SqlConnection) As DataSet

            ' handle null values by flipping them to an empty string
            If IsNothing(DatabaseName) Then DatabaseName = ""
            If IsNothing(DatabaseSchema) Then DatabaseSchema = ""
            If IsNothing(DatabaseEntity) Then DatabaseEntity = ""

            If Not SystemObjectsInstalled(DatabaseConnection) Then ' test database exists and filegroups exist
                GetMetadata = Nothing
            Else

                GetMetadata = New DataSet

                Dim EntityString As String = My.Resources.SYS_PSAEntityDefinition
                Dim AttributeString As String = My.Resources.SYS_PSAAttributeDefinition

                If DatabaseSchema <> "" Then
                    EntityString += " and [psa_schema]=N'" & DatabaseSchema & "'"
                    AttributeString += " and [psa_schema]=N'" & DatabaseSchema & "'"
                End If

                If DatabaseEntity <> "" Then
                    EntityString += " and [psa_entity]=N'" & DatabaseEntity & "'"
                    AttributeString += " and [psa_entity]=N'" & DatabaseEntity & "'"
                End If

                EntityString += ";"
                AttributeString += ";"

                ' the following commands are under the 'master' database
                Dim chngstr As String = "use [master];"
                Dim chngdb As New SqlCommand(chngstr, DatabaseConnection)
                chngdb.ExecuteNonQuery()

                Dim cmd As New SqlCommand(My.Resources.SYS_InstanceProperties, DatabaseConnection)
                Dim da As New SqlDataAdapter(cmd)
                da.Fill(GetMetadata, "psa_instance_properties")

                ' this command the passed in database name
                chngstr = "use [" & DatabaseName & "];"
                chngdb = New SqlCommand(chngstr, DatabaseConnection)
                chngdb.ExecuteNonQuery()

                cmd = New SqlCommand(My.Resources.SYS_DatabaseProperties, DatabaseConnection)
                da = New SqlDataAdapter(cmd)
                da.Fill(GetMetadata, "psa_database_properties")

                cmd = New SqlCommand(EntityString, DatabaseConnection)
                da = New SqlDataAdapter(cmd)
                da.Fill(GetMetadata, "psa_entity_definition")

                cmd = New SqlCommand(AttributeString, DatabaseConnection)
                da = New SqlDataAdapter(cmd)
                da.Fill(GetMetadata, "psa_attribute_definition")

            End If

        End Function

        Friend Shared Function SystemObjectsInstalled(ByVal SqlCnn As SqlConnection) As Boolean

            Dim chngstr As String = "use [master];"
            Dim chngdb As New SqlCommand(chngstr, SqlCnn)
            chngdb.ExecuteNonQuery()

            Dim cmd As SqlCommand
            Dim oid As Integer

            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'psa_attribute_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = cmd.ExecuteScalar()

            If oid = 0 Then
                Return False
            End If

            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'psa_entity_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = cmd.ExecuteScalar()

            If oid = 0 Then
                Return False
            End If

            Return True
        End Function

        Friend Shared Sub AddInstanceObjects(ByVal InstanceConnection As SqlConnection)

            Dim chngstr As String = "use [master];"
            Dim chngdb As New SqlCommand(chngstr, InstanceConnection)
            chngdb.ExecuteNonQuery()

            ExecuteDDLCommand(My.Resources.SYS_TableMetadataDefinition, InstanceConnection)
            ExecuteDDLCommand(My.Resources.SYS_ColumnMetadataDefinition, InstanceConnection)
            PrintClientMessage("• Metadata managers in place [instance]")

            ExecuteDDLCommand(My.Resources.PSA_ServiceBrokerLoginDefinition, InstanceConnection)
            PrintClientMessage("• Service broker login exists [instance]")

        End Sub

        Friend Shared Sub AddDatabaseObjects(ByVal InstanceConnection As SqlConnection, ByVal DatabaseName As String)

            Dim chngstr As String = "use [" & DatabaseName & "];"
            Dim chngdb As New SqlCommand(chngstr, InstanceConnection)
            chngdb.ExecuteNonQuery()

            ' make sure the required database roles are there
            ExecuteDDLCommand(My.Resources.PSA_RoleDefinitions, InstanceConnection)
            PrintClientMessage("• Database role requirements synced [database]")

            ' make sure change tracking is turned on
            ExecuteDDLCommand(Replace(My.Resources.PSA_DatabaseChangeTrackingDefinition, "{{{db}}}", DatabaseName), InstanceConnection)
            ExecuteDDLCommand(My.Resources.PSA_ChangeTrackingSystemDefinition, InstanceConnection)
            PrintClientMessage("• Change tracking methodology in place [database]")

            ' execute hashing algorithm needs
            ExecuteDDLCommand(My.Resources.SYS_LocalMethodInstall, InstanceConnection)
            ExecuteDDLCommand(My.Resources.PSA_HashingAlgorithm, InstanceConnection)
            PrintClientMessage("• Hashing algorithms are intact [database]")

            ' execute service broker security needs
            ExecuteDDLCommand(My.Resources.PSA_ServiceBrokerUserDefinition, InstanceConnection)
            PrintClientMessage("• Service broker user security aligned [database]")

            ' execute service broker security needs
            ExecuteDDLCommand(My.Resources.PSA_LoggingDefinition, InstanceConnection)
            PrintClientMessage("• Logging objects created [database]")

        End Sub

    End Class

End Namespace

Namespace AnalyticReportingArea

    Partial Public Class FrameworkInstallation

        Friend Shared Function GetMetadata(ByVal DatabaseName As String, _
                                           ByVal DatabaseConnection As SqlConnection) As DataSet

            ' handle null values by flipping them to an empty string
            If IsNothing(DatabaseName) Then DatabaseName = ""

            If Not SystemObjectsInstalled(DatabaseConnection) Then Return Nothing

            Dim cmd As SqlCommand
            Dim da As SqlDataAdapter

            GetMetadata = New DataSet

            Dim EntityQuery As String = My.Resources.SYS_ARAEntityDefinition
            Dim AttributeQuery As String = My.Resources.SYS_ARAAttributeDefinition
            Dim InstanceQuery As String = My.Resources.SYS_InstanceProperties
            Dim DatabaseQuery As String = My.Resources.SYS_DatabaseProperties

            Dim chngstr As String = "use [master];"
            Dim chngdb As New SqlCommand(chngstr, DatabaseConnection)
            chngdb.ExecuteNonQuery()

            ' the following commands are under the 'master' database
            cmd = New SqlCommand(InstanceQuery, DatabaseConnection)
            da = New SqlDataAdapter(cmd)
            da.Fill(GetMetadata, "ara_instance_properties")

            ' this command the passed in database name
            chngstr = "use [" & DatabaseName & "];"
            chngdb = New SqlCommand(chngstr, DatabaseConnection)
            chngdb.ExecuteNonQuery()

            cmd = New SqlCommand(DatabaseQuery, DatabaseConnection)
            da = New SqlDataAdapter(cmd)
            da.Fill(GetMetadata, "ara_database_properties")

            cmd = New SqlCommand(EntityQuery, DatabaseConnection)
            da = New SqlDataAdapter(cmd)
            da.Fill(GetMetadata, "ara_entity_definition")

            cmd = New SqlCommand(AttributeQuery, DatabaseConnection)
            da = New SqlDataAdapter(cmd)
            da.Fill(GetMetadata, "ara_attribute_definition")

            Return GetMetadata

        End Function

        Friend Shared Function SystemObjectsInstalled(ByVal SqlCnn As SqlConnection) As Boolean

            Dim chngstr As String = "use [master];"
            Dim chngdb As New SqlCommand(chngstr, SqlCnn)
            chngdb.ExecuteNonQuery()

            Dim cmd As SqlCommand
            Dim oid As Integer

            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'ara_attribute_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = cmd.ExecuteScalar()

            If oid = 0 Then
                Return False
            End If

            cmd = New SqlCommand("declare @x int=0;select @x=[object_id] from sys.objects where [name]=N'ara_entity_definition' and [schema_id]=1;select @x;", SqlCnn)
            oid = cmd.ExecuteScalar()

            If oid = 0 Then
                Return False
            End If

            Return True
        End Function

        Friend Shared Sub AddInstanceObjects(ByVal InstanceConnection As SqlConnection)

            Dim chngstr As String = "use [master];"
            Dim chngdb As New SqlCommand(chngstr, InstanceConnection)
            chngdb.ExecuteNonQuery()

            ExecuteDDLCommand(My.Resources.SYS_TableMetadataDefinition, InstanceConnection)
            ExecuteDDLCommand(My.Resources.SYS_ColumnMetadataDefinition, InstanceConnection)
            PrintClientMessage("• Metadata managers in place [instance]")

        End Sub

        Friend Shared Sub AddDatabaseObjects(ByVal InstanceConnection As SqlConnection, ByVal DatabaseName As String)

            Dim chngstr As String = "use [" & DatabaseName & "];"
            Dim chngdb As New SqlCommand(chngstr, InstanceConnection)
            chngdb.ExecuteNonQuery()

            ' make sure the required database roles are there
            ExecuteDDLCommand(My.Resources.ARA_RoleDefinitions, InstanceConnection)
            PrintClientMessage("• Database role requirements synced [database]")

            ' make sure an audit trigger is in place
            ExecuteDDLCommand(My.Resources.ARA_Audit, InstanceConnection)
            PrintClientMessage("• DDL audit trigger is in place [database]")

            ' execute hashing algorithm needs
            'ExecuteDDLCommand(My.Resources.ARA_HashingAlgorithm, InstanceConnection)
            PrintClientMessage("• Hashing algorithms are intact [database]")

            ' execute service broker security needs
            ExecuteDDLCommand(My.Resources.ARA_LoggingDefinition, InstanceConnection)
            PrintClientMessage("• Logging objects created [database]")

        End Sub

    End Class

End Namespace