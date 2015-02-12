Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports System.Security.Cryptography.SHA1
Imports EDW.Common.SqlClientOutbound

Namespace Common

    Partial Public Class Methods

        <Microsoft.SqlServer.Server.SqlFunction> _
        Public Shared Function StringHash(ByVal HashingString As String) As SqlBinary
            Return If(IsNothing(HashingString), New SqlBinary, Security.Cryptography.SHA1.Create.ComputeHash(Text.UnicodeEncoding.Unicode.GetBytes(HashingString)))
        End Function

    End Class ' Methods

    Partial Public Class Aggregates


    End Class ' Aggregates

    Partial Public Class SqlClientOutbound

        Friend Shared Sub ExecuteDDLCommand(ByVal cmd_string As String, cnn_obj As SqlConnection)
            Dim cmd As New SqlCommand(cmd_string, cnn_obj)
            cmd.ExecuteNonQuery()
        End Sub

        Friend Shared Sub PrintClientMessage(ByVal print_string As String, Optional tab_spaces As UShort = 0)
            Dim pl As New String(" ", tab_spaces)
            SqlContext.Pipe.Send(pl & print_string)
        End Sub

        Friend Shared Sub ReturnClientResults(ByVal sql_command As SqlCommand)
            Dim x As SqlDataReader = sql_command.ExecuteReader
            SqlContext.Pipe.Send(x)
        End Sub

    End Class ' OutboundMethods

End Namespace

Namespace PersistentStagingArea

    Partial Public Class FrameworkInstallation

        Friend Shared Function GetMetadata(ByVal DatabaseName As String, _
                                           ByVal DatabaseSchema As String, _
                                           ByVal DatabaseEntity As String, _
                                           ByVal DatabaseConnection As SqlConnection) As DataSet

            GetMetadata = New DataSet

            ' handle null values by flipping them to an empty string
            If IsNothing(DatabaseName) Then DatabaseName = ""
            If IsNothing(DatabaseSchema) Then DatabaseSchema = ""
            If IsNothing(DatabaseEntity) Then DatabaseEntity = ""

            If Not SystemObjectsInstalled(DatabaseConnection) Then ' test database exists and filegroups exist
                PrintClientMessage("The existing PSA framework is not valid. Entity Build aborted.")
                ' message on how to install
            Else

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
                Dim cmd As New SqlCommand(EntityString, DatabaseConnection)
                Dim da As New SqlDataAdapter(cmd)
                da.Fill(GetMetadata, "psa_entity_definition")

                cmd = New SqlCommand(AttributeString, DatabaseConnection)
                da = New SqlDataAdapter(cmd)
                da.Fill(GetMetadata, "psa_attribute_definition")

                cmd = New SqlCommand(My.Resources.PSA_InstanceProperties, DatabaseConnection)
                da = New SqlDataAdapter(cmd)
                da.Fill(GetMetadata, "psa_instance_properties")

                ExecuteDDLCommand(My.Resources.SYS_TableMetadataDefinition, DatabaseConnection)
                ExecuteDDLCommand(My.Resources.SYS_ColumnMetadataDefinition, DatabaseConnection)
                PrintClientMessage("• Metadata managers in place")

                ' this command is under whatever we find in the above cmd
                Dim chngstr As String = "use " & DatabaseName & ";"
                Dim chngdb As New SqlCommand(chngstr, DatabaseConnection)
                chngdb.ExecuteNonQuery()

                ExecuteDDLCommand(My.Resources.PSA_RoleDefinitions, DatabaseConnection)
                PrintClientMessage("• Database Role Requirements sync'd")

                ExecuteDDLCommand(Replace(My.Resources.PSA_DatabaseChangeTrackingDefinition, "{{{db}}}", DatabaseName), DatabaseConnection)
                ExecuteDDLCommand(My.Resources.PSA_ChangeTrackingSystemDefinition, DatabaseConnection)
                PrintClientMessage("• Change Tracking methodology in place")

                cmd = New SqlCommand(My.Resources.SYS_DatabaseProperties, DatabaseConnection)
                da = New SqlDataAdapter(cmd)
                da.Fill(GetMetadata, "psa_database_properties")

            End If

        End Function

        Friend Shared Function SystemObjectsInstalled(SqlCnn As SqlConnection) As Boolean

            ' TODO: install objects if not there

            Dim cmd As New SqlCommand("select [object_id] from sys.objects where [name]=N'psa_attribute_definition' and [schema_id]=1;", SqlCnn)
            Dim oid As Integer = cmd.ExecuteScalar()

            If oid = 0 Then
                PrintClientMessage("System table [dbo].[psa_attribute_definition] does not exist. Contact the database administrator.")
                Return False
            End If

            cmd = New SqlCommand("select [object_id] from sys.objects where [name]=N'psa_entity_definition' and [schema_id]=1;", SqlCnn)
            oid = cmd.ExecuteScalar()

            If oid = 0 Then
                PrintClientMessage("System table [dbo].[psa_entity_definition] does not exist. Contact the database administrator.")
                Return False
            End If

            Return True
        End Function

    End Class

End Namespace

Namespace AnalyticReportingArea

    Partial Public Class FrameworkInstallation

        Friend Shared Function GetMetadata(ByVal entity_cmd_string As String, ByVal attribute_cmd_string As String, sqlcnn As SqlConnection) As DataSet

            GetMetadata = New DataSet

            If Not SystemObjectsInstalled(sqlcnn) Then
                PrintClientMessage("The existing PSA framework is not valid. Entity Build aborted.")
            Else


            End If

        End Function

        Friend Shared Function SystemObjectsInstalled(SqlCnn As SqlConnection) As Boolean

            Return True
        End Function

        Friend Shared Sub InstallSystemObjects()

        End Sub

    End Class

End Namespace

Namespace LoggingArea

    Partial Public Class FrameworkInstallation

    End Class

End Namespace