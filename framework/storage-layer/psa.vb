Imports System
Imports System.Data
Imports System.Data.Sql
Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports System.Runtime.InteropServices
Imports EDW.Common.SqlClientOutbound
Imports EDW.Common.InstanceSettings

''' <summary>
''' 
''' </summary>
''' <remarks></remarks>
Partial Public Class PSA

#Region "CLR Exposed Methods"

    ''' <summary>
    ''' This is the primary method that is called to create the database objects in the PSA.  Before executing the DDL, 
    ''' the method will ensure both required server and database level objects are in place.
    ''' </summary>
    ''' <param name="DatabaseName">The name of the database on the connected instance to build the entities.</param>
    ''' <param name="DatabaseSchema">The schema subset of domain objects to process.</param>
    ''' <param name="DatabaseEntity">The entity subset of domain objects to process.</param>
    ''' <remarks>
    ''' To snap-in this CLR method, use the following T-SQL, or some variant of it:
    ''' <code language = "sqlserver" numberLines="true">
    '''create procedure [dbo].[psa_build_entities]
    ''' (
    '''   @DatabaseName sysname,
    '''   @DatabaseSchema sysname,
    '''   @DatabaseObject sysname
    ''' )
    '''as external name [Slalom.Framework.StorageLayer].[EDW.PSA].[BuildEntities];
    '''go
    '''exec sys.sp_MS_marksystemobject 'psa_build_entities';
    '''go 
    ''' </code> 
    ''' </remarks>
    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub BuildEntities(ByVal DatabaseName As String, _
                                    ByVal DatabaseSchema As String, _
                                    ByVal DatabaseEntity As String)

        Dim SqlCnn As New SqlConnection("context connection=true")
        SqlCnn.Open()

        Try
            PrintHeader()

            ' if the user is part of the sysadmin role, move along
            If Not UserIsSysAdmin(SqlCnn) Then Exit Try

            ProcessCall(SqlCnn, DatabaseName, DatabaseSchema, DatabaseEntity, False, False)

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
        End Try

        SqlCnn.Close()

    End Sub

    ''' <summary>
    ''' This method is called to verify potential changes to the database objects in the PSA.
    ''' </summary>
    ''' <param name="DatabaseName">The name of the database on the connected instance to build the entities.</param>
    ''' <param name="DatabaseSchema">The schema subset of domain objects to process.</param>
    ''' <param name="DatabaseEntity">The entity subset of domain objects to process.</param>
    ''' <remarks>
    ''' To snap-in this CLR method, use the following T-SQL, or some variant of it:
    ''' <code language = "sqlserver" numberLines="true">
    '''create procedure [dbo].[psa_verify_entities]
    ''' (
    '''   @DatabaseName sysname,
    '''   @DatabaseSchema sysname,
    '''   @DatabaseObject sysname
    ''' )
    '''as external name [Slalom.Framework.StorageLayer].[EDW.PSA].[VerifyEntities];
    '''go
    '''exec sys.sp_MS_marksystemobject 'psa_verify_entities';
    '''go 
    ''' </code> 
    ''' </remarks>
    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub VerifyEntities(ByVal DatabaseName As String, _
                                     ByVal DatabaseSchema As String, _
                                     ByVal DatabaseEntity As String)

        Dim SqlCnn As New SqlConnection("context connection=true")
        SqlCnn.Open()

        Try
            PrintHeader()

            ' if the user is part of the sysadmin role, move along
            If Not UserIsSysAdmin(SqlCnn) Then Exit Try

            ProcessCall(SqlCnn, DatabaseName, DatabaseSchema, DatabaseEntity, True, False)

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
        End Try

        SqlCnn.Close()

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="DatabaseName">The name of the database on the connected instance to build the entities.</param>
    ''' <param name="DatabaseSchema">The schema subset of domain objects to process.</param>
    ''' <param name="DatabaseEntity">The entity subset of domain objects to process.</param>
    ''' <remarks>
    ''' To snap-in this CLR method, use the following T-SQL, or some variant of it:
    ''' <code language = "sqlserver" numberLines="true">
    '''create procedure [dbo].[psa_drop_entities]
    ''' (
    '''   @DatabaseName sysname,
    '''   @DatabaseSchema sysname,
    '''   @DatabaseObject sysname
    ''' )
    '''as external name [Slalom.Framework.StorageLayer].[EDW.PSA].[DropEntities];
    '''go
    '''exec sys.sp_MS_marksystemobject 'psa_drop_entities';
    '''go 
    ''' </code> 
    ''' </remarks>
    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub DropEntities(ByVal DatabaseName As String, _
                                    ByVal DatabaseSchema As String, _
                                    ByVal DatabaseEntity As String)

        Dim SqlCnn As New SqlConnection("context connection=true")
        SqlCnn.Open()

        Try
            PrintHeader()

            ' if the user is part of the sysadmin role, move along
            If Not UserIsSysAdmin(SqlCnn) Then Exit Try

            ProcessCall(SqlCnn, DatabaseName, DatabaseSchema, DatabaseEntity, False, True)

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
        End Try

        SqlCnn.Close()

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub GetModel

        Dim SqlCnn As New SqlConnection("context connection=true")
        SqlCnn.Open()

        Try
            PrintHeader()

            ' if the user is part of the sysadmin role, move along
            If Not UserIsSysAdmin(SqlCnn) Then Exit Try

            Dim model_query As String = My.Resources.PSA_GetModel
            ReturnClientResults(model_query, SqlCnn)
            PrintClientMessage("The model has been successfully exported.")

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
        End Try

        SqlCnn.Close()

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="ImportModel"></param>
    ''' <remarks></remarks>
    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub SetModel(ByVal ImportModel As SqlXml)

        Dim SqlCnn As New SqlConnection("context connection=true")
        SqlCnn.Open()

        Try
            PrintHeader()

            ' if the user is part of the sysadmin role, move along
            If Not UserIsSysAdmin(SqlCnn) Then Exit Try

            If IsNothing(ImportModel) Then PrintClientMessage("You cannot process a null model.") : Exit Try

            Dim model_query As String = Replace(My.Resources.PSA_SetModel, "{{{xml}}}", ImportModel.Value.ToString)
            ExecuteDDLCommand(model_query, SqlCnn)
            PrintClientMessage("The model has been successfully imported.")

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
        End Try

        SqlCnn.Close()

    End Sub

#End Region

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="SqlCnn"></param>
    ''' <param name="DatabaseName"></param>
    ''' <param name="DatabaseSchema"></param>
    ''' <param name="DatabaseEntity"></param>
    ''' <param name="vo"></param>
    ''' <param name="del"></param>
    ''' <remarks></remarks>
    Private Shared Sub ProcessCall(ByVal SqlCnn As SqlConnection,
                                   ByVal DatabaseName As String,
                                   ByVal DatabaseSchema As String,
                                   ByVal DatabaseEntity As String,
                                   ByVal vo As Boolean,
                                   ByVal del As Boolean)

        ' see if metadata tables are ready in master database
        If Not MetadataObjectsInstalled(SqlCnn) Then Exit Sub

        ' validate incoming database
        If Not DatabaseIsValid(DatabaseName, SqlCnn) Then Exit Sub

        ' make sure server objects are in place
        AddInstanceObjects(SqlCnn)

        ' make sure database objects are in place
        AddDatabaseObjects(SqlCnn, DatabaseName)

        Try

            ' get the metadata from system tables
            Dim md As DataSet = GetMetadata(DatabaseName, DatabaseSchema, DatabaseEntity, SqlCnn)

            If md Is Nothing Then PrintClientMessage("metadata fatal error?!?!?!") : Exit Try

            ' load up a variable with the usable construct
            Dim cons As New Construct(md)

            ' with we have records, we can begin with the DDL process, otherwise, alert client and exit
            If cons.EntityCount > 0 Then
                ProcessConstruct(cons, SqlCnn, vo, del)
            Else
                PrintClientMessage(vbCrLf)
                PrintClientMessage("There is not defined metadata for the PSA in [dbo].[psa_entity_definition] and/or [dbo].[psa_attribute_definition] in the [master] database.")
            End If ' metadata content exists

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
        End Try

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="pc"></param>
    ''' <param name="SqlCnn"></param>
    ''' <param name="VerifyOnly"></param>
    ''' <param name="DeleteObjects"></param>
    ''' <remarks></remarks>
    Private Shared Sub ProcessConstruct(pc As Construct, SqlCnn As SqlConnection, VerifyOnly As Boolean, DeleteObjects As Boolean)

        Dim st As Date = Now()
        Dim c As Integer = 0
        Dim e As Construct.Entity = Nothing
        Dim s As String = Nothing
        Dim fmt As String = "yyyy-MM-dd HH:mm:ss.ff"
        Dim ls As String ' logical signature
        Dim cs As String ' construct signature
        Dim lbl As String = "" ' updating label
        Dim plbl As String = "" ' printed label

        ' print header information
        PrintClientMessage(vbCrLf)
        PrintClientMessage("• Process construct started at " & st.ToString(fmt))
        PrintClientMessage("• Database Name: " & pc.DatabaseName)
        PrintClientMessage("• Database Compatibility: " & pc.DatabaseCompatibility.ToString)

        ' get entity count and labels to alert of process taking place
        If DeleteObjects = True Then
            lbl = "DELETE"
        Else
            If VerifyOnly = True Then
                lbl = "VERIFY"
            Else
                lbl = "BUILD"
            End If
        End If

        Dim i As Integer = pc.EntityCount
        If i = 1 Then
            PrintClientMessage("• Begin processing 1 PSA entity for " & lbl & ":" & vbCrLf & vbCrLf)
        Else
            PrintClientMessage("• Begin processing " & i.ToString & " PSA entities for " & lbl & ":" & vbCrLf & vbCrLf)
        End If

        lbl = ""

        ' move through each entity
        For c = 0 To (i - 1)

            ' initiate the transaction
            ExecuteDDLCommand("begin transaction;", SqlCnn)

            Try
                e = pc.Entities(c)

                ' check for no attributes
                If e.AttributeCount = 0 Then
                    lbl = "[NO COLUMNS DEFINED]"
                    GoTo nextc
                End If

                ' check for no attributes
                If e.BusinessIdentifierCount = 0 Then
                    lbl = "[NO BUSINESS IDENTIFIER DEFINED]"
                    GoTo nextc
                End If

                ' check for deletes
                If DeleteObjects = True Then
                    lbl = "[DELETED]"
                    DropAbstracts(e, SqlCnn)
                    DropEntity(e, SqlCnn)
                    GoTo nextc
                End If

                ' process schema def and sequence definition (these never go away)
                If VerifyOnly = False Then
                    ExecuteDDLCommand(e.SchemaDefinition, SqlCnn)
                    ExecuteDDLCommand(e.SequenceDefinition, SqlCnn)
                End If

                ' get signatures

                ls = ExecuteSQLScalar(e.LogicalSignatureLookup, SqlCnn)
                cs = ExecuteSQLScalar(e.ConstructSignatureLookup, SqlCnn)

                ' check for construct differences
                If cs = e.ConstructSignature Then
                    ' no construct changes, so check logical
                    If ls = e.LogicalSignature Then
                        lbl = "[NO CHANGE]"
                    Else
                        lbl = "[LOGICAL CHANGE]"
                        If VerifyOnly = False Then
                            DropAbstracts(e, SqlCnn)
                            BuildAbstracts(e, SqlCnn)
                        End If
                    End If
                Else
                    ' the construct differs, so rebuild entity in full
                    lbl = If(cs = "", "[NEW ENTITY]", "[PHYSICAL CHANGE]")
                    If VerifyOnly = False Then
                        DropAbstracts(e, SqlCnn)
                        DropEntity(e, SqlCnn)
                        BuildEntity(e, SqlCnn)
                        BuildAbstracts(e, SqlCnn)
                    End If
                End If

                ' make sure all the extended properties are in order
                If VerifyOnly = False Then
                    SyncMetadata(e, SqlCnn)
                End If

nextc:
                ' alert completion
                plbl = (c + 1).ToString
                plbl = New String(" "c, 4 - Len(plbl)) & plbl
                plbl += "  " & e.Domain
                PrintClientMessage(plbl + New String("."c, 48 - Len(plbl)) + lbl, 2)

                ExecuteDDLCommand("commit transaction;", SqlCnn)

            Catch ex As Exception

                ' alert completion
                lbl = "[ERROR]"
                plbl = (c + 1).ToString
                plbl = New String(" "c, 4 - Len(plbl)) & plbl
                plbl += "  " & e.Domain
                PrintClientMessage(plbl + New String("."c, 48 - Len(plbl)) + lbl, 2)

                PrintClientMessage("There was an error building " & e.Domain & ".", 8)
                PrintClientMessage(ex.Message, 8)
                PrintClientMessage(vbCrLf)

                ExecuteDDLCommand("rollback transaction;", SqlCnn)

            End Try

        Next c

        Dim ed As Date = Now()
        PrintClientMessage(vbCrLf & "• Process ended at " & ed.ToString(fmt))

        Dim min As Integer = CInt(DateDiff(DateInterval.Minute, st, ed))
        Dim sec As Integer = CInt(DateDiff(DateInterval.Second, st, ed) Mod 60)
        PrintClientMessage("• Time to execute " & min.ToString & " min(s) " & sec.ToString & " sec(s)")

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="e"></param>
    ''' <param name="SqlCnn"></param>
    ''' <remarks></remarks>
    Private Shared Sub DropEntity(e As Construct.Entity, ByVal SqlCnn As SqlConnection)

        ExecuteDDLCommand(e.RenameDefintion, SqlCnn)

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="e"></param>
    ''' <param name="SqlCnn"></param>
    ''' <remarks></remarks>
    Private Shared Sub BuildEntity(e As Construct.Entity, ByVal SqlCnn As SqlConnection)

        ExecuteDDLCommand(e.TableDefinition, SqlCnn)

        Dim oid As String = ExecuteSQLScalar("select object_id(N'" & e.Domain & "') [oid]", SqlCnn)
        e.ObjectID = CInt(oid)

        ExecuteDDLCommand(e.ChangeTrackingDefinition, SqlCnn)
        ExecuteDDLCommand(e.AlternateKeyStatsDefinition, SqlCnn)
        ExecuteDDLCommand(e.DropTemporalGovernorDefinition, SqlCnn)
        ExecuteDDLCommand(e.TemporalGovernorDefinition, SqlCnn)

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="e"></param>
    ''' <param name="SqlCnn"></param>
    ''' <remarks></remarks>
    Private Shared Sub DropAbstracts(e As Construct.Entity, ByVal SqlCnn As SqlConnection)
        ExecuteDDLCommand(e.DropRelatedObjectsDefintion, SqlCnn)
    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="e"></param>
    ''' <param name="SqlCnn"></param>
    ''' <remarks></remarks>
    Private Shared Sub BuildAbstracts(e As Construct.Entity, ByVal SqlCnn As SqlConnection)

        If e.ObjectID = 0 Then
            Dim oid As String = ExecuteSQLScalar("select object_id(N'" & e.Domain & "') [oid]", SqlCnn)
            e.ObjectID = CInt(oid)
        End If

        ' control abstract
        ExecuteDDLCommand(e.ControlViewDefinition, SqlCnn)
        ExecuteDDLCommand(e.ControlInsertDefinition, SqlCnn)
        ExecuteDDLCommand(e.ControlUpdateDefinition, SqlCnn)
        ExecuteDDLCommand(e.ControlDeleteDefinition, SqlCnn)
        ExecuteDDLCommand(e.ControlSecurityDefinition, SqlCnn)

        ' as-is abstract
        ExecuteDDLCommand(e.AsIsViewDefinition, SqlCnn)
        ExecuteDDLCommand(e.AsIsTriggerDefinition, SqlCnn)
        ExecuteDDLCommand(e.AsIsSecurityDefinition, SqlCnn)

        ' as-was abstract
        ExecuteDDLCommand(e.AsWasViewDefinition, SqlCnn)
        ExecuteDDLCommand(e.AsWasTriggerDefinition, SqlCnn)
        ExecuteDDLCommand(e.AsWasSecurityDefinition, SqlCnn)

        ' batch-count abstract
        ExecuteDDLCommand(e.BatchCountViewDefinition, SqlCnn)
        ExecuteDDLCommand(e.BatchCountTriggerDefinition, SqlCnn)
        ExecuteDDLCommand(e.BatchCountSecurityDefinition, SqlCnn)

        ' as-of abstract
        ExecuteDDLCommand(e.AsOfFunctionDefinition, SqlCnn)
        ExecuteDDLCommand(e.AsOfSecurityDefinition, SqlCnn)

        ' changes abstract
        ExecuteDDLCommand(e.ChangesFunctionDefinition, SqlCnn)
        ExecuteDDLCommand(e.ChangesSecurityDefinition, SqlCnn)

        ' queues
        ExecuteDDLCommand(e.LoadStageDefinition, SqlCnn)

        ' methods
        ExecuteDDLCommand(e.ProcessUpsertDefintion, SqlCnn)
        ExecuteDDLCommand(e.ProcessUpsertSecurityDefinition, SqlCnn)
        ExecuteDDLCommand(e.WorkerUpsertDefintion, SqlCnn)

        ExecuteDDLCommand(e.ProcessDeleteDefintion, SqlCnn)
        ExecuteDDLCommand(e.ProcessDeleteSecurityDefinition, SqlCnn)
        ExecuteDDLCommand(e.WorkerDeleteDefintion, SqlCnn)



        ' service broker
        ExecuteDDLCommand(e.ServiceBrokerDefinition, SqlCnn)

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="e"></param>
    ''' <param name="SqlCnn"></param>
    ''' <remarks></remarks>
    Private Shared Sub SyncMetadata(e As Construct.Entity, ByVal SqlCnn As SqlConnection)

        ExecuteDDLCommand(e.EntityMetadataDefinition, SqlCnn)
        ExecuteDDLCommand(e.AttributeMetadataDefintion, SqlCnn)

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Private Shared Sub PrintHeader()

        PrintClientMessage(My.Resources.SYS_SlalomTextArt1 & vbCrLf)
        PrintClientMessage("EDW Framework - Persistent Staging Area ('PSA') Definition")
        PrintClientMessage("Slalom Consulting | Copyright © 2014 | www.slalom.com" & vbCrLf & vbCrLf)

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="InstanceConnection"></param>
    ''' <param name="DatabaseName"></param>
    ''' <remarks></remarks>
    Private Shared Sub AddDatabaseObjects(ByVal InstanceConnection As SqlConnection, ByVal DatabaseName As String)

        ExecuteDDLCommand("use [" & DatabaseName & "];", InstanceConnection)

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

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="DatabaseName"></param>
    ''' <param name="DatabaseSchema"></param>
    ''' <param name="DatabaseEntity"></param>
    ''' <param name="DatabaseConnection"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Shared Function GetMetadata(ByVal DatabaseName As String, _
                                        ByVal DatabaseSchema As String, _
                                        ByVal DatabaseEntity As String, _
                                        ByVal DatabaseConnection As SqlConnection) As DataSet

        Try

            ' handle null values by flipping them to an empty string
            If IsNothing(DatabaseName) Then DatabaseName = ""
            If IsNothing(DatabaseSchema) Then DatabaseSchema = ""
            If IsNothing(DatabaseEntity) Then DatabaseEntity = ""

            GetMetadata = New DataSet

            Dim InstanceString As String = My.Resources.SYS_InstanceProperties
            Dim DatabaseString As String = My.Resources.SYS_DatabaseProperties
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
            ExecuteDDLCommand("use [master];", DatabaseConnection)

            ReturnInternalResults(GetMetadata, InstanceString, "psa_instance_properties", DatabaseConnection)

            ' this command the passed in database name
            ExecuteDDLCommand("use [" & DatabaseName & "];", DatabaseConnection)

            ReturnInternalResults(GetMetadata, DatabaseString, "psa_database_properties", DatabaseConnection)

            ReturnInternalResults(GetMetadata, EntityString, "psa_entity_definition", DatabaseConnection)

            ReturnInternalResults(GetMetadata, AttributeString, "psa_attribute_definition", DatabaseConnection)

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
            Return Nothing
        End Try

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Class Construct

#Region "Construct Variables"
        Private _entities As Entity()
        Private _databasecompatibility As EDW.Common.SQLServerCompatibility
        Private _databasename As String
#End Region

#Region "Construct Properties"

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Friend ReadOnly Property EntityCount As Integer
            Get
                If IsNothing(_entities) Then
                    Return 0
                Else
                    Return _entities.Length
                End If
            End Get
        End Property

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <param name="EntityNumber"></param>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Friend ReadOnly Property Entities(EntityNumber As Integer) As Entity
            Get
                Return _entities(EntityNumber)
            End Get
        End Property

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Friend ReadOnly Property DatabaseName As String
            Get
                Return _databasename
            End Get
        End Property

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Friend ReadOnly Property DatabaseCompatibility As EDW.Common.SQLServerCompatibility
            Get
                Return _databasecompatibility
            End Get
        End Property

#End Region

#Region "Construct Constructors"

        ''' <summary></summary>
        Protected Friend Sub New(ByVal NewConstruct As DataSet)

            Try

                Dim dp As DataTable = NewConstruct.Tables("psa_database_properties")
                Dim EntityDefinition As DataTable = NewConstruct.Tables("psa_entity_definition")
                Dim adt As DataTable = NewConstruct.Tables("psa_attribute_definition")
                Dim e As Entity
                Dim edr As DataRow

                _databasename = CStr(dp.Rows(0).Item("database_name").ToString)
                _databasecompatibility = dp.Rows(0).Item("compatibility_level").ToString.StringToDatabaseCompatibility

                For Each edr In EntityDefinition.Rows

                    e = New Entity(edr("psa_schema").ToString,
                                   edr("psa_entity").ToString,
                                   edr("psa_entity_description").ToString.RemoveNulls,
                                   edr("source_schema").ToString,
                                   edr("source_entity").ToString,
                                   edr("psa_hash_large_objects").ToString,
                                   edr("etl_infer_deletions").ToString,
                                   edr("etl_build_group").ToString.RemoveNulls,
                                   CShort(edr("etl_sequence_order").ToString),
                                   edr("psa_logical_signature").ToString,
                                   edr("psa_construct_signature").ToString,
                                   CInt(edr("psa_max_threads")),
                                   CInt(edr("etl_max_record_count")),
                                   edr("etl_full_load").ToString,
                                   edr("etl_source_variables").ToString,
                                   edr("etl_fixed_predicate").ToString,
                                   edr("etl_variable_predicate").ToString
                                   )

                    For Each adr In adt.Select("psa_schema = '" & edr("psa_schema").ToString & "' and psa_entity = '" & edr("psa_entity").ToString & "'", "psa_attribute_ordinal")

                        e.AddEntityAttribute(adr("psa_attribute").ToString, _
                                             CInt(adr("psa_attribute_ordinal")), _
                                             adr("psa_attribute_datatype").ToString, _
                                             adr("psa_attribute_optional").ToString, _
                                             adr("psa_attribute_business_id").ToString, _
                                             adr("psa_attribute_index").ToString, _
                                             adr("psa_attribute_sort").ToString, _
                                             adr("psa_attribute_description").ToString.RemoveNulls)

                    Next

                    _entities.AddMember(e)
                Next

            Catch ex As Exception
                PrintClientError(New StackFrame().GetMethod().Name, ex)
            End Try

        End Sub

#End Region

        ''' <summary>
        ''' 
        ''' </summary>
        ''' <remarks></remarks>
        Class Entity

#Region "Entity Variables"
            Private _schema As String
            Private _entity As String
            Private _description As String
            Private _sourceschema As String
            Private _sourceentity As String
            Private _hashlargeobjects As EDW.Common.YesNoType
            Private _inferdeletions As EDW.Common.YesNoType
            Private _buildgroup As String
            Private _sequenceorder As Short
            Private _logicalsig As String
            Private _constructsig As String
            Private _maxthreads As Integer
            Private _maxrecordcount As Integer
            Private _objectid As Integer
            Private _fullload As EDW.Common.YesNoType
            Private _sourcevariables As String
            Private _fixedpredicate As String
            Private _variablepredicate As String
            Private _attribute As EntityAttribute()
#End Region

#Region "Entity Properties"

            ''' <summary></summary>
            Protected Friend Property Schema As String
                Get
                    Return _schema
                End Get
                Set(value As String)
                    _schema = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property Entity As String
                Get
                    Return _entity
                End Get
                Set(value As String)
                    _entity = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property Description As String
                Get
                    Return _description
                End Get
                Set(value As String)
                    _description = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property SourceSchema As String
                Get
                    Return _sourceschema
                End Get
                Set(value As String)
                    _sourceschema = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property SourceEntity As String
                Get
                    Return _sourceentity
                End Get
                Set(value As String)
                    _sourceentity = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property HashLargeObjects As EDW.Common.YesNoType
                Get
                    Return _hashlargeobjects
                End Get
                Set(value As EDW.Common.YesNoType)
                    _hashlargeobjects = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property MaxThreads As Integer
                Get
                    Return _maxthreads
                End Get
                Set(value As Integer)
                    _maxthreads = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property BuildGroup As String
                Get
                    Return _buildgroup
                End Get
                Set(value As String)
                    _buildgroup = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property MaxRecordCount As Integer
                Get
                    Return _maxrecordcount
                End Get
                Set(value As Integer)
                    _maxrecordcount = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property SequenceOrder As Short
                Get
                    Return _sequenceorder
                End Get
                Set(value As Short)
                    _sequenceorder = value
                End Set
            End Property

            ''' <summary></summary>
            Protected Friend Property InferDeletions As EDW.Common.YesNoType
                Get
                    Return _inferdeletions
                End Get
                Set(value As EDW.Common.YesNoType)
                    _inferdeletions = value
                End Set
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend Property FullLoad As EDW.Common.YesNoType
                Get
                    Return _fullload
                End Get
                Set(value As EDW.Common.YesNoType)
                    _fullload = value
                End Set
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend Property SourceVariables As String
                Get
                    Return _sourcevariables
                End Get
                Set(value As String)
                    _sourcevariables = value
                End Set
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend Property FixedPredicate As String
                Get
                    Return _fixedpredicate
                End Get
                Set(value As String)
                    _fixedpredicate = value
                End Set
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend Property VariablePredicate As String
                Get
                    Return _variablepredicate
                End Get
                Set(value As String)
                    _variablepredicate = value
                End Set
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend Property ObjectID As Integer
                Get
                    Return If(IsNothing(_objectid), 0, _objectid)
                End Get
                Set(value As Integer)
                    _objectid = value
                End Set
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend Property LogicalSignature As String

                Get
                    Return _logicalsig
                End Get
                Set(value As String)
                    _logicalsig = value
                End Set
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend Property ConstructSignature As String
                Get
                    Return _constructsig
                End Get
                Set(ByVal value As String)
                    _constructsig = value
                End Set
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend ReadOnly Property LogicalSignatureLookup As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_LogicalSignatureLookup
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend ReadOnly Property ConstructSignatureLookup As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_ConstructSignatureLookup
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AttributeCount As Integer
                Get
                    If _attribute Is Nothing Then
                        Return 0
                    Else
                        Return _attribute.Count
                    End If

                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property BusinessIdentifierCount As Integer
                Get
                    If _attribute Is Nothing Then
                        Return 0
                    Else
                        Return (Aggregate ea As Entity.EntityAttribute In _attribute Where ea.BusinessIdentifier = Common.YesNoType.Yes Into Count())
                    End If

                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property Domain As String
                Get
                    If Schema Is Nothing Or Entity Is Nothing Then
                        Return ""
                    Else
                        Return "[" & Schema & "].[" & Entity & "]"
                    End If
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property Label As String
                Get
                    If Schema Is Nothing Or Entity Is Nothing Then
                        Return ""
                    Else
                        Return Schema & "." & Entity
                    End If
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property SchemaDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_SchemaDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property SequenceDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_SequenceDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property DropRelatedObjectsDefintion As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_DropRelatedObjects
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd

                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property RenameDefintion As String
                Get
                    Dim n As Date = Now()
                    Dim ext As String = Year(n).ToString
                    ext += Right("0" & Month(n).ToString, 2)
                    ext += Right("0" & Day(n).ToString, 2)
                    ext += Right("0" & Hour(n).ToString, 2)
                    ext += Right("0" & Minute(n).ToString, 2)
                    ext += Right("0" & Second(n).ToString, 2)

                    Dim sd As String
                    sd = My.Resources.PSA_RenameDefinition
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{label}}}", Label)
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{ext}}}", ext)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property TableDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_TableDefinition
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{label}}}", Label)
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{bichunk}}}", BusinessIdentifierChunk)
                    sd = Replace(sd, "{{{akchunk}}}", AlternateKeyChunk)
                    sd = Replace(sd, "{{{attrchunk}}}", AttributeChunk)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AlternateKeyStatsDefinition As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""

                    For Each bia In _attribute
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.Yes And bia.Ordinal <> 1 Then
                            rstr += "create statistics [st : " & Label & " :: " & bia.Name & "] on " & Domain & vbCrLf & " (" & vbCrLf & "   [" & bia.Name & "]" & vbCrLf & " );" & vbCrLf & vbCrLf
                        End If
                    Next

                    Return If(rstr = "", "print 'no alternate key stats'", rstr)
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ChangeTrackingDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_ChangeTrackingDefinition
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property DropTemporalGovernorDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_DropTemporalGovernorDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{label}}}", Label)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property TemporalGovernorDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_TemporalGovernorDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{label}}}", Label)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{insertset}}}", ColumnSetChunk("", 6))
                    sd = Replace(sd, "{{{selectset}}}", ColumnSetChunk("d", 6))
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlViewDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 3)
                    cs = Left(cs, Len(cs) - 1) ' remove the last comma

                    Dim sd As String
                    sd = My.Resources.PSA_ControlViewDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{columnset}}}", cs)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlInsertDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 6)

                    Dim sd As String
                    sd = My.Resources.PSA_ControlInsertDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{label}}}", Label)
                    sd = Replace(sd, "{{{hashset}}}", HashChunk)
                    sd = Replace(sd, "{{{columnset}}}", cs)

                    If HashLargeObjects = EDW.Common.YesNoType.Yes Then
                        sd = Replace(sd, "{{{hashfunction}}}", "[dbo].[psa_hash]")
                        sd = Replace(sd, "{{{hashext}}}", "")
                    Else
                        sd = Replace(sd, "{{{hashfunction}}}", "hashbytes")
                        sd = Replace(sd, "{{{hashext}}}", "N'sha1',")
                    End If

                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlUpdateDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 9)

                    Dim sd As String
                    sd = My.Resources.PSA_ControlUpdateDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{label}}}", Label)
                    sd = Replace(sd, "{{{updateset}}}", UpdateChunk)
                    sd = Replace(sd, "{{{joinset}}}", ControlJoinChunk)
                    sd = Replace(sd, "{{{hashset}}}", HashChunk)
                    sd = Replace(sd, "{{{columnset}}}", cs)
                    sd = Replace(sd, "{{{updatekeyset}}}", UpdateKeyChunk)
                    If HashLargeObjects = EDW.Common.YesNoType.Yes Then
                        sd = Replace(sd, "{{{hashfunction}}}", "[dbo].[psa_hash]")
                        sd = Replace(sd, "{{{hashext}}}", "")
                    Else
                        sd = Replace(sd, "{{{hashfunction}}}", "hashbytes")
                        sd = Replace(sd, "{{{hashext}}}", "N'sha1',")
                    End If
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlDeleteDefinition As String
                Get
                    Dim cs As String = BusinessIdentifierColumnSetChunk("", 9)

                    Dim sd As String
                    sd = My.Resources.PSA_ControlDeleteDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{label}}}", Label)
                    sd = Replace(sd, "{{{updateset}}}", UpdateChunk)
                    sd = Replace(sd, "{{{joinset}}}", ControlJoinChunk)
                    sd = Replace(sd, "{{{hashset}}}", HashChunk)
                    sd = Replace(sd, "{{{columnset}}}", cs)

                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlSecurityDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_ControlSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AsIsViewDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 3)
                    cs = Left(cs, Len(cs) - 1) ' remove the last comma

                    Dim sd As String
                    sd = My.Resources.PSA_AsIsViewDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{columnset}}}", cs)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AsIsTriggerDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_AsIsTriggerDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{label}}}", Label)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AsIsSecurityDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_AsIsSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AsWasViewDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 3)
                    cs = Left(cs, Len(cs) - 1) ' remove the last comma

                    Dim sd As String
                    sd = My.Resources.PSA_AsWasViewDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{columnset}}}", cs)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AsWasTriggerDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_AsWasTriggerDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{label}}}", Label)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AsWasSecurityDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 3)

                    Dim sd As String
                    sd = My.Resources.PSA_AsWasSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property BatchCountViewDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_BatchCountViewDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property BatchCountTriggerDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_BatchCountTriggerDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{label}}}", Label)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property BatchCountSecurityDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_BatchCountSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AsOfFunctionDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 3)
                    cs = Left(cs, Len(cs) - 1) ' remove the last comma

                    Dim sd As String
                    sd = My.Resources.PSA_AsOfFunctionDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{columnset}}}", cs)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AsOfSecurityDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 3)

                    Dim sd As String
                    sd = My.Resources.PSA_AsOfSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ChangesFunctionDefinition As String
                Get
                    Dim cs As String = NewColumnSetChunk("d", 3, "", "", EDW.Common.AttributeType.BusinessIdentifier)

                    Dim sd As String
                    sd = My.Resources.PSA_ChangesFunctionDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{ak_columnset}}}", cs)
                    cs = NewColumnSetChunk("d", 3, "case when @NullOnDeletes=1 and d.[psa_dml_action]=N'D' then null else ", " end", EDW.Common.AttributeType.Atomic)
                    cs = Left(cs, Len(cs) - 1) ' remove the last comma
                    sd = Replace(sd, "{{{columnset}}}", cs)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ChangesSecurityDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_ChangesSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property LoadStageDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_LoadStageDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{bichunk}}}", BusinessIdentifierChunk)
                    sd = Replace(sd, "{{{akchunk}}}", AlternateKeyChunkForQueue)
                    sd = Replace(sd, "{{{attrchunk}}}", AttributeChunk)
                    Return sd
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ProcessUpsertDefintion As String
                Get
                    Dim d As String
                    d = My.Resources.PSA_ProcessUpsertDefinition
                    d = Replace(d, "{{{schema}}}", Schema)
                    d = Replace(d, "{{{entity}}}", Entity)
                    d = Replace(d, "{{{objectid}}}", ObjectID.ToString)

                    If ObjectID = 0 Then
                        d = Replace(d, "{{{updatestats}}}", "-- ")
                    Else
                        d = Replace(d, "{{{updatestats}}}", "")
                    End If

                    Return d
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ProcessUpsertSecurityDefinition As String
                Get
                    Dim d As String
                    d = My.Resources.PSA_ProcessUpsertSecurityDefinition
                    d = Replace(d, "{{{schema}}}", Schema)
                    d = Replace(d, "{{{entity}}}", Entity)
                    Return d
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property WorkerUpsertDefintion As String
                Get
                    Dim c As String = ColumnSetChunk("", 15)
                    c = Left(c, Len(c) - 1)
                    Dim i As String = ColumnSetChunk("s", 15)
                    i = Left(i, Len(i) - 1)
                    Dim u As String = UpdateChunk(15)
                    u = Left(u, Len(u) - 1)

                    Dim d As String
                    d = My.Resources.PSA_WorkerUpsertDefinition
                    d = Replace(d, "{{{schema}}}", Schema)
                    d = Replace(d, "{{{entity}}}", Entity)
                    d = Replace(d, "{{{columnset}}}", c)
                    d = Replace(d, "{{{insertset}}}", i)
                    d = Replace(d, "{{{updateset}}}", u)
                    d = Replace(d, "{{{mergejoinchunk}}}", MergeJoinChunk(28))

                    Return d
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ProcessDeleteDefintion As String
                Get
                    Dim d As String
                    d = My.Resources.PSA_ProcessDeleteDefinition
                    d = Replace(d, "{{{schema}}}", Schema)
                    d = Replace(d, "{{{entity}}}", Entity)
                    d = Replace(d, "{{{objectid}}}", ObjectID.ToString)

                    If ObjectID = 0 Then
                        d = Replace(d, "{{{updatestats}}}", "-- ")
                    Else
                        d = Replace(d, "{{{updatestats}}}", "")
                    End If

                    Return d
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ProcessDeleteSecurityDefinition As String
                Get
                    Dim d As String
                    d = My.Resources.PSA_ProcessDeleteSecurityDefinition
                    d = Replace(d, "{{{schema}}}", Schema)
                    d = Replace(d, "{{{entity}}}", Entity)
                    Return d
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property WorkerDeleteDefintion As String
                Get
                    Dim c As String = BusinessIdentifierColumnSetChunk("", 15)
                    c = Left(c, Len(c) - 1)

                    Dim d As String
                    d = My.Resources.PSA_WorkerDeleteDefinition
                    d = Replace(d, "{{{schema}}}", Schema)
                    d = Replace(d, "{{{entity}}}", Entity)
                    d = Replace(d, "{{{columnset}}}", c)
                    d = Replace(d, "{{{mergejoinchunk}}}", MergeJoinChunk(28))

                    Return d
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ServiceBrokerDefinition As String
                Get
                    Dim def As String
                    def = My.Resources.PSA_ServiceBrokerDefinition
                    def = Replace(def, "{{{schema}}}", Schema)
                    def = Replace(def, "{{{entity}}}", Entity)
                    def = Replace(def, "{{{threads}}}", MaxThreads.ToString)

                    Return def
                End Get
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend ReadOnly Property EntityMetadataDefinition As String
                Get
                    Dim ep As String = ""

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Domain")
                    ep = Replace(ep, "{{{value}}}", Domain)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Description")
                    ep = Replace(ep, "{{{value}}}", Description.EscapeTicks)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Source Schema")
                    ep = Replace(ep, "{{{value}}}", SourceSchema.EscapeTicks)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Source Entity")
                    ep = Replace(ep, "{{{value}}}", SourceEntity.EscapeTicks)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Use Large Object Hashing Algorithm")
                    ep = Replace(ep, "{{{value}}}", HashLargeObjects.ToString)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Infer Deletions from Source Domain")
                    ep = Replace(ep, "{{{value}}}", InferDeletions.ToString)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Build Group")
                    ep = Replace(ep, "{{{value}}}", BuildGroup.EscapeTicks)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Build Group Sequence Order")
                    ep = Replace(ep, "{{{value}}}", SequenceOrder.ToString)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Assembly")
                    ep = Replace(ep, "{{{value}}}", "Slalom.Framework")
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Copyright")
                    ep = Replace(ep, "{{{value}}}", "Slalom Consulting © 2014")
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Website")
                    ep = Replace(ep, "{{{value}}}", "www.slalom.com")
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Build Timestamp")
                    ep = Replace(ep, "{{{value}}}", Now.ToString)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Logical Signature")
                    ep = Replace(ep, "{{{value}}}", LogicalSignature)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Construct Signature")
                    ep = Replace(ep, "{{{value}}}", ConstructSignature)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Max Threads")
                    ep = Replace(ep, "{{{value}}}", MaxThreads.ToString)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Max Records Per Thread")
                    ep = Replace(ep, "{{{value}}}", MaxRecordCount.ToString)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Perform Full Load Only")
                    ep = Replace(ep, "{{{value}}}", FullLoad.ToString)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Source Variables")
                    ep = Replace(ep, "{{{value}}}", SourceVariables.EscapeTicks)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Fixed Predicate")
                    ep = Replace(ep, "{{{value}}}", FixedPredicate.EscapeTicks)
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Variable Predicate")
                    ep = Replace(ep, "{{{value}}}", VariablePredicate.EscapeTicks)
                    ep += vbCrLf

                    Return ep
                End Get
            End Property

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <value></value>
            ''' <returns></returns>
            ''' <remarks></remarks>
            Protected Friend ReadOnly Property AttributeMetadataDefintion As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim ep As String = ""

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Domain")
                        ep = Replace(ep, "{{{value}}}", Domain)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Description")
                        ep = Replace(ep, "{{{value}}}", bia.Description.EscapeTicks)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Sort Order")
                        ep = Replace(ep, "{{{value}}}", bia.SortOrder.ToString)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Ordinal")
                        ep = Replace(ep, "{{{value}}}", bia.Ordinal.ToString)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Optionality")
                        ep = Replace(ep, "{{{value}}}", bia.Optionality.ToString)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Datatype")
                        ep = Replace(ep, "{{{value}}}", bia.Datatype)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Business Identifier")
                        ep = Replace(ep, "{{{value}}}", bia.BusinessIdentifier.ToString)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Optimize with Index")
                        ep = Replace(ep, "{{{value}}}", bia.Index.ToString)
                        ep += vbCrLf

                    Next

                    Return ep
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property Attributes(ByVal AttributeNumber As Integer) As EntityAttribute
                Get
                    Return _attribute(AttributeNumber)
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property BusinessIdentifierChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.Yes Then
                            rstr += "   [" & bia.Name & "] " & bia.Datatype & " " & If(bia.Optionality = EDW.Common.YesNoType.No, "not null,", "null,") & vbCrLf
                        End If
                    Next

                    Return rstr
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property AttributeChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.No Then
                            rstr += "   [" & bia.Name & "] " & bia.Datatype & " " & If(bia.Optionality = EDW.Common.YesNoType.No, "not null,", "null,") & vbCrLf
                        End If
                    Next

                    Return rstr
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property AlternateKeyChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = "("

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.Yes Then
                            rstr += "[" & bia.Name & "] " & If(bia.SortOrder = EDW.Common.SortOrderType.asc, "asc,", "desc,")
                        End If
                    Next

                    Return rstr & "[psa_entity_sequence] desc)"
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property AlternateKeyChunkForQueue As String
                Get
                    Dim rstr As String = AlternateKeyChunk
                    rstr = Replace(rstr, ",[psa_entity_sequence] desc)", ")")
                    Return rstr
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property HashChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.No Then
                            rstr += "[" & bia.Name & "],"
                        End If
                    Next

                    Return rstr & "N'Slalom.Framework' [HA]"
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property UpdateChunk(Optional ByVal ColumnPadding As UShort = 6) As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim cp As New String(" "c, ColumnPadding)

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.No Then
                            rstr += cp & "[" & bia.Name & "]=s.[" & bia.Name & "]," & vbCrLf
                        End If
                    Next

                    If Len(rstr) > 0 Then
                        rstr = Left(rstr, Len(rstr) - 2)
                    Else
                        rstr = cp & "-- there are no attributes in this psa entity, i.e. this code will never be reached..."
                    End If

                    Return rstr
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property ControlJoinChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim spacer As New String(" "c, Len(Domain) + 12)

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.Yes Then
                            If bia.Optionality = EDW.Common.YesNoType.No Then
                                rstr += spacer & "s.[" & bia.Name & "]=t.[" & bia.Name & "] and" & vbCrLf
                            Else
                                rstr += spacer & "("
                                rstr += "(s.[" & bia.Name & "]=t.[" & bia.Name & "]) or "
                                rstr += "(s.[" & bia.Name & "] is not null and t.[" & bia.Name & "] is not null) or "
                                rstr += "(s.[" & bia.Name & "] is null and t.[" & bia.Name & "] is null)"
                                rstr += ") and" & vbCrLf
                            End If
                        End If
                    Next

                    rstr += spacer & "t.[psa_current_flag]=1;"

                    Return rstr
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property MergeJoinChunk(ByVal Padding As UShort) As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim spacer As New String(" "c, Padding)

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.Yes Then
                            If bia.Optionality = EDW.Common.YesNoType.No Then
                                rstr += spacer & "and s.[" & bia.Name & "]=t.[" & bia.Name & "]" & vbCrLf
                            Else
                                rstr += spacer & "and ("
                                rstr += "(s.[" & bia.Name & "]=t.[" & bia.Name & "]) or "
                                rstr += "(s.[" & bia.Name & "] is not null and t.[" & bia.Name & "] is not null) or "
                                rstr += "(s.[" & bia.Name & "] is null and t.[" & bia.Name & "] is null)"
                                rstr += ")" & vbCrLf
                            End If
                        End If
                    Next

                    Return Left(rstr, Len(rstr) - 2)
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property UpdateKeyChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim spacer As New String(" "c, Len(Domain) + 12)

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.Yes Then
                            rstr += "   if update ([" & bia.Name & "]) begin;" & vbCrLf
                            rstr += "      raiserror(N'The Source Native Key (SNK), or Business Identifier, [" & bia.Name & "] cannot be updated. Insert a new record the delete the old record.',16,1);" & vbCrLf
                            rstr += "   end;" & vbCrLf & vbCrLf
                        End If
                    Next

                    Return Left(rstr, Len(rstr) - 2)
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property ColumnSetChunk(ByVal ColumnSetAlias As String, ByVal ColumnPadding As UShort) As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim pad As New String(" "c, ColumnPadding)
                    ColumnSetAlias = If(ColumnSetAlias = "", "", ColumnSetAlias & ".")

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        rstr += pad & ColumnSetAlias & "[" & bia.Name & "]," & vbCrLf
                    Next

                    Return Left(rstr, Len(rstr) - 2)
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property BusinessIdentifierColumnSetChunk(ByVal ColumnSetAlias As String, ByVal ColumnPadding As UShort) As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim pad As New String(" "c, ColumnPadding)
                    ColumnSetAlias = If(ColumnSetAlias = "", "", ColumnSetAlias & ".")

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = EDW.Common.YesNoType.Yes Then
                            rstr += pad & ColumnSetAlias & "[" & bia.Name & "]," & vbCrLf
                        End If

                    Next

                    Return Left(rstr, Len(rstr) - 2)
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property NewColumnSetChunk(Optional ByVal ColumnSetAlias As String = "", Optional ByVal ColumnPadding As UShort = 0, _
                                                        Optional ByVal ColumnPrefix As String = "", Optional ByVal ColumnSuffix As String = "", _
                                                        Optional ByVal AttributeTypesToInclude As EDW.Common.AttributeType = EDW.Common.AttributeType.Both) As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim pad As New String(" "c, ColumnPadding)
                    ColumnSetAlias = If(ColumnSetAlias = "", "", ColumnSetAlias & ".")

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)

                        If (bia.BusinessIdentifier = EDW.Common.YesNoType.Yes And AttributeTypesToInclude = EDW.Common.AttributeType.BusinessIdentifier) Or _
                           (bia.BusinessIdentifier = EDW.Common.YesNoType.No And AttributeTypesToInclude = EDW.Common.AttributeType.Atomic) Or _
                           AttributeTypesToInclude = EDW.Common.AttributeType.Both Then
                            rstr += pad & ColumnPrefix & ColumnSetAlias & "[" & bia.Name & "]" & ColumnSuffix & " [" & bia.Name & "]," & vbCrLf
                        End If

                    Next

                    Return Left(rstr, Len(rstr) - 2)
                End Get
            End Property

#End Region

#Region "Entity Constructors"

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <param name="NewSchema"></param>
            ''' <param name="NewEntity"></param>
            ''' <param name="NewDescription"></param>
            ''' <param name="NewSourceSchema"></param>
            ''' <param name="NewSourceEntity"></param>
            ''' <param name="NewHashLargeObjects"></param>
            ''' <param name="NewInferDeletions"></param>
            ''' <param name="NewBuildGroup"></param>
            ''' <param name="NewSequenceOrder"></param>
            ''' <param name="NewLogicalSignature"></param>
            ''' <param name="NewConstructSignature"></param>
            ''' <param name="NewMaxThreads"></param>
            ''' <param name="NewMaxRecordCount"></param>
            ''' <remarks></remarks>
            Protected Friend Sub New(ByVal NewSchema As String,
                                     ByVal NewEntity As String,
                                     ByVal NewDescription As String,
                                     ByVal NewSourceSchema As String,
                                     ByVal NewSourceEntity As String,
                                     ByVal NewHashLargeObjects As String,
                                     ByVal NewInferDeletions As String,
                                     ByVal NewBuildGroup As String,
                                     ByVal NewSequenceOrder As Short,
                                     ByVal NewLogicalSignature As String,
                                     ByVal NewConstructSignature As String,
                                     ByVal NewMaxThreads As Integer,
                                     ByVal NewMaxRecordCount As Integer,
                                     ByVal NewFullLoad As String,
                                     ByVal NewSourceVariables As String,
                                     ByVal NewFixedPredicate As String,
                                     ByVal NewVariablePredicate As String)

                Try

                    Schema = NewSchema
                    Entity = NewEntity
                    Description = NewDescription
                    SourceSchema = NewSourceSchema
                    SourceEntity = NewSourceEntity
                    HashLargeObjects = NewHashLargeObjects.StringToYesNoType
                    MaxThreads = NewMaxThreads
                    BuildGroup = NewBuildGroup
                    MaxRecordCount = NewMaxRecordCount
                    SequenceOrder = NewSequenceOrder
                    InferDeletions = NewInferDeletions.StringToYesNoType
                    FullLoad = NewFullLoad.StringToYesNoType
                    SourceVariables = NewSourceVariables
                    FixedPredicate = NewFixedPredicate
                    VariablePredicate = NewVariablePredicate
                    LogicalSignature = NewLogicalSignature
                    ConstructSignature = NewConstructSignature

                Catch ex As Exception
                    PrintClientError(New StackFrame().GetMethod().Name, ex)

                End Try

            End Sub

            ''' <summary>
            ''' 
            ''' </summary>
            ''' <param name="AttributeName"></param>
            ''' <param name="AttributeOrdinal"></param>
            ''' <param name="AttributeDatatype"></param>
            ''' <param name="AttributeOptionality"></param>
            ''' <param name="AttributeBusinessIdentifier"></param>
            ''' <param name="AttributeIndex"></param>
            ''' <param name="AttributeSortOrder"></param>
            ''' <param name="AttributeDescription"></param>
            ''' <remarks></remarks>
            Protected Friend Sub AddEntityAttribute(ByVal AttributeName As String,
                                                    ByVal AttributeOrdinal As Integer,
                                                    ByVal AttributeDatatype As String,
                                                    ByVal AttributeOptionality As String,
                                                    ByVal AttributeBusinessIdentifier As String,
                                                    ByVal AttributeIndex As String,
                                                    ByVal AttributeSortOrder As String,
                                                    ByVal AttributeDescription As String)

                Try

                    Dim ao As EDW.Common.YesNoType = If(AttributeOptionality = "No", EDW.Common.YesNoType.No, EDW.Common.YesNoType.Yes)
                    Dim bi As EDW.Common.YesNoType = If(AttributeBusinessIdentifier = "No", EDW.Common.YesNoType.No, EDW.Common.YesNoType.Yes)
                    Dim so As EDW.Common.SortOrderType = If(AttributeSortOrder = "desc", EDW.Common.SortOrderType.desc, EDW.Common.SortOrderType.asc)

                    Dim ai As EDW.Common.YesNoType = AttributeIndex.StringToYesNoType

                    _attribute.AddMember(New EntityAttribute(AttributeName, AttributeOrdinal, AttributeDatatype, ao, bi, ai, so, AttributeDescription))

                    'If IsNothing(_attribute) Then
                    '    ReDim _attribute(0)
                    '    _attribute(0) = New EntityAttribute(AttributeName, AttributeOrdinal, AttributeDatatype, ao, bi, ai, so, AttributeDescription, AttributeSourcePredicate)
                    'Else
                    '    ReDim Preserve _attribute(_attribute.Length)
                    '    _attribute(_attribute.Length - 1) = New EntityAttribute(AttributeName, AttributeOrdinal, AttributeDatatype, ao, bi, ai, so, AttributeDescription, AttributeSourcePredicate)
                    'End If

                Catch ex As Exception
                    PrintClientError(New StackFrame().GetMethod().Name, ex)
                End Try

            End Sub

#End Region

            ''' <summary></summary>
            Class EntityAttribute

#Region "Entity Attribute Variables"

                Private _name As String
                Private _ordinal As Integer
                Private _datatype As String
                Private _sortorder As EDW.Common.SortOrderType
                Private _optionality As EDW.Common.YesNoType
                Private _businessidentifier As EDW.Common.YesNoType
                Private _index As EDW.Common.YesNoType
                Private _description As String

#End Region

#Region "Entity Attribute Properties"

                ''' <summary></summary>
                Protected Friend Property Name As String
                    Get
                        Return _name
                    End Get
                    Set(value As String)
                        _name = value
                    End Set
                End Property

                ''' <summary></summary>
                Protected Friend Property Ordinal As Integer
                    Get
                        Return _ordinal
                    End Get
                    Set(value As Integer)
                        _ordinal = value
                    End Set
                End Property

                ''' <summary></summary>
                Protected Friend Property Datatype As String
                    Get
                        Select Case _datatype
                            Case "ntext"
                                Return "nvarchar(max)"
                            Case "text"
                                Return "varchar(max)"
                            Case "image"
                                Return "nvarchar(max)"
                            Case "geography"
                                Return "varbinary(max)"
                            Case "geometry"
                                Return "varbinary(max)"
                            Case "timestamp"
                                Return "binary(8)"
                            Case Else
                                Return _datatype
                        End Select
                    End Get
                    Set(value As String)
                        _datatype = value
                    End Set
                End Property

                ''' <summary></summary>
                Protected Friend Property SortOrder As EDW.Common.SortOrderType
                    Get
                        Return _sortorder
                    End Get
                    Set(value As EDW.Common.SortOrderType)
                        _sortorder = value
                    End Set
                End Property

                ''' <summary></summary>
                Protected Friend Property Optionality As EDW.Common.YesNoType
                    Get
                        Return _optionality
                    End Get
                    Set(value As EDW.Common.YesNoType)
                        _optionality = value
                    End Set
                End Property

                ''' <summary></summary>
                Protected Friend Property BusinessIdentifier As EDW.Common.YesNoType
                    Get
                        Return _businessidentifier
                    End Get
                    Set(value As EDW.Common.YesNoType)
                        _businessidentifier = value
                    End Set
                End Property

                ''' <summary></summary>
                Protected Friend Property Index As EDW.Common.YesNoType
                    Get
                        Return _index
                    End Get
                    Set(value As EDW.Common.YesNoType)
                        _index = value
                    End Set
                End Property

                ''' <summary></summary>
                Protected Friend Property Description As String
                    Get
                        Return _description
                    End Get
                    Set(ByVal value As String)
                        _description = value
                    End Set
                End Property

#End Region

#Region "Entity Attribute Contructors"

                ''' <summary>
                ''' 
                ''' </summary>
                ''' <param name="AttributeName"></param>
                ''' <param name="AttributeOrdinal"></param>
                ''' <param name="AttributeDatatype"></param>
                ''' <param name="AttributeOptionality"></param>
                ''' <param name="AttributeBusinessIdentifier"></param>
                ''' <param name="AttributeIndex"></param>
                ''' <param name="AttributeSortOrder"></param>
                ''' <param name="AttributeDescription"></param>
                ''' <remarks></remarks>
                Protected Friend Sub New(ByVal AttributeName As String,
                                         ByVal AttributeOrdinal As Integer,
                                         ByVal AttributeDatatype As String,
                                         ByVal AttributeOptionality As EDW.Common.YesNoType,
                                         ByVal AttributeBusinessIdentifier As EDW.Common.YesNoType,
                                         ByVal AttributeIndex As EDW.Common.YesNoType,
                                         ByVal AttributeSortOrder As EDW.Common.SortOrderType,
                                         ByVal AttributeDescription As String)

                    Name = AttributeName
                    Ordinal = AttributeOrdinal
                    Datatype = AttributeDatatype
                    Optionality = AttributeOptionality
                    BusinessIdentifier = AttributeBusinessIdentifier
                    Index = AttributeIndex
                    SortOrder = AttributeSortOrder
                    Description = AttributeDescription

                End Sub

#End Region

            End Class ' EntityAttribute

        End Class ' Entity

    End Class ' Construct

End Class ' PSA