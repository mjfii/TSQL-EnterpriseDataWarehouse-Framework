Imports System
Imports System.Data
Imports System.Data.Sql
Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports System.Runtime.InteropServices
Imports EDW.Common.SqlClientOutbound
Imports EDW.Common.InstanceSettings
Imports EDW.AnalyticReportingArea.FrameworkInstallation
Imports System.Linq

Public Class ARA

#Region "CLR Exposed Methods"

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub BuildEntities(ByVal DatabaseName As String)

        ProcessCall(DatabaseName, False, False)

    End Sub

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub VerifyEntities(ByVal DatabaseName As String, _
                                     ByVal DatabaseSchema As String, _
                                     ByVal DatabaseEntity As String)

        ProcessCall(DatabaseName, True, False)

    End Sub

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub DropEntities(ByVal DatabaseName As String, _
                                    ByVal DatabaseSchema As String, _
                                    ByVal DatabaseEntity As String)

        ProcessCall(DatabaseName, False, True)

    End Sub

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub GetModel()

        Dim model_query As String = My.Resources.PSA_GetModel

        ReturnClientResults(model_query)

    End Sub

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub SetModel(ByVal model As SqlXml)

        If IsNothing(model) Then
            PrintClientMessage("You cannot process a null model.")
            Exit Sub
        End If

        Dim model_query As String = Replace(My.Resources.PSA_SetModel, "{{{xml}}}", model.Value.ToString)

        Dim cnn As New SqlConnection("context connection=true")
        cnn.Open()

        ExecuteDDLCommand(model_query, cnn)

        cnn.Close()

    End Sub

#End Region

    Private Shared Sub ProcessCall(ByVal DatabaseName As String, _
                                   ByVal vo As Boolean, _
                                   ByVal del As Boolean)

        Dim cnn As New SqlConnection("context connection=true")

        cnn.Open()

        PrintHeader()

        ' if the user is part of the sysadmin role, move along
        If UserIsSysAdmin(cnn) Then

            ' make sure server objects are in place
            AddInstanceObjects(cnn)

            ' validate incoming database
            Dim cmd As New SqlCommand("select isnull(db_id(N'" & DatabaseName & " '),-1) N'?';", cnn)
            Dim dbe As Integer = CInt(cmd.ExecuteScalar.ToString)

            If dbe > 0 Then

                ' make sure database objects are in place
                AddDatabaseObjects(cnn, DatabaseName)

                ' get the metadata from system tables
                Dim md As DataSet = GetMetadata(DatabaseName, cnn)

                ' if we have a promising set, i.e. the selects worked, we can move forward
                If Not md Is Nothing Then

                    ' load up a variable with the usable construct
                    Dim cons As New Construct(md)

                    ' with we have records, we can begin with the DDL process, otherwise, alert client and exit
                    If cons.EntityCount > 0 Then
                        ProcessConstruct(cons, cnn, vo)
                    Else
                        PrintClientMessage(vbCrLf)
                        PrintClientMessage("There is not defined metadata for the ARA in [dbo].[ara_entity_definition] and/or [dbo].[ara_attribute_definition] in the [master] database.")
                        PrintClientMessage("You may manage data directly or via the [soon to be authored] MS Excel Add-In.")
                    End If ' metadata content exists

                Else
                    ' alert the tables arent there and make them
                    ExecuteDDLCommand(My.Resources.SYS_ARAMetadataTableDefinition, cnn)
                    PrintClientMessage(vbCrLf)
                    PrintClientMessage("The ARA Framework was not ready for use. The required system tables have been built; you can now use the [dbo].[ara_entity_definition] and")
                    PrintClientMessage("[dbo].[ara_attribute_definition] tables in the [master] database to add the metadata construct elements to build each of the ARA objects.")

                End If ' metadata tables exist

            Else
                PrintClientMessage(vbCrLf)
                PrintClientMessage("A database with the name '" & DatabaseName & "' does not exist.  Create that database or use an alternate one.")

            End If ' ara database exists

        End If ' user is admin

        ' ensure connection object is closed
        cnn.Close()

    End Sub

    Private Shared Sub ProcessConstruct(ContructToProcess As Construct, SqlCnn As SqlConnection, VerifyOnly As Boolean)

        Const space As String = " "

        Dim Entity As Construct.Entity = Nothing
        Dim Entities As Construct.Entity() = Nothing
        Dim EntityAttributes As Construct.Entity.EntityAttribute() = Nothing
        Dim EntityNumber As Integer
        Dim ModelInvalid As Boolean

        Dim StartTime As Date = Now()
        Dim fmt As String = "yyyy-MM-dd HH:mm:ss.ff..."
        Dim cmd As New SqlCommand

        ' print header information
        PrintClientMessage(vbCrLf)
        PrintClientMessage("• Process construct started at " & StartTime.ToString(fmt))
        PrintClientMessage("• Database Name: " & ContructToProcess.DatabaseName)
        PrintClientMessage("• Database Compatibility: " & ContructToProcess.DatabaseCompatibility.ToString)

        Dim EntityCount As Integer = ContructToProcess.EntityCount - 1

        ' notify of invalidations to the model
        PrintClientMessage("• Determining model validation: ")

        For EntityNumber = 0 To EntityCount
            Entity = ContructToProcess.Entities(EntityNumber)
            If Not Entity.HasBusinessKey And Not Entity.DeleteEntity Then
                PrintClientMessage(New String(space, 3) & Entity.Domain.ToString & " -> Missing Business Identifiers")
                ModelInvalid = True
            End If
        Next EntityNumber

        If ModelInvalid Then
            PrintClientMessage("  -> Data model is INVALID ")
        Else
            PrintClientMessage("  -> Data model is VALID ")
        End If

        ' notify of warnings to the model
        PrintClientMessage("• Identifying non-fatal issues: ")
        PrintClientMessage(New String(space, 2) & "-> No issues / warnings found")

        ' notify esitmated changes in model
        PrintClientMessage("• Estimated changes to data model: ")

        For EntityNumber = 0 To EntityCount

            PrintClientMessage(space)

            Entity = ContructToProcess.Entities(EntityNumber)
            EntityAttributes = Entity.Attributes

            If Entity.CreateEntity Then
                PrintClientMessage(New String(space, 3) & Entity.Domain.ToString & " -> Create Entity")

            ElseIf Entity.DeleteEntity Then
                PrintClientMessage(New String(space, 3) & Entity.Domain.ToString & " -> Delete Entity")

            ElseIf EntityAttributes IsNot Nothing Then

                PrintClientMessage(New String(space, 3) & Entity.Domain.ToString & " -> Update Entity")

                If Entity.HasAlterAlternateKey Then PrintClientMessage(New String(space, 6) & Entity.Entity.ToString & " -> Business Identifier Change")

                For Each att In EntityAttributes

                    If att.AddColumn = True Then PrintClientMessage(New String(space, 6) & att.ColumnName.ToString & " -> Add Column")
                    If att.DeleteColumn = True Then PrintClientMessage(New String(space, 6) & att.ColumnName.ToString & " -> Delete Column")
                    If att.AlterColumn = True Then PrintClientMessage(New String(space, 6) & att.ColumnName.ToString & " -> Alter Column")
                    If att.AlterDefault = True Then PrintClientMessage(New String(space, 6) & att.ColumnName.ToString & " -> Alter Default")
                    If att.AlterForeignKey = True Then PrintClientMessage(New String(space, 6) & att.ColumnName.ToString & " -> Alter Foreign Key")

                Next att

            End If

        Next EntityNumber

        ' if validated and not verify

        Try
            ExecuteDDLCommand("begin transaction;", SqlCnn)

            ' create new key stores
            Entities = ContructToProcess.NewEntities
            For Each Entity In Entities
                ExecuteDDLCommand(Entity.KeystoreDefintion, SqlCnn)
            Next Entity

            '


            ExecuteDDLCommand("commit transaction", SqlCnn)

        Catch ex As Exception
            PrintClientMessage(ex.Message, 3)
            PrintClientMessage(vbCrLf)

            ExecuteDDLCommand("rollback transaction", SqlCnn)
        End Try


        Dim ed As Date = Now()
        PrintClientMessage(vbCrLf & "• Process ended at " & ed.ToString(fmt))

        Dim min As Integer = DateDiff(DateInterval.Minute, StartTime, ed)
        Dim sec As Integer = DateDiff(DateInterval.Second, StartTime, ed) Mod 60
        PrintClientMessage("• Time to execute " & min.ToString & " min(s) " & sec.ToString & " sec(s)")

    End Sub

    Private Shared Sub DropEntity(e As Construct.Entity, ByVal SqlCnn As SqlConnection)

        'ExecuteDDLCommand(e.RenameDefintion, SqlCnn)

    End Sub

    Private Shared Sub BuildEntity(e As Construct.Entity, ByVal SqlCnn As SqlConnection)

        'ExecuteDDLCommand(e.TableDefinition, SqlCnn)

        Dim cmd As New SqlCommand("select object_id(N'" & e.Domain & "') [oid]", SqlCnn)
        Dim oid As Integer = CInt(cmd.ExecuteScalar)
        e.ObjectID = oid

        'ExecuteDDLCommand(e.ChangeTrackingDefinition, SqlCnn)
        'ExecuteDDLCommand(e.AlternateKeyStatsDefinition, SqlCnn)
        'ExecuteDDLCommand(e.DropTemporalGovernorDefinition, SqlCnn)
        'ExecuteDDLCommand(e.TemporalGovernorDefinition, SqlCnn)

    End Sub

    Private Shared Sub DropAbstracts(e As Construct.Entity, ByVal SqlCnn As SqlConnection)
        'ExecuteDDLCommand(e.DropRelatedObjectsDefintion, SqlCnn)
    End Sub

    Private Shared Sub BuildAbstracts(e As Construct.Entity, ByVal SqlCnn As SqlConnection)

        '' control abstract
        'ExecuteDDLCommand(e.ControlViewDefinition, SqlCnn)
        'ExecuteDDLCommand(e.ControlInsertDefinition, SqlCnn)
        'ExecuteDDLCommand(e.ControlUpdateDefinition, SqlCnn)
        'ExecuteDDLCommand(e.ControlDeleteDefinition, SqlCnn)
        'ExecuteDDLCommand(e.ControlSecurityDefinition, SqlCnn)

        '' as-is abstract
        'ExecuteDDLCommand(e.AsIsViewDefinition, SqlCnn)
        'ExecuteDDLCommand(e.AsIsTriggerDefinition, SqlCnn)
        'ExecuteDDLCommand(e.AsIsSecurityDefinition, SqlCnn)

        '' as-was abstract
        'ExecuteDDLCommand(e.AsWasViewDefinition, SqlCnn)
        'ExecuteDDLCommand(e.AsWasTriggerDefinition, SqlCnn)
        'ExecuteDDLCommand(e.AsWasSecurityDefinition, SqlCnn)

        '' batch-count abstract
        'ExecuteDDLCommand(e.BatchCountViewDefinition, SqlCnn)
        'ExecuteDDLCommand(e.BatchCountTriggerDefinition, SqlCnn)
        'ExecuteDDLCommand(e.BatchCountSecurityDefinition, SqlCnn)

        '' as-of abstract
        'ExecuteDDLCommand(e.AsOfFunctionDefinition, SqlCnn)
        'ExecuteDDLCommand(e.AsOfSecurityDefinition, SqlCnn)

        '' changes abstract
        'ExecuteDDLCommand(e.ChangesFunctionDefinition, SqlCnn)
        'ExecuteDDLCommand(e.ChangesSecurityDefinition, SqlCnn)

        '' queues
        'ExecuteDDLCommand(e.LoadStageDefinition, SqlCnn)

        '' methods
        'ExecuteDDLCommand(e.ProcessUpsertDefintion, SqlCnn)
        'ExecuteDDLCommand(e.ProcessUpsertSecurityDefinition, SqlCnn)
        'ExecuteDDLCommand(e.WorkerUpsertDefintion, SqlCnn)

        'ExecuteDDLCommand(e.ProcessDeleteDefintion, SqlCnn)
        'ExecuteDDLCommand(e.ProcessDeleteSecurityDefinition, SqlCnn)
        'ExecuteDDLCommand(e.WorkerDeleteDefintion, SqlCnn)



        '' service broker
        'ExecuteDDLCommand(e.ServiceBrokerDefinition, SqlCnn)

    End Sub

    Private Shared Sub SyncMetadata(e As Construct.Entity, ByVal SqlCnn As SqlConnection)

        ExecuteDDLCommand(e.EntityMetadataDefinition, SqlCnn)
        ExecuteDDLCommand(e.AttributeMetadataDefintion, SqlCnn)

    End Sub

    Private Shared Sub PrintHeader()

        PrintClientMessage(My.Resources.SYS_SlalomTextArt1 & vbCrLf)
        PrintClientMessage("EDW Framework - Analytics & Reporting Area ('ARA') Definition")
        PrintClientMessage("Slalom Consulting | Copyright © 2015 | www.slalom.com" & vbCrLf & vbCrLf)

    End Sub

    Class Construct

#Region "Construct Variables"
        Private _construct As New DataSet
        Private _entities As Entity()
        Private _databasecompatibility As SQLServerCompatibility
        Private _databasename As String
        Private _new_entities As Entity()
#End Region

#Region "Construct Properties"

        WriteOnly Property Construct As DataSet
            ' build the object class
            Set(value As DataSet)

                _construct = value

                Dim dpt As DataTable = _construct.Tables("ara_database_properties")
                Dim edt As DataTable = _construct.Tables("ara_entity_definition")
                Dim adt As DataTable = _construct.Tables("ara_attribute_definition")
                Dim e As Entity
                Dim i As Integer = 0

                _databasename = CStr(dpt.Rows(0).Item("database_name").ToString)
                _databasecompatibility = CUShort(dpt.Rows(0).Item("compatibility_level").ToString)

                ' determine entites to add or update or do nothing
                For Each edr In edt.Rows

                    ReDim Preserve _entities(i)

                    e = New Entity(edr("ara_entity"), _
                                   edr("ara_entity_description"), _
                                   edr("ara_entity_type"),
                                   edr("ara_hash_large_objects"), _
                                   edr("ara_new_entity_type"), _
                                   CBool(edr("ara_entity_create")), _
                                   CBool(edr("ara_entity_delete")))

                    For Each adr In adt.Select("ara_entity = '" & edr("ara_entity") & "'", "ara_attribute_ordinal")

                        e.AddEntityAttribute(adr("ara_attribute_name"), _
                                             If(IsDBNull(adr("ara_attribute_referenced_entity")), "", adr("ara_attribute_referenced_entity")), _
                                             adr("ara_attribute_datatype"), _
                                             If(IsDBNull(adr("ara_attribute_default")), "", adr("ara_attribute_default")), _
                                             adr("ara_attribute_ordinal"), _
                                             adr("ara_attribute_sort"), _
                                             adr("ara_attribute_optional"), _
                                             adr("ara_attribute_business_identifier"), _
                                             If(IsDBNull(adr("ara_attribute_description")), "", adr("ara_attribute_description")), _
                                             adr("ara_attribute_distribution"),
                                             adr("ara_attribute_add_column"), _
                                             adr("ara_attribute_delete_column"), _
                                             adr("ara_attribute_alter_column"), _
                                             adr("ara_attribute_alter_foreign_key"), _
                                             adr("ara_attribute_alter_default"), _
                                             adr("ara_attribute_alter_alternate_key"))

                    Next adr

                    _entities(i) = e
                    i += 1 ' increment
                Next edr

            End Set
        End Property

        ReadOnly Property EntityCount As Integer
            Get
                If IsNothing(_entities) Then
                    Return 0
                Else
                    Return _entities.Length
                End If
            End Get
        End Property

        ReadOnly Property Entities(EntityNumber As Integer) As Entity
            Get
                Return _entities(EntityNumber)
            End Get
        End Property

        ReadOnly Property Entities(EntityName As String) As Entity
            Get
                Dim i As Integer
                'TODO: fix with linq
                For i = 0 To (EntityCount - 1)
                    If _entities(i).Entity = EntityName Then
                        Return _entities(i)
                    End If
                Next i

                Return Nothing
            End Get
        End Property

        Public ReadOnly Property DatabaseName As String
            Get
                Return _databasename
            End Get
        End Property

        Public ReadOnly Property DatabaseCompatibility As SQLServerCompatibility
            Get
                Return _databasecompatibility
            End Get
        End Property

        Public ReadOnly Property NewEntities As Entity()
            Get
                Return (From ne As Entity In _entities Where ne.CreateEntity = True Select ne).ToArray
            End Get
        End Property

#End Region

#Region "Construct Types"
        Enum SQLServerCompatibility As UShort
            SQLServer2008 = 100
            SQLServer2012 = 110
            SQLServer2014 = 120
            SQLServer2016 = 130
        End Enum

        Enum YesNoType As UShort
            Yes = 1
            No = 2
        End Enum

        Enum EntityType As UShort
            Dimension = 1
            Fact = 2
        End Enum

        Enum BuildAction As UShort
            AddEntity = 1
            UpdateEntity = 2
            DeleteEntity = 3
            None = 4
        End Enum

        Enum AttributeType As UShort
            BusinessIdentifier = 2
            Variable = 4
            Both = 16
        End Enum
#End Region

#Region "Construct Constructors"

        Sub New(ByVal ds As DataSet)
            Construct = ds
        End Sub

#End Region

        Class Entity

#Region "Entity Variables"
            Private _entity As String
            Private _description As String
            Private _entitytype As EntityType
            Private _existingentitytype As EntityType
            Private _hashlargeobjects As String
            Private _objectid As Integer
            Private _createentity As Boolean
            Private _deleteentity As Boolean
            Private _attribute As EntityAttribute()

            Private _has_business_key As Boolean

            Private _has_add_column As Boolean
            Private _has_delete_column As Boolean
            Private _has_alter_column As Boolean
            Private _has_alter_foreign_key As Boolean
            Private _has_alter_default As Boolean
            Private _has_alter_alternate_key As Boolean

            Public Const Spacer As String = " "
            Public Const EmptyString As String = ""
#End Region

#Region "Entity Properties"

            Property Entity As String
                Get
                    Return _entity
                End Get
                Set(value As String)
                    _entity = value
                End Set
            End Property

            Property Description As String
                Get
                    Return _description
                End Get
                Set(value As String)
                    _description = value
                End Set
            End Property

            Property EntityType As EntityType
                Get
                    Return _entitytype
                End Get
                Set(value As EntityType)
                    _entitytype = value
                End Set
            End Property

            Property ExistingEntityType As EntityType
                Get
                    Return _existingentitytype
                End Get
                Set(value As EntityType)
                    _existingentitytype = value
                End Set
            End Property

            Property HashLargeObjects As YesNoType
                Get
                    Return _hashlargeobjects
                End Get
                Set(value As YesNoType)
                    _hashlargeobjects = value
                End Set
            End Property

            Property CreateEntity As Boolean
                Get
                    Return _createentity
                End Get
                Set(value As Boolean)
                    _createentity = value
                End Set
            End Property

            Property DeleteEntity As Boolean
                Get
                    Return _deleteentity
                End Get
                Set(value As Boolean)
                    _deleteentity = value
                End Set
            End Property

            Property ObjectID As Integer
                Get
                    Return If(IsNothing(_objectid), 0, _objectid)
                End Get
                Set(value As Integer)
                    _objectid = value
                End Set
            End Property

            ReadOnly Property AttributeCount As Integer
                Get
                    If _attribute Is Nothing Then
                        Return 0
                    Else
                        Return _attribute.Count
                    End If

                End Get
            End Property

            ReadOnly Property Domain As String
                Get
                    Return "[dbo].[" & Entity & "]"
                End Get
            End Property

            ReadOnly Property Label As String
                Get
                    Return "dbo." & Entity
                End Get
            End Property

            ReadOnly Property KeystoreDefintion As String
                Get
                    Dim ReturnString As String = EmptyString

                    If EntityType = ARA.Construct.EntityType.Fact Then
                        ReturnString = "-- Keystore dimension does not exist for measure groups"
                    Else
                        ReturnString = My.Resources.ARA_KeyStoreDefinition
                        ReturnString = Replace(ReturnString, "{{{creation log}}}", My.Resources.ARA_ObjectCreationLogMessage)
                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{table column set}}}", TableColumnDefinition(BusinessIdentifiers, 3, False))
                        ReturnString = Replace(ReturnString, "{{{index column set}}}", IndexColumnDefinition(BusinessIdentifiers, 3, True))
                    End If

                    Return ReturnString
                End Get
            End Property


            Private ReadOnly Property TableColumnDefinition(ByVal ColumnSet As EntityAttribute(), _
                                                            Optional ByVal Padding As Integer = 0, _
                                                            Optional ByVal RemoveTrailingComma As Boolean = True) As String
                Get
                    Dim ReturnString As String = EmptyString

                    For Each Column In ColumnSet
                        ReturnString += New String(Spacer, Padding) & Column.ColumnName & Spacer & Column.Datatype & Spacer & _
                                        If(Column.Optionality = YesNoType.Yes, "null,", "not null,") & vbCrLf
                    Next Column

                    If RemoveTrailingComma Then
                        ReturnString = Left(ReturnString, Len(ReturnString) - 3)
                    Else
                        ReturnString = Left(ReturnString, Len(ReturnString) - 2)
                    End If

                    Return ReturnString
                End Get
            End Property

            Private ReadOnly Property IndexColumnDefinition(ByVal ColumnSet As EntityAttribute(), _
                                                            Optional ByVal Padding As Integer = 0, _
                                                            Optional ByVal RemoveTrailingComma As Boolean = True) As String
                Get
                    Dim ReturnString As String = EmptyString
                    For Each Column In ColumnSet
                        ReturnString += New String(Spacer, Padding) & Column.ColumnName & Spacer & _
                                        Column.SortOrder.ToString & "," & vbCrLf
                    Next

                    If RemoveTrailingComma Then
                        ReturnString = Left(ReturnString, Len(ReturnString) - 3)
                    Else
                        ReturnString = Left(ReturnString, Len(ReturnString) - 2)
                    End If

                    Return ReturnString
                End Get
            End Property



            ReadOnly Property EntityMetadataDefinition As String
                Get
                    Dim ep As String = ""

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Domain")
                    ep = Replace(ep, "{{{value}}}", Domain)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Description")
                    ep = Replace(ep, "{{{value}}}", Replace(Description, "'", "''"))
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Use Large Object Hashing Algorithm")
                    ep = Replace(ep, "{{{value}}}", HashLargeObjects.ToString)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Assembly")
                    ep = Replace(ep, "{{{value}}}", "Slalom.Framework")
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Copyright")
                    ep = Replace(ep, "{{{value}}}", "Slalom Consulting © 2014")
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Website")
                    ep = Replace(ep, "{{{value}}}", "www.slalom.com")
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Build Timestamp")
                    ep = Replace(ep, "{{{value}}}", Now.ToString())
                    ep += vbCrLf

                    Return ep
                End Get
            End Property

            ReadOnly Property AttributeMetadataDefintion As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim ep As String = ""

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)

                        ep += My.Resources.PSA_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Domain")
                        ep = Replace(ep, "{{{value}}}", Domain)
                        ep += vbCrLf

                        ep += My.Resources.PSA_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Description")
                        ep = Replace(ep, "{{{value}}}", Replace(bia.Description, "'", "''"))
                        ep += vbCrLf

                        ep += My.Resources.PSA_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Sort Order")
                        ep = Replace(ep, "{{{value}}}", bia.SortOrder)
                        ep += vbCrLf

                        ep += My.Resources.PSA_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Ordinal")
                        ep = Replace(ep, "{{{value}}}", bia.Ordinal)
                        ep += vbCrLf

                        ep += My.Resources.PSA_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Optionality")
                        ep = Replace(ep, "{{{value}}}", bia.Optionality)
                        ep += vbCrLf

                        ep += My.Resources.PSA_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Datatype")
                        ep = Replace(ep, "{{{value}}}", bia.Datatype)
                        ep += vbCrLf

                        ep += My.Resources.PSA_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", bia.Name)
                        ep = Replace(ep, "{{{property}}}", "Business Identifier")
                        ep = Replace(ep, "{{{value}}}", bia.BusinessIdentifier)
                        ep += vbCrLf

                        ep += My.Resources.PSA_TablePropertyDefintion
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{property}}}", "Assembly")
                        ep = Replace(ep, "{{{value}}}", "Slalom.Framework")
                        ep += vbCrLf

                        ep += My.Resources.PSA_TablePropertyDefintion
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{property}}}", "Copyright")
                        ep = Replace(ep, "{{{value}}}", "Slalom Consulting © 2014")
                        ep += vbCrLf

                        ep += My.Resources.PSA_TablePropertyDefintion
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{property}}}", "Website")
                        ep = Replace(ep, "{{{value}}}", "www.slalom.com")
                        ep += vbCrLf

                    Next

                    Return ep
                End Get
            End Property

            ReadOnly Property Attribute(ByVal AttributeNumber As Integer) As EntityAttribute
                Get
                    Return _attribute(AttributeNumber)
                End Get
            End Property


            ReadOnly Property BusinessIdentifiers As EntityAttribute()
                Get
                    Dim a As EntityAttribute() = Nothing

                    If IsNothing(_attribute) Then
                        Return Nothing
                    End If

                    If _has_business_key = False Then
                        Return Nothing
                    End If

                    For Each x In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If IsNothing(a) Then
                            ReDim a(0)
                            a(0) = x
                        Else
                            ReDim Preserve a(a.Length)
                            a(a.Length - 1) = x
                        End If
                    Next

                    Return a
                End Get
            End Property

            ReadOnly Property Attributes As EntityAttribute()
                Get
                    Return _attribute
                End Get
            End Property


            Public ReadOnly Property HasBusinessKey As Boolean
                Get
                    Return _has_business_key
                End Get
            End Property

            Public ReadOnly Property HasAddColumn As Boolean
                Get
                    Return _has_add_column
                End Get
            End Property

            Public ReadOnly Property HasAlterAlternateKey As Boolean
                Get
                    Return _has_alter_alternate_key
                End Get
            End Property

#End Region

            Sub New(ByVal NewEntity As String, _
                    ByVal NewDescription As String, _
                    ByVal NewEntityType As String, _
                    ByVal NewHashLargeObjects As String, _
                    ByVal NewExistingEntityType As String, _
                    ByVal NewEntityCreate As Boolean, _
                    ByVal NewEntityDelete As Boolean)

                Entity = NewEntity
                Description = NewDescription
                EntityType = If(NewEntityType = "D", EntityType.Dimension, EntityType.Fact)
                HashLargeObjects = If(NewHashLargeObjects = "Yes", YesNoType.Yes, YesNoType.No)
                ExistingEntityType = If(NewExistingEntityType = "D", EntityType.Dimension, EntityType.Fact)
                CreateEntity = NewEntityCreate
                DeleteEntity = NewEntityDelete

            End Sub

            Sub AddEntityAttribute(ByVal AttributeName As String, _
                                   ByVal AttributeReferencedEntity As String, _
                                   ByVal AttributeDatatype As String, _
                                   ByVal AttributeDefault As String, _
                                   ByVal AttributeOrdinal As Integer, _
                                   ByVal AttributeSortOrder As String, _
                                   ByVal AttributeOptionality As String, _
                                   ByVal AttributeBusinessIdentifier As String, _
                                   ByVal AttributeDescription As String, _
                                   ByVal AttributeDistribution As String, _
                                   ByVal AttributeAddColumn As Boolean, _
                                   ByVal AttributeDeleteColumn As Boolean, _
                                   ByVal AttributeAlterColumn As Boolean, _
                                   ByVal AttributeAlterForeignKey As Boolean, _
                                   ByVal AttributeAlterDefault As Boolean, _
                                   ByVal AttributeAlterAlternateKey As Boolean)

                Dim ao As UShort = If(AttributeOptionality = "No", 2, 1)
                Dim bi As YesNoType = If(AttributeBusinessIdentifier = "Yes", YesNoType.Yes, YesNoType.No)
                Dim so As UShort = If(AttributeSortOrder = "desc", 2, 1)
                Dim sd As UShort = If(AttributeDistribution = "D", 1, 2)

                Dim AttributeNumber As Integer

                If IsNothing(_attribute) Then
                    ReDim _attribute(0)
                    AttributeNumber = 0
                Else
                    ReDim Preserve _attribute(_attribute.Length)
                    AttributeNumber = _attribute.Length - 1
                End If

                _attribute(AttributeNumber) = New EntityAttribute(AttributeName, _
                                                                  AttributeReferencedEntity, _
                                                                  AttributeDatatype, _
                                                                  AttributeDefault,
                                                                  AttributeOrdinal, _
                                                                  so,
                                                                  ao, _
                                                                  bi,
                                                                  AttributeDescription, _
                                                                  sd, _
                                                                  AttributeAddColumn, _
                                                                  AttributeDeleteColumn, _
                                                                  AttributeAlterColumn, _
                                                                  AttributeAlterForeignKey, _
                                                                  AttributeAlterDefault, _
                                                                  AttributeAlterAlternateKey)



                If bi = YesNoType.Yes Then _has_business_key = True

                If AttributeAddColumn = True Then _has_add_column = True
                If AttributeDeleteColumn = True Then _has_delete_column = True
                If AttributeAlterColumn = True Then _has_alter_column = True
                If AttributeAlterForeignKey = True Then _has_alter_foreign_key = True
                If AttributeAlterDefault = True Then _has_alter_default = True
                If AttributeAlterAlternateKey = True Then _has_alter_alternate_key = True

            End Sub

            Class EntityAttribute

                Private _name As String
                Private _referencedentity As String
                Private _datatype As String
                Private _defaultvalue As String
                Private _ordinal As Integer
                Private _sortorder As SortOrderType
                Private _optionality As YesNoType
                Private _businessidentifier As YesNoType
                Private _description As String
                Private _statisticaldistribution As StatiticalDistribution
                Private _add_column As Boolean
                Private _delete_column As Boolean
                Private _alter_column As Boolean
                Private _alter_foreign_key As Boolean
                Private _alter_default As Boolean
                Private _alter_alternate_key As Boolean

                Property Name As String
                    Get
                        Return _name
                    End Get
                    Set(value As String)
                        _name = value
                    End Set
                End Property

                Public ReadOnly Property ColumnName As String
                    Get
                        Return "[" & Replace(Replace(_name, "]", ""), "[", "") & "]"
                    End Get
                End Property

                Property ReferencedEntity As String
                    Get
                        Return _referencedentity
                    End Get
                    Set(value As String)
                        _referencedentity = value
                    End Set
                End Property

                Property Datatype As String
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

                Property DefaultValue As String
                    Get
                        Return _defaultvalue
                    End Get
                    Set(value As String)
                        _defaultvalue = value
                    End Set
                End Property

                Property Ordinal As Integer
                    Get
                        Return _ordinal
                    End Get
                    Set(value As Integer)
                        _ordinal = value
                    End Set
                End Property

                Property SortOrder As SortOrderType
                    Get
                        Return _sortorder
                    End Get
                    Set(value As SortOrderType)
                        _sortorder = value
                    End Set
                End Property

                Property Optionality As YesNoType
                    Get
                        Return _optionality
                    End Get
                    Set(value As YesNoType)
                        _optionality = value
                    End Set
                End Property

                Property BusinessIdentifier As YesNoType
                    Get
                        Return _businessidentifier
                    End Get
                    Set(value As YesNoType)
                        _businessidentifier = value
                    End Set
                End Property

                Property Description As String
                    Get
                        Return _description
                    End Get
                    Set(ByVal value As String)
                        _description = value
                    End Set
                End Property

                Property Distribution As StatiticalDistribution
                    Get
                        Return _statisticaldistribution
                    End Get
                    Set(value As StatiticalDistribution)
                        _statisticaldistribution = value
                    End Set
                End Property

                Property AddColumn As Boolean
                    Get
                        Return _add_column
                    End Get
                    Set(value As Boolean)
                        _add_column = value
                    End Set
                End Property

                Property DeleteColumn As Boolean
                    Get
                        Return _delete_column
                    End Get
                    Set(value As Boolean)
                        _delete_column = value
                    End Set
                End Property

                Property AlterColumn As Boolean
                    Get
                        Return _alter_column
                    End Get
                    Set(value As Boolean)
                        _alter_column = value
                    End Set
                End Property

                Property AlterForeignKey As Boolean
                    Get
                        Return _alter_foreign_key
                    End Get
                    Set(value As Boolean)
                        _alter_foreign_key = value
                    End Set
                End Property

                Property AlterDefault As Boolean
                    Get
                        Return _alter_default
                    End Get
                    Set(value As Boolean)
                        _alter_default = value
                    End Set
                End Property

                Property AlterAlternateKey As Boolean
                    Get
                        Return _alter_alternate_key
                    End Get
                    Set(value As Boolean)
                        _alter_alternate_key = value
                    End Set
                End Property

#Region "Contructors"

                Sub New(ByVal AttributeName As String, _
                        ByVal AttributeReferencedEntity As String, _
                        ByVal AttributeDatatype As String, _
                        ByVal AttributeDefaultValue As String, _
                        ByVal AttributeOrdinal As Integer,
                        ByVal AttributeSortOrder As SortOrderType, _
                        ByVal AttributeOptionality As YesNoType, _
                        ByVal AttributeBusinessIdentifier As YesNoType, _
                        ByVal AttributeDescription As String, _
                        ByVal AttributeDistribution As StatiticalDistribution, _
                        ByVal AttributeAddColumn As Boolean, _
                        ByVal AttributeDeleteColumn As Boolean, _
                        ByVal AttributeAlterColumn As Boolean, _
                        ByVal AttributeAlterForeignKey As Boolean, _
                        ByVal AttributeAlterDefault As Boolean, _
                        ByVal AttributeAlterAlternateKey As Boolean)

                    Name = AttributeName
                    ReferencedEntity = AttributeReferencedEntity
                    Datatype = AttributeDatatype
                    DefaultValue = AttributeDefaultValue
                    Ordinal = AttributeOrdinal
                    SortOrder = AttributeSortOrder
                    Optionality = AttributeOptionality
                    BusinessIdentifier = AttributeBusinessIdentifier
                    Description = AttributeDescription
                    Distribution = AttributeDistribution
                    AddColumn = AttributeAddColumn
                    DeleteColumn = AttributeDeleteColumn
                    AlterColumn = AttributeAlterColumn
                    AlterForeignKey = AttributeAlterForeignKey
                    AlterDefault = AttributeAlterDefault
                    AlterAlternateKey = AttributeAlterAlternateKey

                End Sub

#End Region

#Region "Types"

                Enum SortOrderType As UShort
                    asc = 1
                    desc = 2
                End Enum

                Enum StatiticalDistribution As UShort
                    discrete = 1
                    continuous = 2
                End Enum

#End Region

            End Class ' EntityAttribute

        End Class ' Entity

    End Class ' Construct

End Class
