Imports System
Imports System.Data
Imports System.Data.Sql
Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports System.Runtime.InteropServices
Imports EDW.Common.SqlClientOutbound
Imports EDW.PersistentStagingArea.FrameworkInstallation

Partial Public Class PSA

#Region "CLR Exposed Methods"

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub BuildEntities(ByVal DatabaseName As String, _
                                    ByVal DatabaseSchema As String, _
                                    ByVal DatabaseEntity As String)

        ProcessCall(DatabaseName, DatabaseSchema, DatabaseEntity, False, False)

    End Sub

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub VerifyEntities(ByVal DatabaseName As String, _
                                     ByVal DatabaseSchema As String, _
                                     ByVal DatabaseEntity As String)

        ProcessCall(DatabaseName, DatabaseSchema, DatabaseEntity, True, False)

    End Sub

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub DropEntities(ByVal DatabaseName As String, _
                                    ByVal DatabaseSchema As String, _
                                    ByVal DatabaseEntity As String)

        ProcessCall(DatabaseName, DatabaseSchema, DatabaseEntity, False, True)

    End Sub

#End Region

    Private Shared Sub ProcessCall(ByVal DatabaseName As String, _
                                   ByVal DatabaseSchema As String, _
                                   ByVal DatabaseEntity As String, _
                                   ByVal vo As Boolean, _
                                   ByVal del As Boolean)

        Dim cnn As New SqlConnection("context connection=true")
        cnn.Open()

        PrintHeader()

        ' get the metadata from system tables
        Dim md As DataSet = GetMetadata(DatabaseName, DatabaseSchema, DatabaseEntity, cnn)

        ' if we have a promising set, i.e. the selects worked, we can move forward
        If Not md Is Nothing Then

            ' load up a variable with the usable construct
            Dim cons As New Construct(md)

            ' with we have records, we can begin with the ddl process, otherwise, alert client and exit
            If cons.EntityCount > 0 Then
                ProcessConstruct(cons, cnn, vo, del)
            Else
                PrintClientMessage(vbCrLf)
                PrintClientMessage("There is not defined metadata for the PSA in [dbo].[psa_entity_definition] and/or [dbo].[psa_attribute_definition] in the [master] database.")
                PrintClientMessage("You may manage data directly or via [soon to be authored] MS Excel Add-In.")
            End If

        Else
            ' TODO: make this message better OR make the tables?
            PrintClientMessage("error metadata=nothing")
        End If

        ' ensure connection object is closed
        cnn.Close()

    End Sub

    Private Shared Sub ProcessConstruct(pc As Construct, SqlCnn As SqlConnection, VerifyOnly As Boolean, DeleteObjects As Boolean)

        Dim st As Date = Now()
        Dim c As Integer = 0
        Dim e As Construct.Entity = Nothing
        Dim s As String = Nothing
        Dim fmt As String = "yyyy-MM-dd HH:mm:ss.ff"
        Dim cmd As New SqlCommand
        Dim ls As String ' logical signature
        Dim cs As String ' construct signature
        Dim lbl As String = "" ' updating label
        Dim plbl As String = "" ' printed label

        ' print header information
        PrintClientMessage("• Process started at " & st.ToString(fmt))
        PrintClientMessage("• Database Name: " & pc.DatabaseName)
        PrintClientMessage("• Database Compatibility: " & pc.DatabaseCompatibility.ToString)

        ' execute hashing algorithm needs
        ExecuteDDLCommand(pc.HashingAlgorithm, SqlCnn)
        PrintClientMessage("• Hashing algorithms are intact")

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

        ' move thru each entity
        For c = 0 To (i - 1)

            '
            ExecuteDDLCommand("begin transaction", SqlCnn)

            Try
                e = pc.Entities(c)

                ' check for no attributes
                If e.AttributeCount = 0 Then
                    lbl = "[NO COLUMNS DEFINED]"
                    GoTo nextc
                End If

                ' check for deletes
                If DeleteObjects = True Then
                    lbl = "[DELETED]"
                    DropAbstracts(e, SqlCnn)
                    DropEntity(e, SqlCnn)
                    GoTo nextc
                End If

                ' process schema def and sequence defintion (these never go away)
                If VerifyOnly = False Then
                    ExecuteDDLCommand(e.SchemaDefinition, SqlCnn)
                    ExecuteDDLCommand(e.SequenceDefinition, SqlCnn)
                End If

                ' get signatures
                With cmd
                    .Connection = SqlCnn
                    .CommandText = e.LogicalSignatureLookup
                    ls = .ExecuteScalar
                    .CommandText = e.ConstructSignatureLookup
                    cs = .ExecuteScalar
                End With

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

                ' make sure all the extedned properties are in order
                If VerifyOnly = False Then
                    SyncMetadata(e, SqlCnn)
                End If

nextc:
                ' alert completion
                plbl = (c + 1).ToString
                plbl = New String(" ", 4 - Len(plbl)) & plbl
                plbl += "  " & e.Domain
                PrintClientMessage(plbl + New String(".", 48 - Len(plbl)) + lbl, 2)

                ExecuteDDLCommand("commit transaction", SqlCnn)

            Catch ex As Exception

                ' alert completion
                lbl = "[ERROR]"
                plbl = (c + 1).ToString
                plbl = New String(" ", 4 - Len(plbl)) & plbl
                plbl += "  " & e.Domain
                PrintClientMessage(plbl + New String(".", 48 - Len(plbl)) + lbl, 2)

                PrintClientMessage("There was an error building " & e.Domain & ".", 8)
                PrintClientMessage(ex.Message, 8)
                PrintClientMessage(vbCrLf)

                ExecuteDDLCommand("rollback transaction", SqlCnn)

            Finally

            End Try

        Next c

        Dim ed As Date = Now()
        PrintClientMessage(vbCrLf & "• Process ended at " & ed.ToString(fmt))

        Dim min As Integer = DateDiff(DateInterval.Minute, st, ed)
        Dim sec As Integer = DateDiff(DateInterval.Second, st, ed) Mod 60
        PrintClientMessage("• Time to execute " & min.ToString & " min(s) " & sec.ToString & " sec(s)")

    End Sub

    Private Shared Sub DropEntity(e As Construct.Entity, SqlCnn As SqlConnection)

        ExecuteDDLCommand(e.RenameDefintion, SqlCnn)

    End Sub

    Private Shared Sub BuildEntity(e As Construct.Entity, SqlCnn As SqlConnection)

        ExecuteDDLCommand(e.TableDefinition, SqlCnn)
        ExecuteDDLCommand(e.ChangeTrackingDefinition, SqlCnn)
        ExecuteDDLCommand(e.AlternateKeyStatsDefinition, SqlCnn)
        ExecuteDDLCommand(e.DropTemporalGovernorDefinition, SqlCnn)
        ExecuteDDLCommand(e.TemporalGovernorDefinition, SqlCnn)

    End Sub

    Private Shared Sub DropAbstracts(e As Construct.Entity, SqlCnn As SqlConnection)
        ExecuteDDLCommand(e.DropRelatedObjectsDefintion, SqlCnn)
    End Sub

    Private Shared Sub BuildAbstracts(e As Construct.Entity, SqlCnn As SqlConnection)

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
        ExecuteDDLCommand(e.LoadQueueDefinition, SqlCnn)

        ' methods



    End Sub

    Private Shared Sub SyncMetadata(e As Construct.Entity, SqlCnn As SqlConnection)

        ExecuteDDLCommand(e.EntityMetadataDefinition, SqlCnn)
        ExecuteDDLCommand(e.AttributeMetadataDefintion, SqlCnn)

    End Sub

    Private Shared Sub PrintHeader()

        PrintClientMessage(My.Resources.SYS_SlalomTextArt1 & vbCrLf)
        PrintClientMessage("EDW Framework - Persistent Staging Area ('PSA') Definition")
        PrintClientMessage("Slalom Consulting | Copyright © 2014 | www.slalom.com" & vbCrLf & vbCrLf)

    End Sub

    Class Construct

#Region "Contruct Variables"
        Private _construct As New DataSet
        Private _entities As Entity()
        Private _databasecompatibility As SQLServerCompatibility
        Private _databasename As String
#End Region

#Region "Construct Properties"

        WriteOnly Property Construct As DataSet
            Set(value As DataSet)
                _construct = value

                Dim dp As DataTable = _construct.Tables("psa_database_properties")
                Dim edt As DataTable = _construct.Tables("psa_entity_definition")
                Dim adt As DataTable = _construct.Tables("psa_attribute_definition")
                Dim e As Entity
                Dim i As Integer = 0

                _databasename = CStr(dp.Rows(0).Item("database_name").ToString)
                _databasecompatibility = CUShort(dp.Rows(0).Item("compatibility_level").ToString)

                For Each edr In edt.Rows

                    ReDim Preserve _entities(i)
                    e = New Entity(edr("psa_schema"), _
                                   edr("psa_entity"), _
                                   If(IsDBNull(edr("psa_entity_description")), "", edr("psa_entity_description")), _
                                   If(IsDBNull(edr("psa_source_statement")), "", edr("psa_source_statement")), _
                                   If(IsDBNull(edr("psa_source_predicate_values")), "", edr("psa_source_predicate_values")), _
                                   edr("source_schema"), _
                                   edr("source_entity"), _
                                   edr("hash_large_objects"), _
                                   edr("psa_infer_deletions"), _
                                   If(IsDBNull(edr("etl_build_group")), "", edr("etl_build_group")), _
                                   edr("psa_logical_signature"), _
                                   edr("psa_construct_signature")
                                   )

                    For Each adr In adt.Select("psa_schema = '" & edr("psa_schema") & "' and psa_entity = '" & edr("psa_entity") & "'", "psa_attribute_ordinal")

                        e.AddEntityAttribute(adr("psa_attribute"), _
                                             adr("psa_attribute_ordinal"), _
                                             adr("psa_attribute_datatype"), _
                                             adr("psa_attribute_optional"), _
                                             adr("psa_attribute_business_identifier"), _
                                             adr("psa_attribute_sort"), _
                                             If(IsDBNull(adr("psa_attribute_description")), "", adr("psa_attribute_description")))

                    Next

                    _entities(i) = e
                    i += 1 ' increment
                Next

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

        ReadOnly Property RoleDefinition As String
            Get
                Return My.Resources.PSA_RoleDefinitions
            End Get
        End Property

        ReadOnly Property HashingAlgorithm As String
            Get
                Return My.Resources.PSA_HashingAlgorithmForPSA
            End Get
        End Property

        ReadOnly Property DatabaseName As String
            Get
                Return _databasename
            End Get
        End Property

        ReadOnly Property DatabaseCompatibility As SQLServerCompatibility
            Get
                Return _databasecompatibility
            End Get
        End Property

#End Region

#Region "Construct Types"
        Enum SQLServerCompatibility As UShort
            SQLServer2008 = 100
            SQLServer2012 = 110
            SQLServer2014 = 120
        End Enum

        Enum YesNoType As UShort
            Yes = 1
            No = 2
        End Enum
#End Region

#Region "Construct Constructors"

        Sub New(ByVal ds As DataSet)
            Construct = ds
        End Sub

#End Region

        Class Entity

#Region "Entity Variables"
            Private _schema As String
            Private _entity As String
            Private _description As String
            Private _sourcestatement As String
            Private _sourcepredicatevalues As String
            Private _sourceschema As String
            Private _sourceentity As String
            Private _hashlargeobjects As String
            Private _inferdeletions As String
            Private _buildgroup As String
            Private _logicalsig As String
            Private _constructsig As String
            Private _attribute As EntityAttribute()
#End Region

#Region "Entity Properties"

            Property Schema As String
                Get
                    Return _schema
                End Get
                Set(value As String)
                    _schema = value
                End Set
            End Property

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

            Property SourceStatement As String
                Get
                    Return _sourcestatement
                End Get
                Set(value As String)
                    _sourcestatement = value
                End Set
            End Property

            Property SourcePredicateValues As String
                Get
                    Return _sourcepredicatevalues
                End Get
                Set(value As String)
                    _sourcepredicatevalues = value
                End Set
            End Property

            Property SourceSchema As String
                Get
                    Return _sourceschema
                End Get
                Set(value As String)
                    _sourceschema = value
                End Set
            End Property

            Property SourceEntity As String
                Get
                    Return _sourceentity
                End Get
                Set(value As String)
                    _sourceentity = value
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

            Property InferDeletions As YesNoType
                Get
                    Return _inferdeletions
                End Get
                Set(value As YesNoType)
                    _inferdeletions = value
                End Set
            End Property

            Property BuildGroup As String
                Get
                    Return _buildgroup
                End Get
                Set(value As String)
                    _buildgroup = value
                End Set
            End Property

            Property LogicalSignature As String

                Get
                    Return _logicalsig
                End Get
                Set(value As String)
                    _logicalsig = value
                End Set
            End Property

            Property ConstructSignature As String
                Get
                    Return _constructsig
                End Get
                Set(ByVal value As String)
                    _constructsig = value
                End Set
            End Property

            ReadOnly Property LogicalSignatureLookup As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_LogicalSignatureLookup
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ReadOnly Property ConstructSignatureLookup As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_ConstructSignatureLookup
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
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
                    If Schema Is Nothing Or Entity Is Nothing Then
                        Return ""
                    Else
                        Return "[" & Schema & "].[" & Entity & "]"
                    End If
                End Get
            End Property

            ReadOnly Property Label As String
                Get
                    If Schema Is Nothing Or Entity Is Nothing Then
                        Return ""
                    Else
                        Return Schema & "." & Entity
                    End If
                End Get
            End Property

            ReadOnly Property SchemaDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_SchemaDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    Return sd
                End Get
            End Property

            ReadOnly Property SequenceDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_SequenceDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ReadOnly Property DropRelatedObjectsDefintion As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_DropRelatedObjects
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd

                End Get
            End Property

            ReadOnly Property RenameDefintion As String
                Get
                    Dim n As Date = Now().ToString
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

            ReadOnly Property TableDefinition As String
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

            ReadOnly Property AlternateKeyStatsDefinition As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""

                    For Each bia In _attribute
                        If bia.BusinessIdentifier = YesNoType.Yes And bia.Ordinal <> 1 Then
                            rstr += "create statistics [st : " & Label & " :: " & bia.Name & "] on " & Domain & vbCrLf & " (" & vbCrLf & "   [" & bia.Name & "]" & vbCrLf & " );" & vbCrLf & vbCrLf
                        End If
                    Next

                    Return If(rstr = "", "print 'no alternate key stats'", rstr)
                End Get
            End Property

            ReadOnly Property ChangeTrackingDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_ChangeTrackingDefinition
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    Return sd
                End Get
            End Property

            ReadOnly Property DropTemporalGovernorDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_DropTemporalGovernorDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{label}}}", Label)
                    Return sd
                End Get
            End Property

            ReadOnly Property TemporalGovernorDefinition As String
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

            ReadOnly Property ControlViewDefinition As String
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

            ReadOnly Property ControlInsertDefinition As String
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
                    Return sd
                End Get
            End Property

            ReadOnly Property ControlUpdateDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 9)

                    Dim sd As String
                    sd = My.Resources.PSA_ControlUpdateDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{label}}}", Label)
                    sd = Replace(sd, "{{{updateset}}}", UpdateChunk)
                    sd = Replace(sd, "{{{joinset}}}", JoinChunk)
                    sd = Replace(sd, "{{{hashset}}}", HashChunk)
                    sd = Replace(sd, "{{{columnset}}}", cs)
                    sd = Replace(sd, "{{{updatekeyset}}}", UpdateKeyChunk)
                    Return sd
                End Get
            End Property

            ReadOnly Property ControlDeleteDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 9)

                    Dim sd As String
                    sd = My.Resources.PSA_ControlDeleteDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{label}}}", Label)
                    sd = Replace(sd, "{{{updateset}}}", UpdateChunk)
                    sd = Replace(sd, "{{{joinset}}}", JoinChunk)
                    sd = Replace(sd, "{{{hashset}}}", HashChunk)
                    sd = Replace(sd, "{{{columnset}}}", cs)
                    Return sd
                End Get
            End Property

            ReadOnly Property ControlSecurityDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_ControlSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ReadOnly Property AsIsViewDefinition As String
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

            ReadOnly Property AsIsTriggerDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_AsIsTriggerDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{label}}}", Label)
                    Return sd
                End Get
            End Property

            ReadOnly Property AsIsSecurityDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_AsIsSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ReadOnly Property AsWasViewDefinition As String
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

            ReadOnly Property AsWasTriggerDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_AsWasTriggerDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{label}}}", Label)
                    Return sd
                End Get
            End Property

            ReadOnly Property AsWasSecurityDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 3)

                    Dim sd As String
                    sd = My.Resources.PSA_AsWasSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ReadOnly Property BatchCountViewDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_BatchCountViewDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    Return sd
                End Get
            End Property

            ReadOnly Property BatchCountTriggerDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_BatchCountTriggerDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{label}}}", Label)
                    Return sd
                End Get
            End Property

            ReadOnly Property BatchCountSecurityDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_BatchCountSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ReadOnly Property AsOfFunctionDefinition As String
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

            ReadOnly Property AsOfSecurityDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 3)

                    Dim sd As String
                    sd = My.Resources.PSA_AsOfSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property

            ReadOnly Property ChangesFunctionDefinition As String
                Get
                    Dim cs As String = ColumnSetChunk("", 3)
                    cs = Left(cs, Len(cs) - 1) ' remove the last comma

                    Dim sd As String
                    sd = My.Resources.PSA_ChangesFunctionDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{domain}}}", Domain)
                    sd = Replace(sd, "{{{columnset}}}", cs)
                    Return sd
                End Get
            End Property

            ReadOnly Property ChangesSecurityDefinition As String
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_ChangesSecurityDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    Return sd
                End Get
            End Property


            ReadOnly Property LoadQueueDefinition
                Get
                    Dim sd As String
                    sd = My.Resources.PSA_LoadQueueDefinition
                    sd = Replace(sd, "{{{schema}}}", Schema)
                    sd = Replace(sd, "{{{entity}}}", Entity)
                    sd = Replace(sd, "{{{bichunk}}}", BusinessIdentifierChunk)
                    sd = Replace(sd, "{{{akchunk}}}", AlternateKeyChunkForQueue)
                    sd = Replace(sd, "{{{attrchunk}}}", AttributeChunk)
                    Return sd
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
                    ep = Replace(ep, "{{{value}}}", Description)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Source Statement")
                    ep = Replace(ep, "{{{value}}}", SourceStatement)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Source Predicate Values")
                    ep = Replace(ep, "{{{value}}}", SourcePredicateValues)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Source Schema")
                    ep = Replace(ep, "{{{value}}}", SourceSchema)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Source Entity")
                    ep = Replace(ep, "{{{value}}}", SourceEntity)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Use Large Object Hashing Algorithm")
                    ep = Replace(ep, "{{{value}}}", HashLargeObjects.ToString)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Infer Deltions from Source Domain")
                    ep = Replace(ep, "{{{value}}}", InferDeletions.ToString)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "ETL Build Group")
                    ep = Replace(ep, "{{{value}}}", BuildGroup)
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

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Logical Signature")
                    ep = Replace(ep, "{{{value}}}", LogicalSignature)
                    ep += vbCrLf

                    ep += My.Resources.PSA_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Construct Signature")
                    ep = Replace(ep, "{{{value}}}", ConstructSignature)
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
                        ep = Replace(ep, "{{{value}}}", bia.Description)
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

            ReadOnly Property Attributes(ByVal AttributeNumber As Integer) As EntityAttribute
                Get
                    Return _attribute(AttributeNumber)
                End Get
            End Property

            Private ReadOnly Property BusinessIdentifierChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = YesNoType.Yes Then
                            rstr += "   [" & bia.Name & "] " & bia.Datatype & " " & If(bia.Optionality = YesNoType.No, "not null,", "null,") & vbCrLf
                        End If
                    Next

                    Return rstr
                End Get
            End Property

            Private ReadOnly Property AttributeChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = YesNoType.No Then
                            rstr += "   [" & bia.Name & "] " & bia.Datatype & " " & If(bia.Optionality = YesNoType.No, "not null,", "null,") & vbCrLf
                        End If
                    Next

                    Return rstr
                End Get
            End Property

            Private ReadOnly Property AlternateKeyChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = "("

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = YesNoType.Yes Then
                            rstr += "[" & bia.Name & "] " & If(bia.SortOrder = EntityAttribute.SortOrderType.asc, "asc,", "desc,")
                        End If
                    Next

                    Return rstr & "[psa_entity_sequence] desc)"
                End Get
            End Property

            Private ReadOnly Property AlternateKeyChunkForQueue As String
                Get
                    Dim rstr As String = AlternateKeyChunk
                    rstr = Replace(rstr, ",[psa_entity_sequence] desc)", ")")
                    Return rstr
                End Get
            End Property

            Private ReadOnly Property HashChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = YesNoType.No Then
                            rstr += "[" & bia.Name & "],"
                        End If
                    Next

                    Return rstr & "N'Slalom.Framework' [HA]"
                End Get
            End Property

            Private ReadOnly Property UpdateChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = YesNoType.No Then
                            rstr += "      [" & bia.Name & "]=s.[" & bia.Name & "]," & vbCrLf
                        End If
                    Next

                    If Len(rstr) > 0 Then
                        rstr = Left(rstr, Len(rstr) - 2)
                    Else
                        rstr = "      -- there are no attributes in this psa entity, i.e. this code will never be reached..."
                    End If

                    Return rstr
                End Get
            End Property

            Private ReadOnly Property JoinChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim spacer As New String(" ", Len(Domain) + 12)

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = YesNoType.Yes Then
                            If bia.Optionality = YesNoType.No Then
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

            Private ReadOnly Property UpdateKeyChunk As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim spacer As New String(" ", Len(Domain) + 12)

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        If bia.BusinessIdentifier = YesNoType.Yes Then
                            rstr += "   if update ([" & bia.Name & "]) begin;" & vbCrLf
                            rstr += "      raiserror(N'The Source Native Key (SNK), or Business Identifier, [" & bia.Name & "] cannot be updated. Insert a new record the delete the old record.',16,1);" & vbCrLf
                            rstr += "   end;" & vbCrLf & vbCrLf
                        End If
                    Next

                    Return Left(rstr, Len(rstr) - 2)
                End Get
            End Property

            Private ReadOnly Property ColumnSetChunk(ByVal ColumnSetAlias As String, ByVal ColumnPadding As UShort) As String
                Get
                    Dim bia As EntityAttribute = Nothing
                    Dim rstr As String = ""
                    Dim pad As New String(" ", ColumnPadding)
                    ColumnSetAlias = If(ColumnSetAlias = "", "", ColumnSetAlias & ".")

                    For Each bia In _attribute.OrderBy(Function(EntityAttribute) EntityAttribute.BusinessIdentifier).ThenBy(Function(EntityAttribute) EntityAttribute.Ordinal)
                        rstr += pad & ColumnSetAlias & "[" & bia.Name & "]," & vbCrLf
                    Next

                    Return Left(rstr, Len(rstr) - 2)
                End Get
            End Property

#End Region

            Sub New(ByVal NewSchema As String, ByVal NewEntity As String, ByVal NewDescription As String, _
                    ByVal NewSourceStatement As String, ByVal NewSourcePredicateValues As String, _
                    ByVal NewSourceSchema As String, ByVal NewSourceEntity As String, ByVal NewHashLargeObjects As String, _
                    ByVal NewInferDeletions As String, ByVal NewBuildGroup As String, ByVal NewLogicalSignature As String, _
                    ByVal NewConstructSignature As String)

                Schema = NewSchema
                Entity = NewEntity
                Description = NewDescription
                SourceStatement = NewSourceStatement
                NewSourcePredicateValues = NewSourcePredicateValues
                SourceSchema = NewSourceSchema
                SourceEntity = NewSourceEntity
                HashLargeObjects = If(NewHashLargeObjects = "Yes", YesNoType.Yes, YesNoType.No)
                InferDeletions = If(NewInferDeletions = "Yes", YesNoType.Yes, YesNoType.No)
                BuildGroup = NewBuildGroup
                LogicalSignature = NewLogicalSignature
                ConstructSignature = NewConstructSignature

            End Sub

            Sub AddEntityAttribute(ByVal AttributeName As String, ByVal AttributeOrdinal As Integer, ByVal AttributeDatatype As String, _
                                   ByVal AttributeOptionality As String, ByVal AttributeBusinessIdentifier As String, _
                                   ByVal AttributeSortOrder As String, ByVal AttributeDescription As String)

                Dim ao As UShort = If(AttributeOptionality = "No", 2, 1)
                Dim bi As UShort = If(AttributeBusinessIdentifier = "No", 2, 1)
                Dim so As UShort = If(AttributeSortOrder = "desc", 2, 1)

                If IsNothing(_attribute) Then
                    ReDim _attribute(0)
                    _attribute(0) = New EntityAttribute(AttributeName, AttributeOrdinal, AttributeDatatype, ao, bi, so, AttributeDescription)
                Else
                    ReDim Preserve _attribute(_attribute.Length)
                    _attribute(_attribute.Length - 1) = New EntityAttribute(AttributeName, AttributeOrdinal, AttributeDatatype, ao, bi, so, AttributeDescription)
                End If

            End Sub

            Class EntityAttribute
                Private _name As String
                Private _ordinal As Integer
                Private _datatype As String
                Private _sortorder As SortOrderType
                Private _optionality As YesNoType
                Private _businessidentifier As YesNoType
                Private _description As String

                Property Name As String
                    Get
                        Return _name
                    End Get
                    Set(value As String)
                        _name = value
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
                            Case Else
                                Return _datatype
                        End Select
                    End Get
                    Set(value As String)
                        _datatype = value
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

#Region "Contructors"

                Sub New(ByVal AttributeName As String, ByVal AttributeOrdinal As Integer, ByVal AttributeDatatype As String, _
                        ByVal AttributeOptionality As YesNoType, ByVal AttributeBusinessIdentifier As YesNoType, _
                        ByVal AttributeSortOrder As SortOrderType, ByVal AttributeDescription As String)

                    Name = AttributeName
                    Ordinal = AttributeOrdinal
                    Datatype = AttributeDatatype
                    Optionality = AttributeOptionality
                    BusinessIdentifier = AttributeBusinessIdentifier
                    SortOrder = AttributeSortOrder
                    Description = AttributeDescription

                End Sub

#End Region

#Region "Types"

                Enum SortOrderType As UShort
                    asc = 1
                    desc = 2
                End Enum

#End Region

            End Class ' EntityAttribute

        End Class ' Entity

    End Class ' Contruct

End Class ' PSA