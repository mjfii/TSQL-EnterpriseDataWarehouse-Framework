Imports System
Imports System.Data
Imports System.Data.Sql
Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports System.Runtime.CompilerServices
Imports System.Runtime.InteropServices
Imports System.Linq
Imports EDW.Common.SqlClientOutbound
Imports EDW.Common.InstanceSettings
Imports EDW.AnalyticReportingArea.FrameworkInstallation

Module BuildExtentions

    ''' <summary>A description of what the function does.</summary>
    ''' <param name="ExistingEntities">Description of the first parameter</param>
    ''' <param name="NewEntity">Description of the second parameter</param>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Sub AddEntityToConstruct(Of Entity)(ByRef ExistingEntities As Entity(), NewEntity As Entity)
        If ExistingEntities IsNot Nothing Then
            Array.Resize(ExistingEntities, ExistingEntities.Length + 1)
            ExistingEntities(ExistingEntities.Length - 1) = NewEntity
        Else
            ReDim ExistingEntities(0)
            ExistingEntities(0) = NewEntity
        End If
    End Sub

    ''' <summary>A description of what the function does.</summary>
    ''' <param name="ExistingAttributes">Description of the first parameter</param>
    ''' <param name="NewAttribute">Description of the second parameter</param>
    ''' <remarks></remarks>
    ''' <typeparam name="EntityAttribute">asdf</typeparam>
    <Extension()> _
    Friend Sub AddAttributeToEntity(Of EntityAttribute)(ByRef ExistingAttributes As EntityAttribute(), NewAttribute As EntityAttribute)
        If ExistingAttributes IsNot Nothing Then
            Array.Resize(ExistingAttributes, ExistingAttributes.Length + 1)
            ExistingAttributes(ExistingAttributes.Length - 1) = NewAttribute
        Else
            ReDim ExistingAttributes(0)
            ExistingAttributes(0) = NewAttribute
        End If
    End Sub

    <Extension()> _
    Friend Sub AddMember(Of T)(ByRef Members As T(), NewMember As T)
        If Members IsNot Nothing Then
            Array.Resize(Members, Members.Length + 1)
            Members(Members.Length - 1) = NewMember
        Else
            ReDim Members(0)
            Members(0) = NewMember
        End If
    End Sub


    ''' <summary>Returns the name of the code.</summary>
    <Extension()> _
    Friend Function StringToEntityType(ByVal InputString As String) As ARA.Model.EntityType
        Return If(InputString = "D", ARA.Model.EntityType.TypeII, ARA.Model.EntityType.TypeI)
    End Function

    ''' <summary>A description of what the function does. </summary>
    ''' <param name="InputString">Description of the first parameter</param>
    ''' <returns>Description for what the function returns</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Function StringToYesNoType(ByVal InputString As String) As ARA.Model.YesNoType
        Return If(InputString = "Yes", ARA.Model.YesNoType.Yes, ARA.Model.YesNoType.No)
    End Function

    ''' <summary>A description of what the function does. </summary>
    ''' <param name="InputString">Description of the first parameter</param>
    ''' <returns>Description for what the function returns</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Function RemoveNulls(ByVal InputString As String) As String
        Return If(IsDBNull(InputString), "", InputString)
    End Function

    ''' <summary>A description of what the function does. </summary>
    ''' <param name="InputString">Description of the first parameter</param>
    ''' <returns>Description for what the function returns</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Function StringToBool(ByVal InputString As String) As Boolean
        Return CBool(InputString)
    End Function

    ''' <summary>A description of what the function does. </summary>
    ''' <param name="InputString">Description of the first parameter</param>
    ''' <returns>Description for what the function returns</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Function StringToSortOrderType(ByVal InputString As String) As ARA.Model.SortOrderType
        Return If(InputString = "desc", ARA.Model.SortOrderType.desc, ARA.Model.SortOrderType.asc)
    End Function

    ''' <summary>A description of what the function does. </summary>
    ''' <param name="InputString">Description of the first parameter</param>
    ''' <returns>Description for what the function returns</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Function StringToStatisticalDistribution(ByVal InputString As String) As ARA.Model.StatiticalDistribution
        Return If(InputString = "D", ARA.Model.StatiticalDistribution.discrete, _
                  ARA.Model.StatiticalDistribution.continuous)
    End Function


End Module

''' <summary></summary>
''' <remarks></remarks>
Public Class ARA

#Region "CLR Exposed Methods"

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub BuildEntities(ByVal DatabaseName As SqlString,
                                    ByVal BuildType As SqlString)

        Dim cnn As New SqlConnection("context connection=true")
        cnn.Open() ' make a property

        ProcessCallToBuild(cnn, DatabaseName, BuildType)

        cnn.Close()
    End Sub

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub GetModel()

        Dim model_query As String = My.Resources.ARA_ModelGet

        ReturnClientResults(model_query)

    End Sub

    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub SetModel(ByVal model As SqlXml)

        If IsNothing(model) Then
            PrintClientMessage("You cannot process a null model.")
            Exit Sub
        End If

        Dim model_query As String = Replace(My.Resources.ARA_ModelSet, "{{{xml}}}", model.Value.ToString)

        Dim cnn As New SqlConnection("context connection=true")
        cnn.Open()

        ExecuteDDLCommand(model_query, cnn)

        cnn.Close()

    End Sub

#End Region

#Region "ARA Methods"

    ''' <summary>Execute the call to build the model from the specified metadata.</summary>
    ''' <param name="DatabaseName">The name of the database name on the instance that the model will deployed on.</param>
    ''' <param name="BuildType">If set to 'true', nothing will be built, but the process will be verify against the database.</param>
    ''' <remarks>...</remarks>
    Private Shared Sub ProcessCallToBuild(ByVal SqlCnn As SqlConnection,
                                          ByVal DatabaseName As String,
                                          ByVal BuildType As String)

        PrintHeader()

        ' if the user is part of the sysadmin role, move along
        If Not UserIsSysAdmin(SqlCnn) Then Exit Sub

        ' make sure server objects are in place
        AddInstanceObjects(SqlCnn)

        ' validate incoming database
        If Not DatabaseIsValid(DatabaseName, SqlCnn) Then Exit Sub

        ' make sure database objects are in place
        AddDatabaseObjects(SqlCnn, DatabaseName)

        ' get the metadata from system tables
        Dim md As DataSet = GetMetadata(DatabaseName, SqlCnn)

        ' if we have a promising set, i.e. the selects worked, we can move forward
        If md Is Nothing Then
            ' alert the tables arent there and make them
            ExecuteDDLCommand(My.Resources.SYS_ARAMetadataTableDefinition, SqlCnn)
            PrintClientMessage(vbCrLf)
            PrintClientMessage("The ARA Framework was not ready for use. The required system tables have been built; you can now use the [dbo].[ara_entity_definition] and")
            PrintClientMessage("[dbo].[ara_attribute_definition] tables in the [master] database to add the metadata construct elements to build each of the ARA objects.")
            Exit Sub
        End If


        ' load up a variable with the usable construct
        Dim cons As New Model(md)


        ' notify of invalidations to the model
        PrintClientMessage("• Determining model validation: ")
        If cons.ModelInvalidations IsNot Nothing Then
            PrintClientMessage("  -> Data model is INVALID ")
            For i = 0 To cons.ModelInvalidations.Length - 1
                PrintClientMessage("-> " & cons.ModelInvalidations(i), 2)
            Next
        Else
            PrintClientMessage("  -> Data model is VALID ")
        End If

        ' notify of warnings to the model
        PrintClientMessage("• Identifying non-fatal issues: ")
        If cons.ModelWarnings IsNot Nothing Then
            For i = 0 To cons.ModelWarnings.Length - 1
                PrintClientMessage("-> " & cons.ModelWarnings(i), 2)
            Next
        Else
            PrintClientMessage("  -> Data model did NOT produce any WARNINGS")
        End If

        If cons.ModelInvalidations IsNot Nothing Then Exit Sub


        ' with we have records, we can begin with the DDL process, otherwise, alert client and exit
        If cons.EntityCount > 0 Then
            BuildValidModel(cons, SqlCnn, BuildType)
        Else
            PrintClientMessage(vbCrLf)
            PrintClientMessage("There is not defined metadata for the ARA in [dbo].[ara_entity_definition] and/or [dbo].[ara_attribute_definition] in the [master] database.")
            PrintClientMessage("You may manage data directly or via the MS Excel Add-In.")
        End If ' metadata content exists


    End Sub

    ''' <summary>...</summary>
    ''' <param name="ModelToProcess"></param>
    ''' <param name="DatabaseConnection"></param>
    ''' <param name="BuildType"></param>
    ''' <remarks>...</remarks>
    Private Shared Sub BuildValidModel(ByVal ModelToProcess As Model,
                                       ByVal DatabaseConnection As SqlConnection,
                                       ByVal BuildType As String)

        Const space As String = " "

        Dim Entity As Model.Entity = Nothing
        Dim Entities As Model.Entity() = Nothing
        Dim EntityAttributes As Model.Entity.EntityAttribute() = Nothing
        Dim EntityNumber As Integer

        Dim StartTime As Date = Now()
        Dim fmt As String = "yyyy-MM-dd HH:mm:ss.ff..."

        ' print header information
        PrintClientMessage(vbCrLf)
        PrintClientMessage("• Process construct started at " & StartTime.ToString(fmt))
        PrintClientMessage("• Database Name: " & ModelToProcess.DatabaseName)
        PrintClientMessage("• Database Compatibility: " & ModelToProcess.DatabaseCompatibility.ToString)

        Dim EntityCount As Integer = ModelToProcess.EntityCount - 1

        '
        If BuildType = "VERIFY" Then ExecuteDDLCommand("set parseonly on;", DatabaseConnection)

        ' notify esitmated changes in model
        PrintClientMessage(vbCrLf)
        PrintClientMessage("• Estimated changes to data model: ")

        For EntityNumber = 0 To EntityCount

            Entity = ModelToProcess.Entities(EntityNumber)
            EntityAttributes = Entity.Attributes

            If Entity.CreateEntity Then
                PrintClientMessage(New String(space, 3) & Entity.Domain.ToString & " -> Create Entity")

            ElseIf Entity.DeleteEntity Then
                PrintClientMessage(New String(space, 3) & Entity.Domain.ToString & " -> Delete Entity")

            ElseIf Not Entity.HasChange Then
                PrintClientMessage(New String(space, 3) & Entity.Domain.ToString & " -> NO CHANGE")

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
            ExecuteDDLCommand("begin transaction;", DatabaseConnection)

            ' create new entities
            Entities = ModelToProcess.CreatedEntities

            For Each Entity In Entities
                ExecuteDDLCommand(Entity.CreateEntityDefinition, DatabaseConnection)
            Next Entity

            For Each Entity In Entities
                ExecuteDDLCommand(Entity.AlternateKeyDefinition, DatabaseConnection)

                'contraint definition
                ExecuteDDLCommand(Entity.ForeignKeyDefinition(Entity.ForeignKeyAttributes), DatabaseConnection)
                ExecuteDDLCommand(Entity.DefaultDefinition(Entity.DefaultValuedAttributes), DatabaseConnection)

                ' abstract creation
                ExecuteDDLCommand(Entity.ControlCreateDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.ControlInsertDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.ControlUpdateDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.ControlDeleteDefinition, DatabaseConnection)

                ' sync metadata
                ExecuteDDLCommand(Entity.EntityMetadataDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.AttributeMetadataDefintion, DatabaseConnection)

            Next Entity

            ' alter existing entites
            Entities = ModelToProcess.ChangedEntities

            For Each Entity In Entities

                ExecuteDDLCommand(Entity.ControlDropDefinition, DatabaseConnection)

                ' if the business identifier has changed, drop it, in all forms
                If Entity.HasAlterAlternateKey Then
                    ExecuteDDLCommand(Entity.DropAlternateKeyDefinition, DatabaseConnection)
                End If

                '
                ExecuteDDLCommand(Entity.AddColumnDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.AlterColumnDefinition, DatabaseConnection)

                ' drop any columns
                If Entity.HasDeleteColumn Then
                    ExecuteDDLCommand(Entity.DropColumnDefinition, DatabaseConnection)
                End If

                ' since we dropped it above, rebuild it, again, in all forms
                If Entity.HasAlterAlternateKey Then
                    ExecuteDDLCommand(Entity.AlternateKeyDefinition, DatabaseConnection)
                End If

                ' add back the control defintion(s)
                ExecuteDDLCommand(Entity.ControlCreateDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.ControlInsertDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.ControlUpdateDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.ControlDeleteDefinition, DatabaseConnection)

                ' sync metadata
                ExecuteDDLCommand(Entity.EntityMetadataDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.AttributeMetadataDefintion, DatabaseConnection)

            Next Entity

            ' drop entites
            Entities = ModelToProcess.DeletedEntities

            For Each Entity In Entities
                ExecuteDDLCommand(Entity.ControlDropDefinition, DatabaseConnection)
                ExecuteDDLCommand(Entity.DeleteEntityDefinition, DatabaseConnection)
            Next

            ' unchanged entities


            '
            ExecuteDDLCommand("commit transaction;", DatabaseConnection)

        Catch ex As Exception
            ExecuteDDLCommand("rollback transaction;", DatabaseConnection)

            PrintClientMessage(space)
            PrintClientMessage("An ERROR occured with with entity [" & Entity.Entity & "]:", 3)
            PrintClientMessage(ex.Message.ToString, 3)
            PrintClientMessage("The transaction has been rolled back and logged.", 3)
            PrintClientMessage(vbCrLf)
        End Try

exit_sub:
        Dim ed As Date = Now()
        PrintClientMessage(vbCrLf & "• Process ended at " & ed.ToString(fmt))

        Dim min As Integer = DateDiff(DateInterval.Minute, StartTime, ed)
        Dim sec As Integer = DateDiff(DateInterval.Second, StartTime, ed) Mod 60
        PrintClientMessage("• Time to execute " & min.ToString & " min(s) " & sec.ToString & " sec(s)")

    End Sub

    ''' <summary>...</summary>
    ''' <remarks>...</remarks>
    Private Shared Sub PrintHeader()

        PrintClientMessage(My.Resources.SYS_SlalomTextArt1 & vbCrLf)
        PrintClientMessage("EDW Framework - Analytics & Reporting Area ('ARA') Definition")
        PrintClientMessage("Slalom Consulting | Copyright © 2015 | www.slalom.com" & vbCrLf & vbCrLf)

    End Sub

#End Region

    Protected Friend Class Model

#Region "Model Variables"
        Private _entities As Entity()
        Private _database_compatibility As SQLServerCompatibility
        Private _database_name As String
        Private _new_entities As Entity()
        Private _load_successful As Boolean = True
        Private _model_invalidations As String()
        Private _model_warnings As String()
#End Region

#Region "Model Properties"

        ''' <summary>Returns the number of entities residing in the model.</summary>
        Protected Friend ReadOnly Property EntityCount As Integer
            Get
                If IsNothing(_entities) Then
                    Return 0
                Else
                    Return _entities.Length
                End If
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property Entities(EntityNumber As Integer) As Entity
            Get
                Return _entities(EntityNumber)
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property Entities(EntityName As String) As Entity
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

        ''' <summary></summary>
        Protected Friend ReadOnly Property DatabaseName As String
            Get
                Return _database_name
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property DatabaseCompatibility As SQLServerCompatibility
            Get
                Return _database_compatibility
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property Entities As Entity()
            Get
                Return _entities
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property CreatedEntities As Entity()
            Get
                Return (From ne As Entity In _entities Where ne.CreateEntity = True Select ne).ToArray
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property ChangedEntities As Entity()
            Get
                Return (From ce As Entity In _entities Where ce.HasChange = True And ce.DeleteEntity = False And ce.CreateEntity = False Select ce).ToArray
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property DeletedEntities As Entity()
            Get
                Return (From de As Entity In _entities Where de.DeleteEntity = True Select de).ToArray
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property UnchangedEntities As Entity()
            Get
                Return (From ue As Entity In _entities Where ue.HasChange = False And ue.DeleteEntity = False And ue.CreateEntity = False Select ue).ToArray
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property ModelInvalidations As String()
            Get
                Return _model_invalidations
            End Get
        End Property

        ''' <summary></summary>
        Protected Friend ReadOnly Property ModelWarnings As String()
            Get
                Return _model_warnings
            End Get
        End Property

#End Region

#Region "Model Types"

        Public Enum SQLServerCompatibility As UShort
            SQLServer2008 = 100
            SQLServer2012 = 110
            SQLServer2014 = 120
            SQLServer2016 = 130
        End Enum

        Public Enum YesNoType As UShort
            Yes = 1
            No = 2
        End Enum

        Public Enum EntityType As UShort
            TypeII = 1
            TypeI = 2
        End Enum

        Public Enum BuildAction As UShort
            AddEntity = 1
            UpdateEntity = 2
            DeleteEntity = 3
            None = 4
        End Enum

        Public Enum AttributeType As UShort
            BusinessIdentifier = 2
            Atomic = 4
        End Enum

        Public Enum SortOrderType As UShort
            asc = 1
            desc = 2
        End Enum

        Public Enum StatiticalDistribution As UShort
            discrete = 1
            continuous = 2
        End Enum

#End Region

#Region "Model Constants"
        Private Const Spacer As String = " "
        Private Const EmptyString As String = ""
        Private Const Null As String = "null"
        Private Const NotNull As String = "not null"


        Private Const ara_select_string As String = "ara_entity='{0}'"

        Private Const ara_entity As String = "ara_entity"
        Private Const ara_entity_description As String = "ara_entity_description"


        Private Const ara_attribute_ordinal As String = "ara_attribute_ordinal"
#End Region

#Region "Model Constructors"

        ''' <summary></summary>
        Protected Friend Sub New(ByVal ModelDataset As DataSet)

            ' load attributes related to the database alone
            Try
                Dim DatabaseAttributes As DataRow = ModelDataset.Tables("ara_database_properties").Rows(0)
                _database_name = DatabaseAttributes.Item("database_name").ToString
                _database_compatibility = CUShort(DatabaseAttributes.Item("compatibility_level").ToString)
            Catch ex As Exception
                _load_successful = False
                PrintClientMessage(ex.Message)
                Exit Sub
            End Try

            ' loop through each entity and load all object via the entity class constructors
            Try
                '
                Dim EntityTable As DataTable = ModelDataset.Tables("ara_entity_definition")
                Dim AttributeTable As DataTable = ModelDataset.Tables("ara_attribute_definition")

                '
                Dim EntityName As String
                Dim EntityRow As DataRow ' for use in the for loop
                Dim AttributeRow As DataRow ' for use in the subset 
                Dim AttributeRows As DataRow() ' the subset for entity attribute looping
                Dim NewEntity As Entity

                ' move through each of the entities and add them to the class
                For Each EntityRow In EntityTable.Rows

                    ' define the entity name for use in the loop and
                    EntityName = EntityRow(ara_entity).ToString
                    AttributeRows = AttributeTable.Select(String.Format(ara_select_string, EntityName), ara_attribute_ordinal)

                    NewEntity = New Entity(EntityName,
                                           EntityRow(ara_entity_description).ToString,
                                           EntityRow("ara_entity_type").ToString.StringToEntityType,
                                           EntityRow("ara_hash_large_objects").ToString.StringToYesNoType,
                                           EntityRow("ara_new_entity_type").ToString.StringToEntityType,
                                           EntityRow("ara_entity_create").ToString.StringToBool,
                                           EntityRow("ara_entity_delete").ToString.StringToBool,
                                           _model_invalidations,
                                           _model_warnings)

                    ' roll through each of the attributes assign to the given entity, add them as a new attribute
                    For Each AttributeRow In AttributeRows

                        NewEntity.NewAttribute(AttributeRow("ara_attribute_name").ToString,
                                               AttributeRow("ara_attribute_referenced_entity").ToString.RemoveNulls,
                                               AttributeRow("ara_attribute_referenced_entity_type").ToString.StringToEntityType,
                                               AttributeRow("ara_attribute_datatype").ToString,
                                               AttributeRow("ara_attribute_default").ToString.RemoveNulls,
                                               AttributeRow("ara_attribute_ordinal"),
                                               AttributeRow("ara_attribute_sort").ToString.StringToSortOrderType,
                                               AttributeRow("ara_attribute_optional").ToString.StringToYesNoType,
                                               AttributeRow("ara_attribute_business_identifier").ToString.StringToYesNoType,
                                               AttributeRow("ara_attribute_description").ToString.RemoveNulls,
                                               AttributeRow("ara_attribute_distribution").ToString.StringToStatisticalDistribution,
                                               AttributeRow("ara_attribute_add_column").ToString.StringToBool,
                                               AttributeRow("ara_attribute_delete_column").ToString.StringToBool,
                                               AttributeRow("ara_attribute_alter_column").ToString.StringToBool,
                                               AttributeRow("ara_attribute_alter_foreign_key").ToString.StringToBool,
                                               AttributeRow("ara_attribute_alter_default").ToString.StringToBool,
                                               AttributeRow("ara_attribute_alter_alternate_key").ToString.StringToBool,
                                               _model_invalidations,
                                               _model_warnings)

                    Next

                    If NewEntity.AttributeCount = 0 Then
                        _model_invalidations.AddMember("Entity [" & EntityName & "] must have at least one attributes defined.")
                    End If

                    If Not NewEntity.HasBusinessKey And Not NewEntity.DeleteEntity Then
                        _model_invalidations.AddMember("Entity [" & EntityName & "] must have a business identifier.")
                    End If

                    _entities.AddEntityToConstruct(NewEntity)

                Next

            Catch sqlex As SqlException
                _load_successful = False
                PrintClientMessage(sqlex.Message)
            Catch ex As Exception
                _load_successful = False
                PrintClientMessage(ex.Message)
            End Try

        End Sub

#End Region

        Protected Friend Class Entity

#Region "Entity Variables"
            Private _entity As String
            Private _description As String
            Private _entity_type As EntityType
            Private _new_entity_type As EntityType
            Private _hash_large_objects As Model.YesNoType
            Private _object_id As Integer
            Private _create_entity As Boolean
            Private _delete_entity As Boolean
            Private _attribute As EntityAttribute()
            Private _has_business_key As Boolean = False
            Private _has_add_column As Boolean = False
            Private _has_delete_column As Boolean = False
            Private _has_alter_column As Boolean = False
            Private _has_alter_foreign_key As Boolean = False
            Private _has_alter_default As Boolean = False
            Private _has_alter_alternate_key As Boolean = False
            Private _has_change As Boolean = False
            Private _has_illegal_attribute_name As Boolean = False
#End Region

#Region "Entity Properties"

            ''' <summary></summary>
            Protected Friend ReadOnly Property Entity As String
                Get
                    Return _entity
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property Description As String
                Get
                    Return _description
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property EntityType As EntityType
                Get
                    Return _entity_type
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HashLargeObjects As YesNoType
                Get
                    Return _hash_large_objects
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property NewEntityType As EntityType
                Get
                    Return _new_entity_type
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property CreateEntity As Boolean
                Get
                    Return _create_entity
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property DeleteEntity As Boolean
                Get
                    Return _delete_entity
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend Property ObjectID As Integer
                Get
                    Return If(IsNothing(_object_id), 0, _object_id)
                End Get
                Set(value As Integer)
                    _object_id = value
                End Set
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
            Protected Friend ReadOnly Property Domain As String
                Get
                    Return "[dbo].[" & Entity & "]"
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property Label As String
                Get
                    Return "dbo." & Entity
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property TableName As String
                Get
                    Return "[" & Entity & "]"
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HasChange As Boolean
                Get
                    Return _has_change
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HasBusinessKey As Boolean
                Get
                    Return _has_business_key
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HasAddColumn As Boolean
                Get
                    Return _has_add_column
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HasAlterAlternateKey As Boolean
                Get
                    Return _has_alter_alternate_key
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HasAlterColumn As Boolean
                Get
                    Return _has_alter_column
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HasAlterDefault As Boolean
                Get
                    Return _has_alter_default
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HasAlterForeignKey As Boolean
                Get
                    Return _has_alter_foreign_key
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HasDeleteColumn As Boolean
                Get
                    Return _has_delete_column
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property HasIllegalAttributeName As Boolean
                Get
                    Return _has_illegal_attribute_name
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property CreateEntityDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    If EntityType = ARA.Model.EntityType.TypeI Then
                        ReturnString = My.Resources.ARA_EntityDefinitionTypeI
                    Else
                        ReturnString = My.Resources.ARA_EntityDefinitionTypeII
                    End If

                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                    ReturnString = Replace(ReturnString, "{{{alternate key table column set}}}", TableColumnDefinition(UniqueAttributes, 6, False))
                    ReturnString = Replace(ReturnString, "{{{atomic table column set}}}", TableColumnDefinition(AtomicAttributes, 6, False))

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AlternateKeyDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    If EntityType = ARA.Model.EntityType.TypeI Then
                        ReturnString = My.Resources.ARA_CreateAlternateKeyDefinitionTypeI
                    Else
                        ReturnString = My.Resources.ARA_CreateAlternateKeyDefinitionTypeII
                    End If

                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                    ReturnString = Replace(ReturnString, "{{{alternate key index column set}}}", IndexColumnDefinition(UniqueAttributes, 3, True))
                    ReturnString = Replace(ReturnString, "{{{alternate key index column set 2}}}", IndexColumnDefinition(UniqueAttributes, 3, False))

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property DropAlternateKeyDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    ReturnString = My.Resources.ARA_DropAlternateKeyDefinition
                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity) & vbCrLf

                    For Each NewColumn In AtomicAttributes
                        ReturnString += My.Resources.ARA_DropAlternateKeyDefinitionTypeII & vbCrLf
                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{attribute}}}", NewColumn.Attribute)
                        ReturnString = Replace(ReturnString, "{{{column name}}}", NewColumn.ColumnName)
                    Next

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AddColumnDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    Dim NewColumns As EntityAttribute() = (From nc As EntityAttribute In _attribute
                                                           Where nc.BusinessIdentifier = YesNoType.Yes Or nc.AddColumn
                                                           Select nc).ToArray()

                    For Each NewColumn In NewColumns

                        If EntityType = ARA.Model.EntityType.TypeI Then
                            ReturnString += My.Resources.ARA_AddColumnDefinitionTypeI & vbCrLf
                        Else
                            ReturnString += My.Resources.ARA_AddColumnDefinitionTypeII & vbCrLf
                        End If

                        ReturnString = Replace(ReturnString, "{{{default clause}}}", If(NewColumn.DefaultValue = EmptyString, EmptyString, Spacer & My.Resources.ARA_ConstraintDefaultDefinitionOnAdd))
                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{attribute}}}", NewColumn.Attribute)
                        ReturnString = Replace(ReturnString, "{{{column name}}}", NewColumn.ColumnName)
                        ReturnString = Replace(ReturnString, "{{{name}}}", NewColumn.Attribute)
                        ReturnString = Replace(ReturnString, "{{{datatype}}}", NewColumn.Datatype)
                        ReturnString = Replace(ReturnString, "{{{optionality}}}", If(NewColumn.Optionality = YesNoType.No, NotNull, Null))
                        ReturnString = Replace(ReturnString, "{{{default value}}}", NewColumn.DefaultValue)

                    Next

                    NewColumns = (From nc As EntityAttribute In _attribute
                                  Where (nc.BusinessIdentifier = YesNoType.Yes Or nc.AddColumn) And nc.ReferencedEntity <> ""
                                  Select nc).ToArray()

                    ReturnString += ForeignKeyDefinition(NewColumns) & vbCrLf

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AlterColumnDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    Dim AlteredColumns As EntityAttribute() = (From nc As EntityAttribute In _attribute
                                                               Where Not nc.AddColumn And (nc.AlterColumn Or nc.AlterDefault Or nc.AlterForeignKey)
                                                               Select nc).ToArray() ' or alter index

                    ReturnString += DropColumnConstraints(AlteredColumns) & vbCrLf

                    AlteredColumns = (From nc As EntityAttribute In _attribute
                                      Where Not nc.AddColumn And nc.AlterColumn
                                      Select nc).ToArray()

                    For Each AlteredColumn In AlteredColumns

                        If EntityType = ARA.Model.EntityType.TypeI Then
                            ReturnString += My.Resources.ARA_AlterColumnDefinitionTypeI & vbCrLf
                        Else
                            ReturnString += My.Resources.ARA_AlterColumnDefinitionTypeII & vbCrLf
                        End If

                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{column name}}}", AlteredColumn.ColumnName)
                        ReturnString = Replace(ReturnString, "{{{attribute}}}", AlteredColumn.Attribute)
                        ReturnString = Replace(ReturnString, "{{{datatype}}}", AlteredColumn.Datatype)
                        ReturnString = Replace(ReturnString, "{{{default}}}", " ")
                        ReturnString = Replace(ReturnString, "{{{optionality}}}", If(AlteredColumn.Optionality = YesNoType.No, NotNull, Null))

                    Next

                    '
                    AlteredColumns = (From nc As EntityAttribute In _attribute
                                      Where Not nc.AddColumn And nc.AlterForeignKey And nc.ReferencedEntity <> EmptyString
                                      Select nc).ToArray()

                    ReturnString += ForeignKeyDefinition(AlteredColumns) & vbCrLf

                    '
                    AlteredColumns = (From nc As EntityAttribute In _attribute
                                      Where Not nc.AddColumn And nc.AlterDefault And nc.DefaultValue <> EmptyString
                                      Select nc).ToArray()

                    ReturnString += DefaultDefinition(AlteredColumns) & vbCrLf

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property DropColumnDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    Dim DeletedColumns As EntityAttribute() = (From nc As EntityAttribute In _attribute Where nc.DeleteColumn = True Select nc).ToArray()

                    ReturnString += DropColumnConstraints(DeletedColumns) & vbCrLf

                    For Each Column In DeletedColumns
                        ReturnString += My.Resources.ARA_DropColumnDefinition & vbCrLf
                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{column name}}}", Column.ColumnName)
                        ReturnString = Replace(ReturnString, "{{{attribute name}}}", Column.Attribute)
                    Next

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property DeleteEntityDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    ReturnString = My.Resources.ARA_DeleteEntityDefinition
                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlDropDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    ReturnString = My.Resources.ARA_ControlDropDefinition

                    ReturnString = My.Resources.ARA_ControlDropDefinition
                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlCreateDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    If EntityType = Model.EntityType.TypeI Then
                        ReturnString = My.Resources.ARA_ControlCreateDefinitionTypeI
                    Else
                        ReturnString = My.Resources.ARA_ControlCreateDefinitionTypeII
                    End If

                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                    ReturnString = Replace(ReturnString, "{{{column set}}}", ColumnListDefinition(Attributes, 3, True, True))

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlInsertDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    If EntityType = Model.EntityType.TypeI Then
                        ReturnString = My.Resources.ARA_ControlInsertDefinitionTypeI
                    Else
                        ReturnString = My.Resources.ARA_ControlInsertDefinitionTypeII
                    End If

                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                    ReturnString = Replace(ReturnString, "{{{column set}}}", ColumnListDefinition(Attributes, 6, False, True))
                    ReturnString = Replace(ReturnString, "{{{hash list}}}", ColumnListDefinition(AtomicAttributes, 0, False, False))

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlUpdateDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    If EntityType = Model.EntityType.TypeI Then
                        ReturnString = My.Resources.ARA_ControlUpdateDefinitionTypeI
                    Else
                        ReturnString = My.Resources.ARA_ControlUpdateDefinitionTypeII
                    End If

                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                    ReturnString = Replace(ReturnString, "{{{column set}}}", ColumnListDefinition(Attributes, 9, False, True))
                    ReturnString = Replace(ReturnString, "{{{hash set}}}", ColumnListDefinition(AtomicAttributes, 0, False, False))
                    ReturnString = Replace(ReturnString, "{{{join set}}}", ColumnJoinDefinition(UniqueAttributes, 23, True))
                    ReturnString = Replace(ReturnString, "{{{set set}}}", ColumnSetDefinition(AtomicAttributes, 6, True))
                    ReturnString = Replace(ReturnString, "{{{key update check}}}", KeyUpdateCheckDefinition(UniqueAttributes))

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ControlDeleteDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    If EntityType = Model.EntityType.TypeI Then
                        ReturnString = My.Resources.ARA_ControlDeleteDefinitionTypeI
                    Else
                        ReturnString = My.Resources.ARA_ControlDeleteDefinitionTypeII
                    End If

                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                    ReturnString = Replace(ReturnString, "{{{column set}}}", ColumnListDefinition(UniqueAttributes, 9, False, True))
                    ReturnString = Replace(ReturnString, "{{{join set}}}", ColumnJoinDefinition(UniqueAttributes, 23, True))

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property DropColumnConstraints(ColumnSet As EntityAttribute()) As String
                Get
                    Dim ReturnString As String = "declare @drop_constraints nvarchar(max);" & vbCrLf

                    For Each Column In ColumnSet
                        ReturnString += My.Resources.ARA_DropColumnConstraints & vbCrLf
                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{attribute name}}}", Column.Attribute)
                    Next

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ForeignKeyDefinition(ByVal ColumnSet As EntityAttribute()) As String
                Get
                    Dim ReturnString As String = EmptyString

                    For Each Column In ColumnSet
                        ReturnString += My.Resources.ARA_ConstraintForeignKeyDefinition
                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{referenced entity}}}", Column.ReferencedEntity)
                        ReturnString = Replace(ReturnString, "{{{referenced entity type}}}", If(Column.ReferencedEntityType = ARA.Model.EntityType.TypeI, EmptyString, ".Keystore"))
                        ReturnString = Replace(ReturnString, "{{{role}}}", If(Column.ReferencedRole = EmptyString, EmptyString, " $$ " & Column.ReferencedRole))
                        ReturnString = Replace(ReturnString, "{{{column name}}}", Column.Attribute)
                        ReturnString += vbCrLf & vbCrLf
                    Next Column

                    Return If(ReturnString = EmptyString, "--> no foreign keys", ReturnString)
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property DefaultDefinition(ByVal ColumnSet As EntityAttribute()) As String
                Get
                    Dim ReturnString As String = EmptyString

                    For Each Column In ColumnSet
                        ReturnString += My.Resources.ARA_ConstraintDefaultDefinition
                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{column name}}}", Column.Attribute)
                        ReturnString = Replace(ReturnString, "{{{default value}}}", Column.DefaultValue)
                        ReturnString += vbCrLf & vbCrLf
                    Next Column

                    Return If(ReturnString = EmptyString, "--> no defaults", ReturnString)
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property Attribute(ByVal AttributeNumber As Integer) As EntityAttribute
                Get
                    Return _attribute(AttributeNumber)
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property Attribute(ByVal AttributeName As String) As EntityAttribute
                Get
                    If IsNothing(_attribute) Then
                        Return Nothing
                    End If

                    Return (From a As EntityAttribute In _attribute Where a.Attribute = AttributeName Select a).ToArray.First
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AllAttributes As EntityAttribute()
                Get
                    If IsNothing(_attribute) Then
                        Return Nothing
                    End If

                    Return (From aa As EntityAttribute In _attribute Order By aa.Ordinal Select aa).ToArray
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property Attributes As EntityAttribute()
                Get
                    If IsNothing(_attribute) Then
                        Return Nothing
                    End If

                    Return (From a As EntityAttribute In _attribute Where Not a.DeleteColumn Order By a.Ordinal Select a).ToArray
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property UniqueAttributes As EntityAttribute()
                Get
                    If IsNothing(_attribute) Then
                        Return Nothing
                    ElseIf Not _has_business_key Then
                        Return Nothing
                    End If

                    Return (From ua As EntityAttribute In _attribute Where ua.BusinessIdentifier = YesNoType.Yes And Not ua.DeleteColumn Order By ua.Ordinal Select ua).ToArray
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AtomicAttributes As EntityAttribute()
                Get
                    If IsNothing(_attribute) Then
                        Return Nothing
                    End If

                    Return (From aa As EntityAttribute In _attribute Where aa.BusinessIdentifier = YesNoType.No And Not aa.DeleteColumn Order By aa.Ordinal Select aa).ToArray
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property ForeignKeyAttributes As EntityAttribute()
                Get
                    If IsNothing(_attribute) Then
                        Return Nothing
                    End If

                    Return (From fka As EntityAttribute In _attribute Where fka.ReferencedEntity <> EmptyString Order By fka.Ordinal Select fka).ToArray
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property DefaultValuedAttributes As EntityAttribute()
                Get
                    If IsNothing(_attribute) Then
                        Return Nothing
                    End If

                    Return (From dva As EntityAttribute In _attribute Where dva.DefaultValue <> EmptyString Order By dva.Ordinal Select dva).ToArray
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property DeletedAttributes As EntityAttribute()
                Get
                    If IsNothing(_attribute) Then
                        Return Nothing
                    End If

                    Return (From a As EntityAttribute In _attribute Where a.DeleteColumn Select a).ToArray
                End Get
            End Property

            ''' <summary></summary>
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
                    ep = Replace(ep, "{{{value}}}", Replace(Description, "'", "''"))
                    ep += vbCrLf

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Use Large Object Hashing Algorithm")
                    ep = Replace(ep, "{{{value}}}", HashLargeObjects.ToString)
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
                    ep = Replace(ep, "{{{value}}}", Now.ToString())
                    ep += vbCrLf

                    Return ep
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AttributeMetadataDefintion As String
                Get
                    Dim ea As EntityAttribute = Nothing
                    Dim ep As String = EmptyString

                    For Each ea In Attributes

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Domain")
                        ep = Replace(ep, "{{{value}}}", Domain)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Description")
                        ep = Replace(ep, "{{{value}}}", Replace(ea.Description, "'", "''"))
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Sort Order")
                        ep = Replace(ep, "{{{value}}}", ea.SortOrder.ToString)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Ordinal")
                        ep = Replace(ep, "{{{value}}}", ea.Ordinal)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Optionality")
                        ep = Replace(ep, "{{{value}}}", ea.Optionality.ToString)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Datatype")
                        ep = Replace(ep, "{{{value}}}", ea.Datatype)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Business Identifier")
                        ep = Replace(ep, "{{{value}}}", ea.BusinessIdentifier.ToString)
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

                    Next

                    Return ep
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property TableColumnDefinition(ByVal ColumnSet As EntityAttribute(), _
                                                            Optional ByVal Padding As Integer = 0, _
                                                            Optional ByVal RemoveTrailingComma As Boolean = True) As String
                Get
                    Dim ReturnString As String = EmptyString

                    For Each Column In ColumnSet
                        ReturnString += New String(Spacer, Padding) & Column.ColumnName & Spacer & Column.Datatype & Spacer & _
                                        If(Column.Optionality = YesNoType.Yes, "null,", "not null,") & vbCrLf
                    Next Column

                    If ReturnString = EmptyString Then
                        ReturnString = New String(Spacer, Padding) & "--> no attributes defined."
                    ElseIf RemoveTrailingComma Then
                        ReturnString = Left(ReturnString, Len(ReturnString) - 3)
                    Else
                        ReturnString = Left(ReturnString, Len(ReturnString) - 2)
                    End If

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property IndexColumnDefinition(ByVal ColumnSet As EntityAttribute(), _
                                                            Optional ByVal Padding As Integer = 0, _
                                                            Optional ByVal RemoveTrailingComma As Boolean = True) As String
                Get
                    Dim ReturnString As String = EmptyString
                    For Each Column In ColumnSet
                        ReturnString += New String(Spacer, Padding) & Column.ColumnName & Spacer & _
                                        Column.SortOrder.ToString & "," & vbCrLf
                    Next

                    If ReturnString = EmptyString Then
                        ReturnString = New String(Spacer, Padding) & "--> no attributes defined."
                    ElseIf RemoveTrailingComma Then
                        ReturnString = Left(ReturnString, Len(ReturnString) - 3)
                    Else
                        ReturnString = Left(ReturnString, Len(ReturnString) - 2)
                    End If

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property ColumnListDefinition(ByVal ColumnSet As EntityAttribute(), _
                                                           Optional ByVal Padding As Integer = 0, _
                                                           Optional ByVal RemoveTrailingComma As Boolean = True,
                                                           Optional ByVal IncludeCrLf As Boolean = True) As String
                Get
                    Dim ReturnString As String = EmptyString

                    For Each Column In ColumnSet
                        ReturnString += New String(Spacer, Padding) & Column.ColumnName & "," & If(IncludeCrLf, vbCrLf, "")
                    Next Column

                    If RemoveTrailingComma Then
                        ReturnString = Left(ReturnString, Len(ReturnString) - If(IncludeCrLf, 3, 0))
                    Else
                        ReturnString = Left(ReturnString, Len(ReturnString) - If(IncludeCrLf, 2, 0))
                    End If

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property ColumnJoinDefinition(ByVal ColumnSet As EntityAttribute(), _
                                                           Optional ByVal Padding As Integer = 0, _
                                                           Optional ByVal IncludeCrLf As Boolean = True) As String
                Get
                    Dim ReturnString As String = EmptyString

                    For Each Column In ColumnSet
                        ReturnString += New String(Spacer, Padding) & "and t." & Column.ColumnName & "=s." & Column.ColumnName & If(IncludeCrLf, vbCrLf, "")
                    Next Column

                    ReturnString = Left(ReturnString, Len(ReturnString) - If(IncludeCrLf, 2, 0))

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property ColumnSetDefinition(ByVal ColumnSet As EntityAttribute(), _
                                                           Optional ByVal Padding As Integer = 0, _
                                                           Optional ByVal IncludeCrLf As Boolean = True) As String
                Get
                    Dim ReturnString As String = EmptyString

                    For Each Column In ColumnSet
                        ReturnString += New String(Spacer, Padding) & Column.ColumnName & "=s." & Column.ColumnName & "," & If(IncludeCrLf, vbCrLf, "")
                    Next Column

                    ReturnString = Left(ReturnString, Len(ReturnString) - If(IncludeCrLf, 2, 0))

                    Return ReturnString
                End Get
            End Property

            ''' <summary></summary>
            Private ReadOnly Property KeyUpdateCheckDefinition(ByVal ColumnSet As EntityAttribute()) As String
                Get
                    Dim ReturnString As String = EmptyString

                    For Each Column In ColumnSet
                        ReturnString += My.Resources.ARA_ControlUpdateInvalidation & vbCrLf & vbCrLf
                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{column name}}}", Column.ColumnName)
                    Next Column

                    ReturnString = Left(ReturnString, Len(ReturnString) - 2)

                    Return ReturnString
                End Get
            End Property

#End Region

#Region "Entity Constructors"

            Protected Friend Sub New(ByVal Entity As String,
                                     ByVal Description As String,
                                     ByVal EntityType As EntityType,
                                     ByVal HashLargeObjects As YesNoType,
                                     ByVal NewEntityType As EntityType,
                                     ByVal EntityCreate As Boolean,
                                     ByVal EntityDelete As Boolean,
                                     ByRef Invalidations As String(),
                                     ByRef Warnings As String())

                _entity = Entity
                _description = Description
                _entity_type = EntityType
                _hash_large_objects = HashLargeObjects
                _new_entity_type = NewEntityType
                _create_entity = EntityCreate
                _delete_entity = EntityDelete

                ' exist type versus new type warning
                ' warn about about backing up data on delete

            End Sub

            Protected Friend Sub NewAttribute(ByVal Attribute As String,
                                              ByVal ReferencedEntity As String,
                                              ByVal ReferencedEntityType As EntityType,
                                              ByVal Datatype As String,
                                              ByVal DefaultValue As String,
                                              ByVal Ordinal As Integer,
                                              ByVal SortOrder As Model.SortOrderType,
                                              ByVal Optionality As YesNoType,
                                              ByVal BusinessIdentifier As YesNoType,
                                              ByVal Description As String,
                                              ByVal Distribution As Model.StatiticalDistribution,
                                              ByVal AddColumn As Boolean,
                                              ByVal DeleteColumn As Boolean,
                                              ByVal AlterColumn As Boolean,
                                              ByVal AlterForeignKey As Boolean,
                                              ByVal AlterDefault As Boolean,
                                              ByVal AlterAlternateKey As Boolean,
                                              ByRef Invalidations As String(),
                                              ByRef Warnings As String())

                Try
                    ' check the attribute for entity changes
                    If BusinessIdentifier = YesNoType.Yes Then _has_business_key = True
                    If AddColumn = True Then _has_add_column = True : _has_change = True
                    If DeleteColumn = True Then _has_delete_column = True : _has_change = True
                    If AlterColumn = True Then _has_alter_column = True : _has_change = True
                    If AlterForeignKey = True Then _has_alter_foreign_key = True : _has_change = True
                    If AlterDefault = True Then _has_alter_default = True : _has_change = True
                    If AlterAlternateKey = True Then _has_alter_alternate_key = True : _has_change = True

                    If Left(Attribute, 4) = "ara_" Then
                        Invalidations.AddMember("Entity attribute [" & Entity & "].[" & Attribute & "] attribute cannot have a prefix of 'ara_'.")
                    End If

                    If Attribute = (Entity & "Key") Then
                        Invalidations.AddMember("Entity [" & Entity & "] cannot have an attribute named [" & Entity & "Key].")
                    End If

                    If ((AddColumn Or AlterColumn) And DefaultValue = "" And Optionality = YesNoType.No) And Not _create_entity Then
                        Warnings.AddMember("Adding or altering entity attribute [" & Entity & "].[" & Attribute & "] without optionality may require a default value.")
                    End If

                    If AlterAlternateKey And Not CreateEntity Then
                        Warnings.AddMember("Altering via deletion of a column on entity [" & Entity & "] may cause unique constraint errors.")
                    End If

                    ' add operand clash warnings

                    ' add the attribute to the entity
                    _attribute.AddAttributeToEntity(New EntityAttribute(Attribute, _
                                                                        ReferencedEntity, _
                                                                        ReferencedEntityType, _
                                                                        Datatype, _
                                                                        DefaultValue,
                                                                        Ordinal, _
                                                                        SortOrder,
                                                                        Optionality, _
                                                                        BusinessIdentifier,
                                                                        Description, _
                                                                        Distribution, _
                                                                        AddColumn, _
                                                                        DeleteColumn, _
                                                                        AlterColumn, _
                                                                        AlterForeignKey, _
                                                                        AlterDefault, _
                                                                        AlterAlternateKey))

                Catch ex As Exception
                    PrintClientMessage(ex.Message)
                End Try

            End Sub

#End Region

#Region "Entity Methods"

#End Region

            Protected Friend Class EntityAttribute

#Region "Attribute Variables"
                Private _attribute As String
                Private _referenced_entity As String
                Private _referenced_entity_type As EntityType
                Private _datatype As String
                Private _default_value As String
                Private _ordinal As Integer
                Private _sort_order As SortOrderType
                Private _optionality As YesNoType
                Private _business_identifier As YesNoType
                Private _description As String
                Private _statistical_distribution As StatiticalDistribution
                Private _add_column As Boolean
                Private _delete_column As Boolean
                Private _alter_column As Boolean
                Private _alter_foreign_key As Boolean
                Private _alter_default As Boolean
                Private _alter_alternate_key As Boolean
#End Region

#Region "Attribute Properties"

                Protected Friend ReadOnly Property Attribute As String
                    Get
                        Return _attribute
                    End Get
                End Property

                Protected Friend ReadOnly Property ColumnName As String
                    Get
                        Return "[" & Replace(Replace(_attribute, "]", ""), "[", "") & "]"
                    End Get
                End Property

                Protected Friend ReadOnly Property ReferencedEntity As String
                    Get
                        Return _referenced_entity
                    End Get
                End Property

                Protected Friend ReadOnly Property ReferencedEntityType As EntityType
                    Get
                        Return _referenced_entity_type
                    End Get
                End Property

                Protected Friend ReadOnly Property ReferencedRole As String
                    Get
                        Return Replace(_attribute, _referenced_entity & "Key", "")
                    End Get
                End Property

                Protected Friend ReadOnly Property Datatype As String
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
                End Property

                Protected Friend ReadOnly Property DefaultValue As String
                    Get
                        Return _default_value
                    End Get
                End Property

                Protected Friend ReadOnly Property Ordinal As Integer
                    Get
                        Return _ordinal
                    End Get
                End Property

                Protected Friend ReadOnly Property SortOrder As SortOrderType
                    Get
                        Return _sort_order
                    End Get
                End Property

                Protected Friend ReadOnly Property Optionality As YesNoType
                    Get
                        Return _optionality
                    End Get
                End Property

                Protected Friend ReadOnly Property BusinessIdentifier As YesNoType
                    Get
                        Return _business_identifier
                    End Get
                End Property

                Protected Friend ReadOnly Property Description As String
                    Get
                        Return _description
                    End Get
                End Property

                Protected Friend ReadOnly Property Distribution As StatiticalDistribution
                    Get
                        Return _statistical_distribution
                    End Get
                End Property

                Protected Friend ReadOnly Property AddColumn As Boolean
                    Get
                        Return _add_column
                    End Get
                End Property

                Protected Friend ReadOnly Property DeleteColumn As Boolean
                    Get
                        Return _delete_column
                    End Get
                End Property

                Protected Friend ReadOnly Property AlterColumn As Boolean
                    Get
                        Return _alter_column
                    End Get
                End Property

                Protected Friend ReadOnly Property AlterForeignKey As Boolean
                    Get
                        Return _alter_foreign_key
                    End Get
                End Property

                Protected Friend ReadOnly Property AlterDefault As Boolean
                    Get
                        Return _alter_default
                    End Get
                End Property

                Protected Friend ReadOnly Property AlterAlternateKey As Boolean
                    Get
                        Return _alter_alternate_key
                    End Get
                End Property

#End Region

#Region "Attribute Contructors"

                Protected Friend Sub New(ByVal Name As String, _
                                         ByVal ReferencedEntity As String, _
                                         ByVal ReferencedEntityType As EntityType, _
                                         ByVal Datatype As String, _
                                         ByVal DefaultValue As String, _
                                         ByVal Ordinal As Integer,
                                         ByVal SortOrder As Model.SortOrderType, _
                                         ByVal Optionality As YesNoType, _
                                         ByVal BusinessIdentifier As YesNoType, _
                                         ByVal Description As String, _
                                         ByVal Distribution As StatiticalDistribution, _
                                         ByVal AddColumn As Boolean, _
                                         ByVal DeleteColumn As Boolean, _
                                         ByVal AlterColumn As Boolean, _
                                         ByVal AlterForeignKey As Boolean, _
                                         ByVal AlterDefault As Boolean, _
                                         ByVal AlterAlternateKey As Boolean)

                    _attribute = Name
                    _referenced_entity = ReferencedEntity
                    _referenced_entity_type = ReferencedEntityType
                    _datatype = Datatype
                    _default_value = DefaultValue
                    _ordinal = Ordinal
                    _sort_order = SortOrder
                    _optionality = Optionality
                    _business_identifier = BusinessIdentifier
                    _description = Description
                    _statistical_distribution = Distribution
                    _add_column = AddColumn
                    _delete_column = DeleteColumn
                    _alter_column = AlterColumn
                    _alter_foreign_key = AlterForeignKey
                    _alter_default = AlterDefault
                    _alter_alternate_key = AlterAlternateKey

                End Sub

#End Region

            End Class ' EntityAttribute

        End Class ' Entity

    End Class ' Construct

End Class
