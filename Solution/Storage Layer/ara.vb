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

    ''' <summary>...</summary>
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

    ''' <summary>...</summary>
    <Extension()> _
    Friend Function StringToEntityType(ByVal InputString As String) As ARA.Model.EntityType
        Return If(InputString = "2", ARA.Model.EntityType.TypeII, If(InputString = "1", ARA.Model.EntityType.TypeI, Nothing))
    End Function

    ''' <summary>...</summary>
    ''' <param name="InputString">Description of the first parameter</param>
    ''' <returns>Description for what the function returns</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Function StringToYesNoType(ByVal InputString As String) As ARA.Model.YesNoType
        Return If(InputString = "Yes", ARA.Model.YesNoType.Yes, ARA.Model.YesNoType.No)
    End Function

    ''' <summary>...</summary>
    ''' <param name="InputString">Description of the first parameter</param>
    ''' <returns>Description for what the function returns</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Function RemoveNulls(ByVal InputString As String) As String
        Return If(IsDBNull(InputString), "", InputString)
    End Function

    ''' <summary>...</summary>
    ''' <param name="InputString">Description of the first parameter</param>
    ''' <returns>Description for what the function returns</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Function StringToBool(ByVal InputString As String) As Boolean
        Return CBool(InputString)
    End Function

    ''' <summary>...</summary>
    ''' <param name="InputString">Description of the first parameter</param>
    ''' <returns>Description for what the function returns</returns>
    ''' <remarks></remarks>
    <Extension()> _
    Friend Function StringToSortOrderType(ByVal InputString As String) As ARA.Model.SortOrderType
        Return If(InputString = "desc", ARA.Model.SortOrderType.desc, ARA.Model.SortOrderType.asc)
    End Function

End Module

''' <summary></summary>
''' <remarks></remarks>
Public Class ARA

#Region "CLR Exposed Methods"

    ''' <summary>...</summary>
    ''' <param name="DatabaseName"></param>
    ''' <param name="VerifyOnly"></param> 
    ''' <param name="ProcessAbstractsInFull"></param>
    ''' <remarks>...</remarks>
    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub BuildEntities(ByVal DatabaseName As SqlString,
                                    ByVal VerifyOnly As SqlBoolean,
                                    ByVal ProcessAbstractsInFull As SqlBoolean)

        Dim SqlCnn As New SqlConnection("context connection=true")
        SqlCnn.Open()

        Try
            PrintHeader()

            If Not UserIsSysAdmin(SqlCnn) Then Exit Try

            ProcessCallToBuild(SqlCnn, CStr(DatabaseName), CBool(VerifyOnly), CBool(ProcessAbstractsInFull))

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
        End Try

        SqlCnn.Close()

    End Sub

    ''' <summary>...</summary>
    ''' <remarks>...</remarks>
    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub GetModel()

        Dim SqlCnn As New SqlConnection("context connection=true")
        SqlCnn.Open()

        Try
            PrintHeader()

            If Not UserIsSysAdmin(SqlCnn) Then Exit Try

            Dim model_query As String = My.Resources.ARA_ModelGet
            ExecuteDDLCommand("set nocount on;", SqlCnn)
            ReturnClientResults(SqlCnn, model_query)
            PrintClientMessage("The model has been successfully exported.")

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
        End Try

        SqlCnn.Close()

    End Sub

    ''' <summary>...</summary>
    ''' <param name="NewModel"></param>
    ''' <remarks>...</remarks>
    <Microsoft.SqlServer.Server.SqlProcedure> _
    Public Shared Sub SetModel(ByVal NewModel As SqlXml)

        Dim SqlCnn As New SqlConnection("context connection=true")
        SqlCnn.Open()

        Try
            PrintHeader()

            If Not UserIsSysAdmin(SqlCnn) Then Exit Try

            If IsNothing(NewModel) Then PrintClientMessage("You cannot process a null model.") : Exit Try

            Dim model_query As String = Replace(My.Resources.ARA_ModelSet, "{{{xml}}}", NewModel.Value.ToString)
            ExecuteDDLCommand(model_query, SqlCnn)
            PrintClientMessage("The model has been successfully imported.")

        Catch ex As Exception
            PrintClientError(New StackFrame().GetMethod().Name, ex)
        End Try

        SqlCnn.Close()

    End Sub



#End Region

#Region "ARA Methods"

    ''' <summary>Execute the call to build the model from the specified metadata.</summary>
    ''' <param name="SqlCnn"></param>
    ''' <param name="DatabaseName">The name of the database name on the instance that the model will deployed on.</param>
    ''' <param name="VerifyOnly">If set to 'true', nothing will be built, but the process will be verify against the database.</param>
    ''' <remarks>...</remarks>
    Private Shared Sub ProcessCallToBuild(ByVal SqlCnn As SqlConnection,
                                          ByVal DatabaseName As String,
                                          ByVal VerifyOnly As Boolean,
                                          ByVal ProcessAbstractsInFull As Boolean)

        ' see if metadata tables are ready in master database
        If Not SystemObjectsInstalled(SqlCnn) Then Exit Sub

        ' validate incoming database
        If Not DatabaseIsValid(DatabaseName, SqlCnn) Then Exit Sub

        ' make sure server objects are in place
        AddInstanceObjects(SqlCnn)

        ' make sure database objects are in place
        AddDatabaseObjects(SqlCnn, DatabaseName)

        ' load up a variable with the usable construct
        Dim ModelToProcess As New Model(GetMetadata(DatabaseName, SqlCnn))

        ' notify of invalidations to the model
        PrintClientMessage("• Determining model validation: ")
        If ModelToProcess.ModelInvalidations IsNot Nothing Then
            PrintClientMessage("  -> Data model is INVALID ")
            For i = 0 To ModelToProcess.ModelInvalidations.Length - 1
                PrintClientMessage("-> " & ModelToProcess.ModelInvalidations(i), 2)
            Next
        Else
            PrintClientMessage("  -> Data model is VALID ")
        End If

        ' notify of warnings to the model
        PrintClientMessage("• Identifying non-fatal issues: ")
        If ModelToProcess.ModelWarnings IsNot Nothing Then
            For i = 0 To ModelToProcess.ModelWarnings.Length - 1
                PrintClientMessage("-> " & ModelToProcess.ModelWarnings(i), 2)
            Next
        Else
            PrintClientMessage("  -> Data model did NOT produce any WARNINGS")
        End If

        ' if there are known invalidations, bail
        If ModelToProcess.ModelInvalidations IsNot Nothing Then Exit Sub

        ' with we have records, we can begin with the DDL process, otherwise, alert client and exit
        BuildValidModel(ModelToProcess, SqlCnn, VerifyOnly, ProcessAbstractsInFull)

    End Sub

    ''' <summary>...</summary>
    ''' <param name="ModelToProcess"></param>
    ''' <param name="DatabaseConnection"></param>
    ''' <param name="VerifyOnly"></param>
    ''' <remarks>...</remarks>
    Private Shared Sub BuildValidModel(ByVal ModelToProcess As Model,
                                       ByVal DatabaseConnection As SqlConnection,
                                       ByVal VerifyOnly As Boolean,
                                       ByVal ProcessAbstractsInFull As Boolean)

        Const space As String = " "

        Dim Entity As Model.Entity = Nothing
        Dim Entities As Model.Entity() = Nothing
        Dim EntityAttributes As Model.Entity.EntityAttribute() = Nothing

        Dim StartTime As Date = Now()
        Dim fmt As String = "yyyy-MM-dd HH:mm:ss.ff..."

        PrintClientMessage("• Database Name: " & ModelToProcess.DatabaseName)
        PrintClientMessage("• Database Compatibility: " & ModelToProcess.DatabaseCompatibility.ToString)

        '
        PrintClientMessage(vbCrLf)
        PrintEstimatedChanges(ModelToProcess)

        '
        PrintClientMessage(vbCrLf)
        PrintClientMessage("• Process construct started at " & StartTime.ToString(fmt))

        '
        If VerifyOnly Then ExecuteDDLCommand("set parseonly on;", DatabaseConnection)

        ' if validated and not verify
        Try
            ExecuteDDLCommand("begin transaction;", DatabaseConnection)

            ' put security into place, irrespective
            PrintClientMessage(vbCrLf)
            PrintClientMessage("  -> Enforcing Schemas and Roles")
            ExecuteDDLCommand(ModelToProcess.SecurityDefinition, DatabaseConnection)

            ' create new entities
            ProcessAddEntity(ModelToProcess, DatabaseConnection)

            ' alter existing entites
            ProcessAlterEntity(ModelToProcess, DatabaseConnection)

            ' drop entites
            ProcessDropEntity(ModelToProcess, DatabaseConnection)

            ' 
            'If ProcessAbstractsInFull And ModelToProcess.EntityCount > 0 Then

            '    Entities = ModelToProcess.Entities

            '    For Each Entity In Entities
            '        ProcessAbstractDrop(Entity, DatabaseConnection)
            '        ProcessAbstractCreate(Entity, DatabaseConnection)
            '    Next

            'End If

            ' if we made it here without error, commit transactions
            ExecuteDDLCommand("commit transaction;", DatabaseConnection)

        Catch ex As Exception
            ExecuteDDLCommand("rollback transaction;", DatabaseConnection)

            PrintClientMessage(space)
            PrintClientMessage("An ERROR occured with with entity [" & Entity.Entity & "]:", 3)
            PrintClientMessage(ex.Message.ToString, 3)
            PrintClientMessage("The transaction has been rolled back and logged.", 3)
            PrintClientMessage(vbCrLf)
        End Try

        Dim ed As Date = Now()
        PrintClientMessage(vbCrLf & "• Process ended at " & ed.ToString(fmt))

        Dim min As Integer = DateDiff(DateInterval.Minute, StartTime, ed)
        Dim sec As Integer = DateDiff(DateInterval.Second, StartTime, ed) Mod 60
        PrintClientMessage("• Time to execute " & min.ToString & " min(s) " & sec.ToString & " sec(s)")

    End Sub

    ''' <summary>...</summary>
    Private Shared Sub PrintHeader()

        PrintClientMessage(My.Resources.SYS_SlalomTextArt1 & vbCrLf)
        PrintClientMessage("EDW Framework - Analytics & Reporting Area ('ARA') Definition")
        PrintClientMessage("Slalom Consulting | Copyright © 2015 | www.slalom.com" & vbCrLf & vbCrLf)

    End Sub

    ''' <summary>...</summary>
    Private Shared Sub PrintEstimatedChanges(ByVal ModelToProcess As Model)

        Dim EntityAttributes As Model.Entity.EntityAttribute()
        Dim Space As String = " "
        Dim Dot As String = "."
        Dim Arrow As String = "."
        Dim DoubleArrow As String = "->>"

        PrintClientMessage("• Estimated changes to data model: ")

        For Each Entity In ModelToProcess.Entities

            EntityAttributes = Entity.AllAttributes

            If Entity.CreateEntity Then
                PrintClientMessage(New String(Space, 2) & "-> " & Entity.TableName.ToString & New String(Dot, 36 - Entity.TableName.ToString.Length) & "...[CREATE]")

            ElseIf Entity.DeleteEntity Then
                PrintClientMessage(New String(Space, 2) & "-> " & Entity.TableName.ToString & New String(Dot, 36 - Entity.TableName.ToString.Length) & ".....[DROP]")

            ElseIf Not Entity.HasChange Then
                PrintClientMessage(New String(Space, 2) & "-> " & Entity.TableName.ToString & New String(Dot, 36 - Entity.TableName.ToString.Length) & "[NO CHANGE]")

            ElseIf EntityAttributes IsNot Nothing Then
                PrintClientMessage(New String(Space, 2) & "-> " & Entity.TableName.ToString & New String(Dot, 36 - Entity.TableName.ToString.Length) & "....[ALTER]")

                For Each att In EntityAttributes
                    If att.AlterAlternateKey Then PrintClientMessage(New String(Space, 5) & DoubleArrow & Space & att.PrintableColumnName.ToString & New String(Dot, 34 - att.PrintableColumnName.ToString.Length) & ".[UNIQUE]")
                    If att.AddColumn Then PrintClientMessage(New String(Space, 5) & DoubleArrow & Space & att.PrintableColumnName.ToString & New String(Dot, 34 - att.PrintableColumnName.ToString.Length) & "....[ADD]")
                    If att.DeleteColumn Then PrintClientMessage(New String(Space, 5) & DoubleArrow & Space & att.PrintableColumnName.ToString & New String(Dot, 34 - att.PrintableColumnName.ToString.Length) & "...[DROP]")
                    If att.AlterColumn Then PrintClientMessage(New String(Space, 5) & DoubleArrow & Space & att.PrintableColumnName.ToString & New String(Dot, 34 - att.PrintableColumnName.ToString.Length) & "..[ALTER]")
                    If att.AlterDefault Then PrintClientMessage(New String(Space, 5) & DoubleArrow & Space & att.PrintableColumnName.ToString & New String(Dot, 34 - att.PrintableColumnName.ToString.Length) & "[DEFAULT]")
                    If att.AlterForeignKey Then PrintClientMessage(New String(Space, 5) & DoubleArrow & Space & att.PrintableColumnName.ToString & New String(Dot, 34 - att.PrintableColumnName.ToString.Length) & "....[REF]")
                Next att

            End If

            If Entity.Entity = "Bank" Then
                'For Each x In Entity.Abstracts
                '    PrintClientMessage(x.SecurityGroup)

                '    PrintClientMessage(x.Abstract)

                '    For Each y In x.AbstractColumns
                '        PrintClientMessage(y.Attribute & " | " & y.ColumnName)
                '    Next

                'Next

            End If
        Next

    End Sub

    ''' <summary>...</summary>
    Private Shared Sub ProcessAbstractDrop(ByVal ProcessEntity As Model.Entity,
                                           ByVal DatabaseConnection As SqlConnection)

        ExecuteDDLCommand(ProcessEntity.ControlDropDefinition, DatabaseConnection)
        ExecuteDDLCommand(ProcessEntity.AbstractDropDefinition, DatabaseConnection)

    End Sub

    ''' <summary>...</summary>
    Private Shared Sub ProcessAbstractCreate(ByVal ProcessEntity As Model.Entity,
                                             ByVal DatabaseConnection As SqlConnection)

        ExecuteDDLCommand(ProcessEntity.ControlCreateDefinition, DatabaseConnection)
        ExecuteDDLCommand(ProcessEntity.ControlInsertDefinition, DatabaseConnection)
        ExecuteDDLCommand(ProcessEntity.ControlUpdateDefinition, DatabaseConnection)
        ExecuteDDLCommand(ProcessEntity.ControlDeleteDefinition, DatabaseConnection)
        ExecuteDDLCommand(ProcessEntity.ControlSecurityDefinition, DatabaseConnection)
        ExecuteDDLCommand(ProcessEntity.AbstractCreateDefinition, DatabaseConnection)

    End Sub

    ''' <summary>...</summary>
    Private Shared Sub ProcessEntityMetadata(ByVal ProcessEntity As Model.Entity,
                                             ByVal DatabaseConnection As SqlConnection)

        ExecuteDDLCommand(ProcessEntity.EntityMetadataDefinition, DatabaseConnection)
        ExecuteDDLCommand(ProcessEntity.AttributeMetadataDefintion, DatabaseConnection)

    End Sub

    ''' <summary>...</summary>
    Private Shared Sub ProcessAddEntity(ByVal ModelToProcess As Model, ByVal DatabaseConnection As SqlConnection)

        Dim Entity As Model.Entity = Nothing
        Dim Entities As Model.Entity() = Nothing

        PrintClientMessage("  -> Adding New Entities")

        Entities = ModelToProcess.CreatedEntities

        For Each Entity In Entities
            ExecuteDDLCommand(Entity.CreateEntityDefinition, DatabaseConnection)
        Next Entity

        For Each Entity In Entities

            ExecuteDDLCommand(Entity.AlternateKeyDefinition, DatabaseConnection)

            'constraint definition
            ExecuteDDLCommand(Entity.ForeignKeyDefinition(Entity.ForeignKeyAttributes), DatabaseConnection)
            ExecuteDDLCommand(Entity.DefaultDefinition(Entity.DefaultValuedAttributes), DatabaseConnection)

            ' control creation
            ProcessAbstractCreate(Entity, DatabaseConnection)

            ' sync metadata
            ProcessEntityMetadata(Entity, DatabaseConnection)

        Next Entity

    End Sub

    ''' <summary>...</summary>
    Private Shared Sub ProcessAlterEntity(ByVal ModelToProcess As Model, ByVal DatabaseConnection As SqlConnection)

        Dim Entity As Model.Entity = Nothing
        Dim Entities As Model.Entity() = Nothing

        PrintClientMessage("  -> Altering Existing Entities")
        Entities = ModelToProcess.ChangedEntities

        For Each Entity In Entities

            ProcessAbstractDrop(Entity, DatabaseConnection)

            ' if the business identifier has changed, drop it, in all forms
            If Entity.HasAlterAlternateKey Then
                ExecuteDDLCommand(Entity.DropAlternateKeyDefinition, DatabaseConnection)
            End If

            ' add and alter any potential columns
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
            ProcessAbstractCreate(Entity, DatabaseConnection)

            ' sync metadata
            ProcessEntityMetadata(Entity, DatabaseConnection)

        Next Entity

    End Sub

    ''' <summary>...</summary>
    Private Shared Sub ProcessDropEntity(ByVal ModelToProcess As Model, ByVal DatabaseConnection As SqlConnection)

        Dim Entity As Model.Entity = Nothing
        Dim Entities As Model.Entity() = Nothing

        PrintClientMessage("  -> Dropping Old Entities")
        Entities = ModelToProcess.DeletedEntities

        For Each Entity In Entities
            ProcessAbstractDrop(Entity, DatabaseConnection)
            ExecuteDDLCommand(Entity.DeleteEntityDefinition, DatabaseConnection)
        Next

    End Sub

#End Region

    Protected Friend Class Model

#Region "Model Variables"
        Private _entities As Entity()
        Private _security As Security()
        Private _database_compatibility As SQLServerCompatibility
        Private _database_name As String
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

        ''' <summary></summary>
        Protected Friend ReadOnly Property SecurityDefinition As String
            Get
                Dim ReturnString As String = EmptyString

                Dim SchemasAndRoles As Security() = (From nc As Security In _security Select nc).ToArray()

                For Each SchemaAndRole In SchemasAndRoles
                    ReturnString += My.Resources.ARA_ModelSecurityDefinition & vbCrLf
                    ReturnString = Replace(ReturnString, "{{{security group}}}", SchemaAndRole.SecurityGroup)
                    ReturnString = Replace(ReturnString, "{{{security role}}}", SchemaAndRole.SecurityRole)
                Next

                If ReturnString = EmptyString Then ReturnString = "--> no security defined"

                Return ReturnString
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

#End Region

#Region "Model Constants"
        Private Const Spacer As String = " "
        Private Const EmptyString As String = ""
        Private Const Null As String = "null"
        Private Const NotNull As String = "not null"

        Private Const ara_select_string As String = "ara_entity='{0}'"
        Private Const ara_abstract_column_select_string As String = "ara_entity='{0}' and ara_abstract_security_group='{1}' and ara_abstract_name='{2}'"

        Private Const ara_entity As String = "ara_entity"
        Private Const ara_entity_description As String = "ara_entity_description"
        Private Const ara_attribute_ordinal As String = "ara_attribute_ordinal"

        Private Const ara_abstract_security_group As String = "ara_abstract_security_group"
        Private Const ara_abstract_security_role As String = "ara_abstract_security_role"
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
                PrintClientError(New StackFrame().GetMethod().Name, ex)
                Exit Sub
            End Try

            ' loop through security
            Try
                Dim SecurityTable As DataTable = ModelDataset.Tables("ara_security_definition")
                Dim SecurityRow As DataRow

                For Each SecurityRow In SecurityTable.Rows
                    _security.AddMember(New Security(SecurityRow(ara_abstract_security_group), SecurityRow(ara_abstract_security_role)))
                Next

            Catch ex As Exception
                _load_successful = False
                PrintClientError(New StackFrame().GetMethod().Name, ex)
                Exit Sub
            End Try

            ' loop through each entity and load all object via the entity class constructors
            Try
                '
                Dim EntityTable As DataTable = ModelDataset.Tables("ara_entity_definition")
                Dim AttributeTable As DataTable = ModelDataset.Tables("ara_attribute_definition")
                Dim AbstractTable As DataTable = ModelDataset.Tables("ara_abstract_definition")
                Dim AbstractColumnTable As DataTable = ModelDataset.Tables("ara_abstract_column_definition")

                '
                Dim EntityName As String
                Dim EntityRow As DataRow ' for use in the for loop
                Dim AttributeRow As DataRow ' for use in the subset 
                Dim AttributeRows As DataRow() = Nothing ' the subset for entity attribute looping
                Dim AbstractRow As DataRow
                Dim AbstractRows As DataRow() = Nothing

                Dim AbstractColumnRows As DataRow() = Nothing

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


                    ' begin the abstracts
                    AbstractRows = AbstractTable.Select(String.Format(ara_select_string, EntityName))

                    For Each AbstractRow In AbstractRows

                        AbstractColumnRows = AbstractColumnTable.Select(String.Format(ara_abstract_column_select_string, EntityName, AbstractRow("ara_abstract_security_group").ToString, AbstractRow("ara_abstract_name").ToString))

                        NewEntity.NewAbstract(AbstractRow("ara_abstract_security_group").ToString,
                                              AbstractRow("ara_abstract_name").ToString,
                                              AbstractRow("ara_abstract_description").ToString,
                                              AbstractRow("ara_abstract_predicate").ToString,
                                              AbstractColumnRows)

                    Next

                    ' last step, add the entity to the model
                    _entities.AddMember(NewEntity)

                Next EntityRow

            Catch ex As Exception
                _load_successful = False
                PrintClientError(New StackFrame().GetMethod().Name, ex)
                Exit Sub
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
            Private _abstract As Abstract()
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
            Protected ReadOnly Property EntityType As EntityType
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

                    Dim stats As EntityAttribute()
                    stats = (From ua As EntityAttribute In UniqueAttributes Where ua.Ordinal > 1 Order By ua.Ordinal Select ua).ToArray

                    ReturnString += vbCrLf & vbCrLf & StatisticsDefinition(stats)

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
                                      Where Not nc.AddColumn And nc.ReferencedEntity <> EmptyString
                                      Select nc).ToArray()

                    ReturnString += ForeignKeyDefinition(AlteredColumns) & vbCrLf

                    '
                    AlteredColumns = (From nc As EntityAttribute In _attribute
                                      Where Not nc.AddColumn And nc.DefaultValue <> EmptyString
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
                    ReturnString = Replace(ReturnString, "{{{hash prefix}}}", If(HashLargeObjects = YesNoType.No, "hashbytes(N'sha1',", "[dbo].[ara_hash]("))
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
                    ReturnString = Replace(ReturnString, "{{{hash prefix}}}", If(HashLargeObjects = YesNoType.No, "hashbytes(N'sha1',", "[dbo].[ara_hash]("))
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
            Protected Friend ReadOnly Property ControlSecurityDefinition As String
                Get
                    Dim ReturnString As String = EmptyString

                    ReturnString = My.Resources.ARA_ControlSecurityDefinition & vbCrLf
                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)

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

                    ReturnString += vbCrLf & vbCrLf & StatisticsDefinition(ColumnSet)

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
            Protected Friend ReadOnly Property StatisticsDefinition(ByVal ColumnSet As EntityAttribute()) As String
                Get
                    Dim ReturnString As String = EmptyString

                    For Each Column In ColumnSet
                        ReturnString += My.Resources.ARA_EntityStatistics
                        ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)
                        ReturnString = Replace(ReturnString, "{{{attribute}}}", Column.Attribute)
                        ReturnString += vbCrLf & vbCrLf
                    Next Column

                    Return If(ReturnString = EmptyString, "--> no statistics", ReturnString)
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
            Protected Friend ReadOnly Property Abstracts As Abstract()
                Get
                    If IsNothing(_abstract) Then
                        Return Nothing
                    End If

                    Return _abstract
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AbstractCreateDefinition As String
                Get

                    Dim ReturnString As String = EmptyString

                    For Each Abstract In Abstracts
                        ReturnString += Abstract.CreateDefinition
                    Next

                    Return If(ReturnString = EmptyString, "--> no abstracts defined", ReturnString)
                End Get

            End Property

            ''' <summary></summary>
            Protected Friend ReadOnly Property AbstractDropDefinition As String
                Get

                    Dim ReturnString As String = EmptyString

                    ReturnString += My.Resources.ARA_AbstractDropDefinition & vbCrLf

                    ReturnString = Replace(ReturnString, "{{{entity}}}", Entity)

                    ReturnString = If(ReturnString = EmptyString, "--> nothing to drop", ReturnString)

                    Return ReturnString
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

                    ep += My.Resources.SYS_TablePropertyDefintion
                    ep = Replace(ep, "{{{domain}}}", Domain)
                    ep = Replace(ep, "{{{property}}}", "Entity Type")
                    ep = Replace(ep, "{{{value}}}", EntityType.ToString)
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
                        ep = Replace(ep, "{{{value}}}", Domain.ToString)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Description")
                        ep = Replace(ep, "{{{value}}}", Replace(ea.Description.ToString, "'", "''"))
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

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Assembly")
                        ep = Replace(ep, "{{{value}}}", "Slalom.Framework")
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{property}}}", "Copyright")
                        ep = Replace(ep, "{{{value}}}", "Slalom Consulting © 2014")
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Website")
                        ep = Replace(ep, "{{{value}}}", "www.slalom.com")
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Referenced Entity")
                        ep = Replace(ep, "{{{value}}}", ea.ReferencedEntity.ToString)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Referenced Entity Role")
                        ep = Replace(ep, "{{{value}}}", ea.ReferencedRole.ToString)
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Referenced Entity Type")
                        ep = Replace(ep, "{{{value}}}", If(ea.ReferencedEntityType = 0, "", ea.ReferencedEntityType.ToString))
                        ep += vbCrLf

                        ep += My.Resources.SYS_ColumnPropertyDefinition
                        ep = Replace(ep, "{{{domain}}}", Domain)
                        ep = Replace(ep, "{{{attribute}}}", ea.Attribute)
                        ep = Replace(ep, "{{{property}}}", "Default")
                        ep = Replace(ep, "{{{value}}}", Replace(ea.DefaultValue.ToString, "'", "''"))
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

                    If ReturnString = EmptyString Then Return EmptyString

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

                    If ReturnString = EmptyString Then Return New String(Spacer, Padding) & "--> no atomic attributes"

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

            ''' <summary></summary>
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
                If _entity_type <> _new_entity_type Then
                    Warnings.AddMember("Entity [" & Entity & "] has already been created as a '" & _entity_type.ToString & "' entity , it [will | can] not be converted to a '" & _new_entity_type.ToString & "' entity.")
                End If

                ' warn about about backing up data on delete

            End Sub

            ''' <summary></summary>
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

                    If Attribute.Contains(".") Then
                        Invalidations.AddMember("Entity attribute [" & Entity & "].[" & Attribute & "] cannot contain a period '.'.")
                    End If

                    If Attribute.Contains("_") Then
                        Invalidations.AddMember("Entity attribute [" & Entity & "].[" & Attribute & "] cannot contain a underscore '_'.")
                    End If

                    If Left(Attribute, 4) = "ara_" Then
                        Invalidations.AddMember("Entity attribute [" & Entity & "].[" & Attribute & "] cannot have a prefix of 'ara_'.")
                    End If

                    If Attribute = (Entity & "Key") Then
                        Invalidations.AddMember("Entity [" & Entity & "] cannot have an attribute named [" & Entity & "Key].")
                    End If

                    If ((AddColumn Or AlterColumn) And DefaultValue = "" And Optionality = YesNoType.No) And Not _create_entity Then
                        Warnings.AddMember("Adding or altering entity attribute [" & Entity & "].[" & Attribute & "] without optionality may require a default value.")
                    End If

                    If AlterAlternateKey And Not CreateEntity And Not DeleteEntity Then
                        Warnings.AddMember("Altering via deletion of a column on entity [" & Entity & "] may cause unique constraint errors.")
                    End If

                    If Attribute.ToUpper = Attribute Then
                        Warnings.AddMember("Entity attribute [" & Entity & "].[" & Attribute & "] is in upper case. Recommend proper case, e.g. CustomerNumber.")
                    End If

                    If Attribute.ToLower = Attribute Then
                        Warnings.AddMember("Entity attribute [" & Entity & "].[" & Attribute & "] is in lower case. Recommend proper case, e.g. CustomerNumber.")
                    End If

                    ' TODO: add operand clash warnings

                    ' add the attribute to the entity
                    _attribute.AddMember(New EntityAttribute(Attribute, _
                                                             ReferencedEntity, _
                                                             ReferencedEntityType, _
                                                             Datatype, _
                                                             DefaultValue,
                                                             Ordinal, _
                                                             SortOrder,
                                                             Optionality, _
                                                             BusinessIdentifier,
                                                             Description, _
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

            ''' <summary></summary>
            Protected Friend Sub NewAbstract(ByVal SecurityGroup As String,
                                             ByVal Abstract As String,
                                             ByVal Description As String,
                                             ByVal Predicate As String,
                                             ByVal Columns As DataRow())

                _abstract.AddMember(New Entity.Abstract(Entity, EntityType, SecurityGroup, Abstract, Description, Predicate, Columns))

            End Sub

#End Region

            ''' <summary></summary>
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
                Private _add_column As Boolean
                Private _delete_column As Boolean
                Private _alter_column As Boolean
                Private _alter_foreign_key As Boolean
                Private _alter_default As Boolean
                Private _alter_alternate_key As Boolean
#End Region

#Region "Attribute Properties"

                ''' <summary></summary>
                Protected Friend ReadOnly Property Attribute As String
                    Get
                        Return _attribute
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property ColumnName As String
                    Get
                        Return "[" & Replace(Replace(_attribute, "]", ""), "[", "") & "]"
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property PrintableColumnName As String
                    Get
                        Dim rt As String = Replace(Replace(_attribute, "]", ""), "[", "")

                        If rt.Length <= 25 Then
                            Return rt
                        Else
                            rt = Left(rt, 24) & "~"
                            Return "[" & rt & "]"
                        End If

                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property ReferencedEntity As String
                    Get
                        Return _referenced_entity
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property ReferencedEntityType As EntityType
                    Get
                        Return _referenced_entity_type
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property ReferencedRole As String
                    Get
                        If ReferencedEntity = EmptyString Then
                            Return EmptyString
                        End If

                        Return Replace(_attribute, _referenced_entity & "Key", "")
                    End Get
                End Property

                ''' <summary></summary>
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

                ''' <summary></summary>
                Protected Friend ReadOnly Property DefaultValue As String
                    Get
                        Return _default_value
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property Ordinal As Integer
                    Get
                        Return _ordinal
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property SortOrder As SortOrderType
                    Get
                        Return _sort_order
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property Optionality As YesNoType
                    Get
                        Return _optionality
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property BusinessIdentifier As YesNoType
                    Get
                        Return _business_identifier
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property Description As String
                    Get
                        Return _description
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property AddColumn As Boolean
                    Get
                        Return _add_column
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property DeleteColumn As Boolean
                    Get
                        Return _delete_column
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property AlterColumn As Boolean
                    Get
                        Return _alter_column
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property AlterForeignKey As Boolean
                    Get
                        Return _alter_foreign_key
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property AlterDefault As Boolean
                    Get
                        Return _alter_default
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property AlterAlternateKey As Boolean
                    Get
                        Return _alter_alternate_key
                    End Get
                End Property

#End Region

#Region "Attribute Contructors"

                ''' <summary></summary>
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
                    _add_column = AddColumn
                    _delete_column = DeleteColumn
                    _alter_column = AlterColumn
                    _alter_foreign_key = AlterForeignKey
                    _alter_default = AlterDefault
                    _alter_alternate_key = AlterAlternateKey

                End Sub

#End Region

            End Class ' EntityAttribute

            ''' <summary></summary>
            Protected Friend Class Abstract

                Private _entity_name As String
                Private _entity_type As Model.EntityType
                Private _security_group As String
                Private _abstract As String
                Private _description As String
                Private _predicate As String
                Private _columns As AbstractColumn()

                ''' <summary></summary>
                Protected Friend ReadOnly Property SecurityGroup As String
                    Get
                        Return _security_group
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property Abstract As String
                    Get
                        Return _abstract
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property Description As String
                    Get
                        Return _description
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property Predicate As String
                    Get
                        Return _predicate
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property AbstractColumns As AbstractColumn()
                    Get
                        Return _columns
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend ReadOnly Property CreateDefinition As String
                    Get

                        Dim ReturnString As String = EmptyString

                        If _entity_type = Model.EntityType.TypeI Then
                            ReturnString += My.Resources.ARA_AbstractDefinitionTypeI & vbCrLf
                        ElseIf _entity_type = Model.EntityType.TypeII Then
                            ReturnString += My.Resources.ARA_AbstractDefinitionTypeII & vbCrLf
                        End If

                        ReturnString = Replace(ReturnString, "{{{entity}}}", _entity_name)
                        ReturnString = Replace(ReturnString, "{{{security group}}}", SecurityGroup)
                        ReturnString = Replace(ReturnString, "{{{abstract name}}}", Abstract)
                        ReturnString = Replace(ReturnString, "{{{return set}}}", ColumnListDefinition(AbstractColumns, True, 3))
                        ReturnString = Replace(ReturnString, "{{{column set}}}", ColumnListDefinition(AbstractColumns, False, 3))
                        ReturnString = Replace(ReturnString, "{{{predicate}}}", Replace(Predicate, "'", "''"))

                        ReturnString += vbCrLf & vbCrLf

                        Return If(ReturnString = EmptyString, "--> no abstracts defined", ReturnString)
                    End Get

                End Property



                ''' <summary></summary>
                Private ReadOnly Property ColumnListDefinition(ByVal ColumnSet As AbstractColumn(),
                                                               ByVal GetReturnName As Boolean,
                                                               Optional ByVal Padding As Integer = 0) As String
                    Get
                        Dim ReturnString As String = EmptyString

                        For Each Column In ColumnSet
                            ReturnString += New String(Spacer, Padding) & If(GetReturnName, Column.ColumnName, Column.Attribute) & "," & vbCrLf
                        Next Column

                        If ReturnString = EmptyString Then Return "--> no attributes defined"

                        Return Left(ReturnString, Len(ReturnString) - 2)
                    End Get
                End Property

                ''' <summary></summary>
                Protected Friend Sub New(ByVal EntityName As String,
                                         ByVal EntityType As EntityType,
                                         ByVal SecurityGroup As String,
                                         ByVal Abstract As String,
                                         ByVal Description As String,
                                         ByVal Predicate As String,
                                         ByVal AbstractColumns As DataRow())

                    _entity_name = EntityName
                    _entity_type = EntityType
                    _security_group = SecurityGroup
                    _abstract = Abstract
                    _description = Description
                    _predicate = Predicate

                    For Each AbstractColumnRow In AbstractColumns
                        _columns.AddMember(New AbstractColumn(AbstractColumnRow("ara_attribute_name").ToString, AbstractColumnRow("ara_abstract_alternate_name").ToString))
                    Next

                End Sub

                ''' <summary></summary>
                Protected Friend Class AbstractColumn

                    Private _attribute As String
                    Private _alternate_name As String

                    ''' <summary></summary>
                    Protected Friend ReadOnly Property Attribute As String
                        Get
                            Return "[" & _attribute & "]"
                        End Get
                    End Property

                    ''' <summary></summary>
                    Protected Friend ReadOnly Property ColumnName As String
                        Get
                            Return "[" & If(_alternate_name = EmptyString, _attribute, _alternate_name) & "]"
                        End Get
                    End Property

                    ''' <summary></summary>
                    Protected Friend Sub New(ByVal Attribute As String,
                                             ByVal AlternateName As String)
                        _attribute = Attribute
                        _alternate_name = AlternateName

                    End Sub

                End Class ' AbstractColumn

            End Class ' Abstract

        End Class ' Entity

        ''' <summary></summary>
        Protected Friend Class Security

            Private _security_group As String
            Private _security_role As String

            ''' <summary></summary>
            Protected Friend ReadOnly Property SecurityGroup As String
                Get
                    Return _security_group
                End Get
            End Property

            Protected Friend ReadOnly Property SecurityRole As String
                Get
                    Return _security_role
                End Get
            End Property

            ''' <summary></summary>
            Protected Friend Sub New(ByVal SecurityGroup As String,
                                     ByVal SecurityRole As String)

                _security_group = SecurityGroup
                _security_role = SecurityRole

            End Sub

        End Class ' Security

    End Class ' Model

End Class
