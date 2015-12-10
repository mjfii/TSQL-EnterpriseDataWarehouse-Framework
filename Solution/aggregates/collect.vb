Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports System.Data
Imports System.Text

''' <summary>
''' 
''' </summary>
''' <remarks></remarks>
<Serializable> _
<SqlUserDefinedAggregate(Format.UserDefined, IsInvariantToOrder:=False, IsInvariantToNulls:=True, IsInvariantToDuplicates:=False, MaxByteSize:=-1)> _
Public Structure Collect

    Implements IBinarySerialize

    ''' <summary>
    ''' deserialize from the reader to recreate the struct
    ''' </summary>
    ''' <param name="r">BinaryReader</param>
    Private Sub Read(r As System.IO.BinaryReader) Implements IBinarySerialize.Read
        _delimiter = r.ReadString()
        _accumulator = New StringBuilder(r.ReadString())

        If _accumulator.Length <> 0 Then
            Me.IsNull = False
        End If
    End Sub

    ''' <summary>
    ''' searialize the struct
    ''' </summary>
    ''' <param name="w">BinaryWriter</param>
    Private Sub Write(w As System.IO.BinaryWriter) Implements IBinarySerialize.Write
        w.Write(_delimiter)
        w.Write(_accumulator.ToString())
    End Sub


    Private _accumulator As StringBuilder
    Private _delimiter As String
    Private _is_null As Boolean

    ''' <summary>
    ''' 
    ''' </summary>
    Public Property IsNull() As Boolean
        Get
            Return _is_null
        End Get
        Private Set(value As Boolean)
            _is_null = value
        End Set
    End Property

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub Init()
        _accumulator = New StringBuilder()
        _delimiter = String.Empty
        Me.IsNull = True
    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="Value"></param>
    ''' <param name="Delimiter"></param>
    ''' <remarks></remarks>
    Public Sub Accumulate(Value As SqlString, Delimiter As SqlString)

        If Not Delimiter.IsNull And Delimiter.Value.Length > 0 Then
            _delimiter = Delimiter.Value

            If _accumulator.Length > 0 Then
                _accumulator.Append(Delimiter.Value)
            End If
        End If

        _accumulator.Append(Value.Value)

        If Value.IsNull = False Then
            Me.IsNull = False
        End If
    End Sub

    ''' <summary>
    ''' Merge onto the end 
    ''' </summary>
    ''' <param name="Group"></param>
    Public Sub Merge(Group As Collect)

        ' add the delimiter between strings
        If _accumulator.Length > 0 And Group._accumulator.Length > 0 Then
            _accumulator.Append(_delimiter)
        End If

        _accumulator.Append(Group._accumulator.ToString())

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function Terminate() As SqlString
        Return New SqlString(_accumulator.ToString())
    End Function


End Structure
