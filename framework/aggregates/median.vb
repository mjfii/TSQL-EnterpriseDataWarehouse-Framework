Imports System.Data
Imports System.Data.SqlClient
Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Collections.Generic
Imports System.Text

<Serializable> _
<Microsoft.SqlServer.Server.SqlUserDefinedAggregate(Format.UserDefined, IsInvariantToDuplicates:=False, IsInvariantToNulls:=False, IsInvariantToOrder:=False, MaxByteSize:=8000)> _
Public Structure Median

    Implements IBinarySerialize

    Public Sub Read(r As System.IO.BinaryReader) Implements IBinarySerialize.Read
        Dim cnt As Integer = r.ReadInt32()
        Me.ld = New List(Of Double)(cnt)
        For i As Integer = 0 To cnt - 1
            Me.ld.Add(r.ReadDouble())
        Next
    End Sub

    Public Sub Write(w As System.IO.BinaryWriter) Implements IBinarySerialize.Write
        w.Write(Me.ld.Count)
        For Each d As Double In Me.ld
            w.Write(d)
        Next
    End Sub





    'Variables to hold the values;
    Private ld As List(Of Double)

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub Init()
        ld = New List(Of Double)
    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="Value"></param>
    ''' <remarks></remarks>
    Public Sub Accumulate(Value As SqlDouble)
        If Not Value.IsNull Then
            ld.Add(Value.Value)
        End If
    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="Group"></param>
    ''' <remarks></remarks>
    Public Sub Merge(Group As Median)
        Me.ld.AddRange(Group.ld.ToArray())
    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function Terminate() As SqlDouble

        If ld.Count = 0 Then
            Return SqlDouble.Null
        End If

        ld.Sort()
        Dim index As Integer = CInt(CInt(ld.Count) / 2)

        If ld.Count Mod 2 = 0 Then
            Return (ld(index) + ld(index - 1)) / 2
        Else
            Return ld(index)
        End If
    End Function

End Structure

