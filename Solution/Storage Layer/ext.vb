Imports System.Runtime.CompilerServices

Module BuildExtentions

    ''' <summary>Extension to add new member to any given array of existing members.</summary>
    ''' <param name="Members">Existing array to which the new member is added.</param>
    ''' <param name="NewMember">New member to add the the Members array.</param>
    ''' <typeparam name="T">Data type of array.</typeparam>
    <Extension()> _
    Friend Sub AddMember(Of T)(ByRef Members As T(), NewMember As T)
        If Members IsNot Nothing Then
            Array.Resize(Members, Members.Length + 1) ' unsure as to whether ReDim Preserve is faster
            Members(Members.Length - 1) = NewMember
        Else
            ReDim Members(0)
            Members(0) = NewMember
        End If
    End Sub

    ''' <summary>Converts string to EDW.Common.EntityType.</summary>
    ''' <param name="InputString">String to convert.</param>
    ''' <returns>EDW.Common.EntityType</returns>
    <Extension()> _
    Friend Function StringToEntityType(ByVal InputString As String) As EDW.Common.EntityType
        Return If(InputString = "2", EDW.Common.EntityType.TypeII, If(InputString = "1", EDW.Common.EntityType.TypeI, Nothing))
    End Function

    ''' <summary>EDW.Common.YesNoType.</summary>
    ''' <param name="InputString">String to convert.</param>
    ''' <returns>EDW.Common.YesNoType</returns>
    <Extension()> _
    Friend Function StringToYesNoType(ByVal InputString As String) As EDW.Common.YesNoType
        Return If(InputString = "Yes", EDW.Common.YesNoType.Yes, EDW.Common.YesNoType.No)
    End Function

    ''' <summary>Converts null string to and empty string.</summary>
    ''' <param name="InputString">String to validate.</param>
    ''' <returns>String</returns>
    <Extension()> _
    Friend Function RemoveNulls(ByVal InputString As String) As String
        Return If(IsDBNull(InputString), "", InputString)
    End Function

    ''' <summary>Converts binary/bit string to Boolean.</summary>
    ''' <param name="InputString">String to validate.</param>
    ''' <returns>Boolean</returns>
    <Extension()> _
    Friend Function StringToBool(ByVal InputString As String) As Boolean
        Return CBool(InputString)
    End Function

    ''' <summary>Converts string to EDW.Common.SortOrderType.</summary>
    ''' <param name="InputString">String to validate.</param>
    ''' <returns>EDW.Common.SortOrderType</returns>
    <Extension()> _
    Friend Function StringToSortOrderType(ByVal InputString As String) As EDW.Common.SortOrderType
        Return If(InputString = "desc", EDW.Common.SortOrderType.desc, EDW.Common.SortOrderType.asc)
    End Function

    ''' <summary>Converts string to EDW.Common.SQLServerCompatibility.</summary>
    ''' <param name="InputString">String to validate.</param>
    ''' <returns>EDW.Common.SQLServerCompatibility</returns>
    <Extension()> _
    Friend Function StringToDatabaseCompatibility(ByVal InputString As String) As EDW.Common.SQLServerCompatibility

        Select Case InputString
            Case "100"
                Return EDW.Common.SQLServerCompatibility.SQLServer2008
            Case "110"
                Return EDW.Common.SQLServerCompatibility.SQLServer2012
            Case "120"
                Return EDW.Common.SQLServerCompatibility.SQLServer2014
            Case "130"
                Return EDW.Common.SQLServerCompatibility.SQLServer2016
            Case Else
                Return Nothing
        End Select

    End Function

    ''' <summary>Converts null string to and empty string.</summary>
    ''' <param name="InputString">String to validate.</param>
    ''' <returns>String</returns>
    <Extension()> _
    Friend Function EscapeTicks(ByVal InputString As String) As String
        Return Replace(InputString, "'", "''")
    End Function

End Module