Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Data.SqlClient
Imports System.Security.Cryptography.SHA1

Namespace Common

    Partial Public Class Methods

        <Microsoft.SqlServer.Server.SqlFunction> _
        Public Shared Function StringHash(ByVal HashingString As String) As SqlBinary
            Return If(IsNothing(HashingString), New SqlBinary, _
                      Security.Cryptography.SHA1.Create.ComputeHash _
                      (Text.UnicodeEncoding.Unicode.GetBytes(HashingString)))
        End Function

    End Class ' Methods

End Namespace