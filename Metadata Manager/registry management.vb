Public Class registryManagement

    Private Const registryKey As String = "EDW Framework"

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New()
        My.Computer.Registry.CurrentUser.CreateSubKey(registryKey)
    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="registryValueName"></param>
    ''' <param name="registryValue"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function AddRegistryValue(ByVal registryValueName As String, registryValue As String) As Boolean

        My.Computer.Registry.SetValue("HKEY_CURRENT_USER\" & registryKey, registryValueName, registryValue)

        Return True
    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="registryValueName"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function GetRegistryValue(ByVal registryValueName As String) As String

        Return My.Computer.Registry.GetValue("HKEY_CURRENT_USER\" & registryKey, registryValueName, Nothing).ToString()

    End Function

End Class
