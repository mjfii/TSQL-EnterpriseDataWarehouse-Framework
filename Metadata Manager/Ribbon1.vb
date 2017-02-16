Imports Microsoft.Office.Tools.Ribbon

Public Class Ribbon1

    Private Sub Ribbon1_Load(ByVal sender As System.Object, ByVal e As RibbonUIEventArgs) Handles MyBase.Load

    End Sub



    Private Sub Button1_Click(sender As Object, e As RibbonControlEventArgs) Handles Button1.Click

        Dim x As New registryManagement

        Dim existingInstances As String = x.GetRegistryValue("psa_servers")
        Dim existingInstance As String() = existingInstances.Split(","c)

        Dim newInstance As String = InputBox("Enter new instance name...", "EDW Framework", , )

        If newInstance <> "" Then

            Array.Resize(existingInstance, existingInstance.Length + 1)

            existingInstance(existingInstance.Length - 1) = newInstance

            existingInstances = String.Join(",", existingInstance)

            x.AddRegistryValue("psa_servers", existingInstances)
        End If

        ''
        Dim z As RibbonDropDown = Me.serverChoice

        z.Items.Clear()

        Dim newChoiceItem As RibbonDropDownItem

        For Each server As String In existingInstance

            newChoiceItem = Globals.Factory.GetRibbonFactory.CreateRibbonDropDownItem
            newChoiceItem.Label = server
            z.Items.Add(newChoiceItem)
        Next










    End Sub
End Class
