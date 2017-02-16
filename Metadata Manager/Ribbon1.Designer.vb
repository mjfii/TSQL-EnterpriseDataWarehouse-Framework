Partial Class Ribbon1
    Inherits Microsoft.Office.Tools.Ribbon.RibbonBase

    <System.Diagnostics.DebuggerNonUserCode()> _
   Public Sub New(ByVal container As System.ComponentModel.IContainer)
        MyClass.New()

        'Required for Windows.Forms Class Composition Designer support
        If (container IsNot Nothing) Then
            container.Add(Me)
        End If

    End Sub

    <System.Diagnostics.DebuggerNonUserCode()> _
    Public Sub New()
        MyBase.New(Globals.Factory.GetRibbonFactory())

        'This call is required by the Component Designer.
        InitializeComponent()

    End Sub

    'Component overrides dispose to clean up the component list.
    <System.Diagnostics.DebuggerNonUserCode()> _
    Protected Overrides Sub Dispose(ByVal disposing As Boolean)
        Try
            If disposing AndAlso components IsNot Nothing Then
                components.Dispose()
            End If
        Finally
            MyBase.Dispose(disposing)
        End Try
    End Sub

    'Required by the Component Designer
    Private components As System.ComponentModel.IContainer

    'NOTE: The following procedure is required by the Component Designer
    'It can be modified using the Component Designer.
    'Do not modify it using the code editor.
    <System.Diagnostics.DebuggerStepThrough()> _
    Private Sub InitializeComponent()
        Me.Tab1 = Me.Factory.CreateRibbonTab
        Me.Group1 = Me.Factory.CreateRibbonGroup
        Me.Header = Me.Factory.CreateRibbonTab
        Me.Group2 = Me.Factory.CreateRibbonGroup
        Me.Separator1 = Me.Factory.CreateRibbonSeparator
        Me.Button2 = Me.Factory.CreateRibbonButton
        Me.ComboBox3 = Me.Factory.CreateRibbonComboBox
        Me.ComboBox4 = Me.Factory.CreateRibbonComboBox
        Me.DropDown1 = Me.Factory.CreateRibbonDropDown
        Me.serverChoice = Me.Factory.CreateRibbonDropDown
        Me.Separator3 = Me.Factory.CreateRibbonSeparator
        Me.Button1 = Me.Factory.CreateRibbonButton
        Me.Tab1.SuspendLayout()
        Me.Header.SuspendLayout()
        Me.Group2.SuspendLayout()
        '
        'Tab1
        '
        Me.Tab1.ControlId.ControlIdType = Microsoft.Office.Tools.Ribbon.RibbonControlIdType.Office
        Me.Tab1.Groups.Add(Me.Group1)
        Me.Tab1.Label = "TabAddIns"
        Me.Tab1.Name = "Tab1"
        '
        'Group1
        '
        Me.Group1.Label = "Group1"
        Me.Group1.Name = "Group1"
        '
        'Header
        '
        Me.Header.Groups.Add(Me.Group2)
        Me.Header.Label = "EDW Framework"
        Me.Header.Name = "Header"
        '
        'Group2
        '
        Me.Group2.Items.Add(Me.Button1)
        Me.Group2.Items.Add(Me.Separator3)
        Me.Group2.Items.Add(Me.serverChoice)
        Me.Group2.Items.Add(Me.DropDown1)
        Me.Group2.Items.Add(Me.ComboBox3)
        Me.Group2.Items.Add(Me.ComboBox4)
        Me.Group2.Items.Add(Me.Separator1)
        Me.Group2.Items.Add(Me.Button2)
        Me.Group2.Label = "PSA"
        Me.Group2.Name = "Group2"
        '
        'Separator1
        '
        Me.Separator1.Name = "Separator1"
        '
        'Button2
        '
        Me.Button2.ControlSize = Microsoft.Office.Core.RibbonControlSize.RibbonControlSizeLarge
        Me.Button2.Label = "Save"
        Me.Button2.Name = "Button2"
        Me.Button2.ShowImage = True
        '
        'ComboBox3
        '
        Me.ComboBox3.Image = Global.Metadata_Manager.My.Resources.Resources.schema
        Me.ComboBox3.Label = "Schema"
        Me.ComboBox3.Name = "ComboBox3"
        Me.ComboBox3.ShowImage = True
        '
        'ComboBox4
        '
        Me.ComboBox4.Label = "Entity"
        Me.ComboBox4.Name = "ComboBox4"
        '
        'DropDown1
        '
        Me.DropDown1.Label = "Database"
        Me.DropDown1.Name = "DropDown1"
        '
        'serverChoice
        '
        Me.serverChoice.Label = "Server"
        Me.serverChoice.Name = "serverChoice"
        '
        'Separator3
        '
        Me.Separator3.Name = "Separator3"
        '
        'Button1
        '
        Me.Button1.ControlSize = Microsoft.Office.Core.RibbonControlSize.RibbonControlSizeLarge
        Me.Button1.Label = "Add Server"
        Me.Button1.Name = "Button1"
        Me.Button1.ShowImage = True
        '
        'Ribbon1
        '
        Me.Name = "Ribbon1"
        Me.RibbonType = "Microsoft.Excel.Workbook"
        Me.Tabs.Add(Me.Tab1)
        Me.Tabs.Add(Me.Header)
        Me.Tab1.ResumeLayout(False)
        Me.Tab1.PerformLayout()
        Me.Header.ResumeLayout(False)
        Me.Header.PerformLayout()
        Me.Group2.ResumeLayout(False)
        Me.Group2.PerformLayout()

    End Sub

    Friend WithEvents Tab1 As Microsoft.Office.Tools.Ribbon.RibbonTab
    Friend WithEvents Group1 As Microsoft.Office.Tools.Ribbon.RibbonGroup
    Friend WithEvents Header As Microsoft.Office.Tools.Ribbon.RibbonTab
    Friend WithEvents Group2 As Microsoft.Office.Tools.Ribbon.RibbonGroup
    Friend WithEvents Button2 As Microsoft.Office.Tools.Ribbon.RibbonButton
    Friend WithEvents Separator1 As Microsoft.Office.Tools.Ribbon.RibbonSeparator
    Friend WithEvents DropDown1 As Microsoft.Office.Tools.Ribbon.RibbonDropDown
    Friend WithEvents ComboBox3 As Microsoft.Office.Tools.Ribbon.RibbonComboBox
    Friend WithEvents ComboBox4 As Microsoft.Office.Tools.Ribbon.RibbonComboBox
    Friend WithEvents Button1 As Microsoft.Office.Tools.Ribbon.RibbonButton
    Friend WithEvents Separator3 As Microsoft.Office.Tools.Ribbon.RibbonSeparator
    Friend WithEvents serverChoice As Microsoft.Office.Tools.Ribbon.RibbonDropDown
End Class

Partial Class ThisRibbonCollection

    <System.Diagnostics.DebuggerNonUserCode()> _
    Friend ReadOnly Property Ribbon1() As Ribbon1
        Get
            Return Me.GetRibbon(Of Ribbon1)()
        End Get
    End Property
End Class
