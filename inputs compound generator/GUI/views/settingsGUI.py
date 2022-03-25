import PySimpleGUI


class settingsGUI:


    def __init__(self):
        self.list_configs = []
        list_configs = []
        self.configWindow = None


    def createWindow(self, window):
        self.configWindow = PySimpleGUI.Window("File properties", self.config_db_GUI(), modal=True, finalize=True)
        self.configWindow.move(window.current_location()[0], window.current_location()[1])

    def config_db_GUI(self):


        col = [
            [PySimpleGUI.Text("CSV Locations:", font='Helvetica 15 bold')],
            [PySimpleGUI.HSeparator()],
            [],
            [PySimpleGUI.Text('EDAR COMPOUNDS (csv)', size=(35, 1)),
             PySimpleGUI.InputText('', key='dp_pop_db'), PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Text('EDAR POPULATIONS (csv)', size=(35, 1)),
             PySimpleGUI.InputText('', key='wwtp_con_db'), PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Text('EDAR ANALITIQUES (xlsx)', size=(35, 1)), PySimpleGUI.InputText('', key='comp_con_db'),
             PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Text('EDAR ANALITIQUES (xlsx)', size=(35, 1)), PySimpleGUI.InputText('', key='comp_con_db'),
             PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Text('EDAR PTR (xlsx)', size=(35, 1)), PySimpleGUI.InputText('', key='comp_con_db'),
             PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Text('EDAR CABALS (xlsx)', size=(35, 1)), PySimpleGUI.InputText('', key='comp_con_db'),
             PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Text('REMOVAL RATE (csv)', size=(35, 1)), PySimpleGUI.InputText('', key='comp_con_db'),
             PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Button('SAVE', key='save_data')]
        ]

        layout = [
            [PySimpleGUI.Column(col)]
        ]
        return layout