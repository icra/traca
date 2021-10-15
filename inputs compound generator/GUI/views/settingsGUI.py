import PySimpleGUI
from lib.db.Custom_SQLite import Custom_SQLite as cS


class settingsGUI:

    def __init__(self, url):
        self.config_db_url = url
        self.configWindow = None
        self.list_configs = []
        self.selected_config = None

    def createWindow(self, window):
        self.configWindow = PySimpleGUI.Window("Settings", self.config_db_GUI(), modal=True, finalize=True)
        self.configWindow.move(window.current_location()[0], window.current_location()[1])
        if self.selected_config is not None:
            index = 0
            for cl in self.list_configs:
                if self.selected_config.config_id == cl.config_id:
                    self.refreshList(index)
                    self.load_data(index)
                index += 1

    def refreshList(self, list_position=None):
        config_db = cS(self.config_db_url)
        self.list_configs = config_db.getConfigurations()
        if self.configWindow is not None:
            self.configWindow['config_list_selection'].update(self.list_configs, set_to_index=list_position)

    def load_data(self, list_index):
        if len(self.list_configs) > list_index:
            config = self.list_configs[list_index]
            self.selected_config = config
            if self.configWindow is not None:
                self.configWindow['url'].update(config.postgre_url)
                self.configWindow['db_name'].update(config.postgre_dbname)
                self.configWindow['user'].update(config.postgre_user)
                self.configWindow['password'].update(config.postgre_pass)
                self.configWindow['url_eu_db'].update(config.eu_db)
                self.configWindow['dp_pop_db'].update(config.dp_pop_db)
                self.configWindow['wwtp_con_db'].update(config.wwtp_con_db)
                self.configWindow['comp_con_db'].update(config.comp_con_db)

    def config_db_GUI(self):
        config_db = cS(self.config_db_url)

        config_list_col = [
            [PySimpleGUI.InputText('', key='config_name'), PySimpleGUI.Button('RENAME', key='rename_data'),
             PySimpleGUI.Button('+', key='add_data')],
            [PySimpleGUI.Listbox(values=self.list_configs, size=(58, 21), key='config_list_selection', enable_events=True)],
            [PySimpleGUI.Button('DELETE', key='delete_data', button_color='red')]
        ]

        conn_data_col = [
            # [PySimpleGUI.Text("Config all input data", font='Helvetica 15 bold')],
            [PySimpleGUI.Text("Postgre configuration:", font='Helvetica 15 bold')],
            [PySimpleGUI.HSeparator()],
            [PySimpleGUI.Text('URL', size=(35, 1)), PySimpleGUI.InputText('', key='url')],
            [PySimpleGUI.Text('DB Name', size=(35, 1)), PySimpleGUI.InputText('', key='db_name')],
            # [PySimpleGUI.Text('Port', size=(15, 1)), PySimpleGUI.InputText('', key='port')],
            [PySimpleGUI.Text('User', size=(35, 1)), PySimpleGUI.InputText('', key='user')],
            [PySimpleGUI.Text('Password', size=(35, 1)), PySimpleGUI.InputText('', key='password', password_char='*')],
            [PySimpleGUI.Button('Test connection', key='test_db')],
            [PySimpleGUI.Text('', key='test_db_response')],
            [PySimpleGUI.Text("Database and CSV Locations:", font='Helvetica 15 bold')],
            [PySimpleGUI.HSeparator()],
            [],
            [PySimpleGUI.Text('EUROPEAN WWTP DATABASE (Sqlite)', size=(35, 1)),
             PySimpleGUI.InputText('', key='url_eu_db'), PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Text('DISCHARGEPOINTS POPULATION (csv)', size=(35, 1)),
             PySimpleGUI.InputText('', key='dp_pop_db'), PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Text('WWTP ENTRY CONCENTRATIONS (csv)', size=(35, 1)),
             PySimpleGUI.InputText('', key='wwtp_con_db'), PySimpleGUI.FileBrowse('Select')],
            [PySimpleGUI.Text('COMPOUNTS BY COUNTRY (csv)', size=(35, 1)), PySimpleGUI.InputText('', key='comp_con_db'),
             PySimpleGUI.FileBrowse('Select')],
        ]

        config_data_col = [[PySimpleGUI.TabGroup([[
            PySimpleGUI.Tab('Connection / Inputs', conn_data_col),
            PySimpleGUI.Tab('Parameters', self.parameters_gui())
        ]])], [PySimpleGUI.Button('SAVE', key='save_data'), PySimpleGUI.Cancel()]]

        layout = [
            [PySimpleGUI.Column(config_list_col), PySimpleGUI.Column(config_data_col)]
        ]
        return layout

    def parameters_gui(self):
        layout = []
        return layout