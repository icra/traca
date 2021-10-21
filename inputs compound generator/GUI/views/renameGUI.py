import PySimpleGUI
from lib.db.Custom_SQLite import Custom_SQLite as cS


class renameGUI:

    def __init__(self, url):
        self.config_db_url = url
        self.renameWindow = None

    def createWindow(self, window):
        self.renameWindow = PySimpleGUI.Window("Rename", self.rename_gui(), modal=True, finalize=True)
        self.renameWindow.move(window.current_location()[0], window.current_location()[1])

    def rename_gui(self):
        sz = (10, 20)
        layout = [
            [PySimpleGUI.Text('Threshold (in km)'), PySimpleGUI.Input(key='input_rename_threshold', default_text="10",), PySimpleGUI.Text('.SQLite location: '), PySimpleGUI.InputText('', key='wwt_file_name', enable_events = True), PySimpleGUI.FileBrowse() ],
            [PySimpleGUI.Table(
                values=[["-", "-", "-", "-", "-"]],
                headings=[
                    "DP ID",
                    "Name",
                    "ID",
                    "Name SQLITE",
                    "Distance (km)",
                ],
                auto_size_columns=False,
                col_widths=[30, 30, 15, 15, 15],
                num_rows=10,
                key="rename_table",
                expand_x=False
            )],
            [PySimpleGUI.Button("RUN", key="run_rename")]

        ]

        return layout


