import PySimpleGUI

class renameGUI:
    def __init__(self, url):
        self.config_db_url = url
        self.renameWindow = None
        self.calibrated_parameters = None

    def createWindow(self, window, calibrated_parameters):
        self.calibrated_parameters = calibrated_parameters
        self.renameWindow = PySimpleGUI.Window("Rename", self.rename_gui(), modal=True, finalize=True)
        self.renameWindow.move(window.current_location()[0], window.current_location()[1])

    def rename_gui(self):
        sz = (10, 20)
        layout_wwtp = [
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

        values = []

        for key in self.calibrated_parameters.keys():
            values.append([key]+list(self.calibrated_parameters[key].values()))

        layout_parameters_calibrated = [
            [PySimpleGUI.Table(
                values=values,
                headings=[
                    "ID",
                    "Name",
                    "Generation (g/person/day)",
                    "Removal P (%)",
                    "Removal SP (%)",
                    "Removal SN (%)",
                    "Removal SC (%)",
                    "Removal UF (%)",
                    "Removal CL (%)",
                    "Removal UV (%)",
                    "Removal OTHER (%)",
                    "Removal SF (%)",
                ],


                auto_size_columns=True,
                key="parameters",
                expand_x=False
            )],
            [PySimpleGUI.Button("ESTIMATE EFFLUENT", key="run_estimate_effluent")]
        ]

        tabs = PySimpleGUI.TabGroup([[
            PySimpleGUI.Tab('WWTP', layout_wwtp),
            PySimpleGUI.Tab('Calibrated parameters', layout_parameters_calibrated, key="calibration_tab", disabled=True),
        ]], expand_x=True)

        return [[tabs]]


