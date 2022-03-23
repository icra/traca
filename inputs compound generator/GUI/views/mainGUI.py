import PySimpleGUI


class mainGUI:
    global list_configs
    list_configs = []

    def __init__(self):
        self.window = PySimpleGUI.Window("IG+", self.init_GUI(), finalize=True, location=(0, 0))

    def init_GUI(self):
        menu_def = [
            ['Settings', ['File properties']]
        ]
        layout = [
            [PySimpleGUI.Menu(menu_def, tearoff=False)],
            [PySimpleGUI.Text("SWAT+ Input Generator (TRAÇA)")],
            [PySimpleGUI.Table(
                values=[["-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"]],
                headings=[
                    "EDAR EU_ID",
                    "SWAT ID",
                    "Name",
                    "Latitude",
                    "Longitude",
                    "Population",
                    "Primary",
                    "Secondary",
                    "Tertiary",
                    "DBO",
                    "TP",
                    "TN"
                ],
                num_rows=12,
                key="dp_table",
                expand_x=True
            )],
            [PySimpleGUI.Text('.SQLite location: '),
             PySimpleGUI.InputText('', key='swat_db_sqlite', enable_events=True),
             PySimpleGUI.FileBrowse(),
             PySimpleGUI.Button("Add data from the point of discharge", key="add_dp_data")
             ],

        ]

        return layout

    def update_table(self, edars_calibrated):
        edars_table = []
        for edar in edars_calibrated.values():
            row = []
            try:
                row.append(edar["eu_code"])
            except:
                row.append("-")
            try:
                row.append(edar["nom_swat"])
            except:
                row.append("-")
            try:
                row.append(edar["nom"])
            except:
                row.append("-")
            try:
                row.append(edar["lat"])
            except:
                row.append("-")
            try:
                row.append(edar["long"])
            except:
                row.append("-")
            try:
                row.append(edar["population_real"])
            except:
                row.append("-")
            try:
                row.append(edar["configuration"][0])
            except:
                row.append("-")
            try:
                row.append(edar["configuration"][1])
            except:
                row.append("-")
            try:
                row.append(edar["configuration"][2])
            except:
                row.append("-")
            try:
                row.append(edar["compounds_effluent"]["dbo"])
            except:
                row.append("-")
            try:
                row.append(edar["compounds_effluent"]["nitrats"])
            except:
                row.append("-")
            try:
                row.append(edar["compounds_effluent"]["fosfor"])
            except:
                row.append("-")

            edars_table.append(row)

        self.window["dp_table"].update(edars_table)
