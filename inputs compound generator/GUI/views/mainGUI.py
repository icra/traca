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
                values=[["-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"]],
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
                    "Flow",
                    "DBO",  # cbn_bod
                    "Organic phosphorus",  # ptl_p
                    "Organic nitrogen",  # ptl_n
                    "Nitrate",  # no3_n
                    "Ammonia"  # nh3_n
                ],
                num_rows=15,
                auto_size_columns=False,
                col_widths=[16, 10, 35, 10, 10, 8, 8, 8, 13, 11, 11, 11, 11, 11, 11],
                key="dp_table",
                expand_x=True
            )],
            [PySimpleGUI.Table(
                values=[["-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"]],
                headings=[
                    "SWAT ID",
                    "Name",
                    "Flow",
                    "DBO",  # cbn_bod
                    "Organic phosphorus",  # ptl_p
                    "Organic nitrogen",  # ptl_n
                    "Nitrate",  # no3_n
                    "Ammonia"  # nh3_n
                ],
                num_rows=15,
                auto_size_columns=False,
                col_widths=[10, 35, 11, 11, 11, 11, 11, 11],
                key="dp_table_in",
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
                lat = round(float(edar["lat"]), 5)
                row.append(lat)
            except:
                row.append("-")
            try:
                long = round(float(edar["long"]), 5)
                row.append(long)
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
                if len(edar["configuration"]) > 2:
                    row.append(edar["configuration"][2:])  # Pot tenir mes d'un tractament terciari, llistar-los tots
                else:
                    row.append("-")
            except:
                row.append("-")
            try:
                cabal = round(float(edar["compounds_effluent"]["cabal"]), 5)
                row.append(cabal)
            except:
                row.append("-")
            try:
                dbo = round(float(edar["compounds_effluent"]["dbo"]), 5)
                row.append(dbo)
            except:
                row.append("-")
            try:
                fosfor = round(float(edar["compounds_effluent"]["fosfor"]), 5)
                row.append(fosfor)
            except:
                row.append("-")
            try:
                organic = round(float(edar["compounds_effluent"]["nitrogen_org"]), 5)
                row.append(organic)
            except:
                row.append("-")
            try:
                nitrats = round(float(edar["compounds_effluent"]["nitrats"]), 5)
                row.append(nitrats)
            except:
                row.append("-")
            try:
                amoni = round(float(edar["compounds_effluent"]["amoni"]), 5)
                row.append(amoni)
            except:
                row.append("-")

            edars_table.append(row)

        self.window["dp_table"].update(edars_table)
    def update_table_in(self, voluemes):
        volumes_table = []
        for volume in voluemes.values():
            row = []
            try:
                row.append(volume["point"])
            except:
                row.append("-")
            try:
                row.append(volume["activitat"])
            except:
                row.append("-")
            try:
                q = round(float(volume["q"]), 5)
                row.append(q)
            except:
                row.append("-")
            try:
                dbo = round(float(volume["dbo"]), 5)
                row.append(dbo)
            except:
                row.append("-")
            try:
                fosfor = round(float(volume["phosphor"]), 5)
                row.append(fosfor)
            except:
                row.append("-")
            try:
                organic = round(float(volume["nitrogen_org"]), 5)
                row.append(organic)
            except:
                row.append("-")
            try:
                nitrats = round(float(volume["nitrats"]), 5)
                row.append(nitrats)
            except:
                row.append("-")
            try:
                amoni = round(float(volume["amoni"]), 5)
                row.append(amoni)
            except:
                row.append("-")

            volumes_table.append(row)

        self.window["dp_table_in"].update(volumes_table)
