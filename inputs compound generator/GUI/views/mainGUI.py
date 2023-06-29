import PySimpleGUI


class mainGUI:
    global list_configs
    list_configs = []

    def __init__(self, contaminants_i_nutrients):
        self.contaminants_i_nutrients = contaminants_i_nutrients
        self.window = PySimpleGUI.Window("IG+", self.init_GUI(), finalize=True, location=(0, 0))

    def init_GUI(self):
        menu_def = [
            ['Settings', ['File properties']]
        ]

        values = ["-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"]
        headings = ["EDAR EU_ID", "SWAT ID", "Nom", "Població", "Primari", "Secundari", "Terciari", "Cabal", "DBO 5 dies",
                    "Fòsfor orgànic",  "Nitrogen orgànic",  "Nitrats",  "Amoni"]


        values_industries = ["-", "-", "-", "-", "-", "-", "-", "-"]
        headings_industries = ["SWAT ID", "Nom", "Cabal", "DBO 5 dies",
                    "Fòsfor orgànic",  "Nitrogen orgànic",  "Nitrats",  "Amoni"]

        for contaminant in self.contaminants_i_nutrients:
            if contaminant not in headings:
                headings.append(contaminant)
                headings_industries.append(contaminant)


        layout = [
            #[PySimpleGUI.Menu(menu_def, tearoff=False)],
            [PySimpleGUI.Text("SWAT+ Input Generator (TRAÇA)")],
            [PySimpleGUI.Table(
                values=[values],
                headings=headings,
                auto_size_columns=False,
                col_widths=[16, 10, 35, 8, 8, 8, 13, 11, 11, 11, 11, 11, 11],
                key="dp_table",
                vertical_scroll_only=False
            )],
            [PySimpleGUI.Table(
                values=[values_industries],
                headings=headings_industries,
                auto_size_columns=False,
                col_widths=[10, 35, 11, 11, 11, 11, 11, 11],
                key="dp_table_in",
                vertical_scroll_only=False
            )],

            [PySimpleGUI.Text('.SQLite location: '),
            PySimpleGUI.InputText('', key='swat_db_sqlite', enable_events=True),
            PySimpleGUI.FileBrowse(),
            PySimpleGUI.Button("Add data from the point of discharge", key="add_dp_data")
            ],

            [
                PySimpleGUI.Button("Guardar generació de contaminants", key="pollutants_generator"),
                PySimpleGUI.Button("Generació d'escenaris", key="scenarios_generator")
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
                row.append(edar["id_swat"])
            except:
                row.append("-")
            try:
                row.append(edar["nom"])
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
                cabal = round(float(edar["compounds_effluent"]["q"]), 3)
                row.append(cabal)
            except:
                row.append("-")
            try:
                dbo = round(float(edar["compounds_effluent"]["DBO 5 dies"] * 1000), 3)
                row.append(dbo)
            except:
                row.append("-")
            try:
                fosfor = round(float(edar["compounds_effluent"]["Fòsfor orgànic"] * 1000), 3)
                row.append(fosfor)
            except:
                row.append("-")
            try:
                organic = round(float(edar["compounds_effluent"]["Nitrogen orgànic"] * 1000), 3)
                row.append(organic)
            except:
                row.append("-")
            try:
                nitrats = round(float(edar["compounds_effluent"]["Nitrats"] * 1000), 3)
                row.append(nitrats)
            except:
                row.append("-")
            try:
                amoni = round(float(edar["compounds_effluent"]["Amoni"] * 1000), 3)
                row.append(amoni)
            except:
                row.append("-")

            for contaminant in self.contaminants_i_nutrients:
                if contaminant not in ["DBO 5 dies", "Fòsfor orgànic", "Nitrogen orgànic", "Nitrats", "Amoni"]:
                    valor = '-'
                    if contaminant in edar["compounds_effluent"]:
                        valor = round(float(edar["compounds_effluent"][contaminant] * 1000), 3)
                    row.append(valor)

            edars_table.append(row)

        self.window["dp_table"].update(edars_table)
    def update_table_in(self, voluemes):

        volumes_sorted = list(voluemes.values())
        volumes_sorted.sort(key=lambda x: x["abocament"])

        volumes_table = []
        for volume in volumes_sorted:
            row = []
            try:
                row.append(volume["id"])
            except:
                row.append("-")
            try:
                row.append(volume["abocament"])
            except:
                row.append("-")
            try:
                q = round(float(volume["q"]), 5)
                row.append(q)
            except:
                row.append("-")
            try:
                dbo = round(float(volume["DBO 5 dies"] * 1000), 5)
                row.append(dbo)
            except:
                row.append("-")
            try:
                fosfor = round(float(volume["Fòsfor"] * 1000), 5)
                row.append(fosfor)
            except:
                row.append("-")
            try:
                organic = round(float(volume["Nitrogen"] * 1000), 5)
                row.append(organic)
            except:
                row.append("-")
            try:
                nitrats = round(float(volume["Nitrats"] * 1000), 5)
                row.append(nitrats)
            except:
                row.append("-")
            try:
                amoni = round(float(volume["Amoniac"] * 1000), 5)
                row.append(amoni)
            except:
                row.append("-")

            for contaminant in self.contaminants_i_nutrients:
                if contaminant not in ["DBO 5 dies", "Fòsfor orgànic", "Nitrogen orgànic", "Nitrats", "Amoni"]:
                    valor = '-'
                    if contaminant in volume:
                        valor = round(float(volume[contaminant] * 1000), 3)
                    row.append(valor)

            volumes_table.append(row)

        self.window["dp_table_in"].update(volumes_table)
